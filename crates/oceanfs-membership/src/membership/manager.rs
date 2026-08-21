//! Membership lifecycle management.
//!
//! Contains the constructor, start-up, join, leave, and state mutation
//! logic for the [`Membership`] coordinator.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use oceanfs_core::{Gauge, Incarnation, LabelSet, NodeId, NodeState};
use oceanfs_network::ConnectionPool;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::{debug, info, trace, warn};

use super::{
    state::{MembershipState, NodeEntry},
    Membership, MembershipEvent,
};
use crate::{
    error::{Error, Result},
    failure_detector::{DetectorCommand, DetectorConfig, FailureDetector},
    gossip::{GossipCommand, GossipProtocol},
};

impl Membership {
    /// Creates a new membership instance.
    ///
    /// Sets up internal channels and state but does NOT start background
    /// tasks. Call [`Self::start`] then [`Self::join`] to join the cluster.
    pub fn new(
        node_id: NodeId,
        address: SocketAddr,
        config: oceanfs_core::GossipConfig,
        ring: Arc<oceanfs_routing::RingCache>,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(256);

        Self {
            node_id,
            address,
            config,
            state: RwLock::new(MembershipState::new()),
            ring,
            event_tx,
            detector_tx: RwLock::new(None),
            gossip_tx: RwLock::new(None),
            pool: RwLock::new(None),
            started: RwLock::new(false),
            shutdown: tokio_util::sync::CancellationToken::new(),
            gossip_sent: RwLock::new(None),
            gossip_received: RwLock::new(None),
            gossip_dropped: RwLock::new(None),
            gossip_round_duration: RwLock::new(None),
            gossip_push_duration: RwLock::new(None),
            gossip_delta_entries: RwLock::new(None),
            probe_duration: RwLock::new(None),
            probe_failures: RwLock::new(None),
            indirect_probes: RwLock::new(None),
            self_version: std::sync::atomic::AtomicU64::new(0),
            ring_version: Gauge::new(
                "ring_version".into(),
                "Ring topology version, incremented on each change".into(),
                LabelSet::empty(),
            ),
        }
    }

    /// Starts the background failure detector and gossip tasks.
    /// Must be called before [`Self::join`].
    ///
    /// # Errors
    /// Returns [`Error::AlreadyStarted`] if called more than once.
    pub fn start(self: &Arc<Self>) -> Result<()> {
        let mut started = self.started.write();
        if *started {
            return Err(Error::AlreadyStarted);
        }
        *started = true;
        drop(started);
        // ---- Failure detector ----
        let detector_config = DetectorConfig {
            interval_ms: self.config.interval_ms,
            ping_timeout_ms: self.config.failure_timeout_ms / 3,
            suspicion_timeout_ms: self.config.suspicion_timeout_ms,
            failure_timeout_ms: self.config.failure_timeout_ms,
            indirect_ping_count: self.config.indirect_ping_count,
        };
        // ADR-0028 D1: probes run over the membership plane's dedicated
        // pool (wired via `set_pool` before `start` in the node; may
        // also arrive later via `DetectorCommand::SetPool`).
        let detector_pool = self.pool.read().clone();
        let (mut detector, detector_cmd_tx) = FailureDetector::new(
            detector_config,
            self.event_tx.clone(),
            self.node_id.clone(),
            64,
            detector_pool,
        );

        // Store the command sender so other methods can control the detector.
        let detector_tx_for_gossip = detector_cmd_tx.clone();
        *self.detector_tx.write() = Some(detector_cmd_tx);

        // Extract the probe metrics for registration (the detector owns
        // them; registration happens after start()).
        {
            let mut dur = self.probe_duration.write();
            let mut failures = self.probe_failures.write();
            let mut indirect = self.indirect_probes.write();
            *dur = Some(detector.metrics.duration_us.clone());
            *failures = Some(detector.metrics.failures_total.clone());
            *indirect = Some(detector.metrics.indirect_total.clone());
        }

        let detector_shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = detector.run() => {},
                _ = detector_shutdown.cancelled() => {},
            }
        });

        // ---- Alive-nodes sync ----
        // Periodically feed the failure detector with the current set of alive
        // nodes from the membership state. Without this, the detector has no
        // peers to ping and cannot detect failures.
        let sync_membership = Arc::clone(self);
        let sync_detector_tx = detector_tx_for_gossip.clone();
        let sync_shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(sync_membership.config.interval_ms));
            // Don't fire immediately — wait for the first interval.
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let alive: Vec<_> = sync_membership
                            .state
                            .read()
                            .nodes
                            .iter()
                            .map(|(id, e)| (id.clone(), e.state, e.address, e.incarnation))
                            .collect();
                        let _ = sync_detector_tx.try_send(
                            DetectorCommand::UpdateAliveNodes { nodes: alive },
                        );
                    }
                    _ = sync_shutdown.cancelled() => break,
                }
            }
        });
        // ---- Gossip protocol ----
        let (gossip_cmd_tx, gossip_cmd_rx) = tokio::sync::mpsc::channel(64);
        let (gossip_event_tx, _gossip_event_rx) = broadcast::channel(16);

        let mut gossip_protocol = GossipProtocol::new(
            gossip_cmd_rx,
            gossip_cmd_tx.clone(),
            gossip_event_tx,
            self.event_tx.clone(),
            self.config.interval_ms,
            self.node_id.clone(),
            self.config.fanout_k,
        );

        // If the connection pool is already set, pass it to the gossip protocol.
        if let Some(pool) = self.pool.read().as_ref() {
            gossip_protocol.set_pool(pool.clone());
        }
        // Extract gossip counters + histograms for metrics registration.
        {
            let mut sent = self.gossip_sent.write();
            let mut recv = self.gossip_received.write();
            let mut dropped = self.gossip_dropped.write();
            let mut round = self.gossip_round_duration.write();
            let mut push = self.gossip_push_duration.write();
            let mut delta_entries = self.gossip_delta_entries.write();
            *delta_entries = Some(gossip_protocol.delta_entries_hist.clone());
            *sent = Some(gossip_protocol.messages_sent.clone());
            *recv = Some(gossip_protocol.messages_received.clone());
            *dropped = Some(gossip_protocol.messages_dropped.clone());
            *round = Some(gossip_protocol.round_duration_us.clone());
            *push = Some(gossip_protocol.push_duration_us.clone());
        }
        *self.gossip_tx.write() = Some(gossip_cmd_tx);
        let gossip_shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = gossip_protocol.run() => {},
                _ = gossip_shutdown.cancelled() => {},
            }
        });

        // ---- Event handler: apply state changes to membership and ring ----
        // The failure detector and gossip protocol emit MembershipEvent via
        // event_tx. This task subscribes and calls upsert_node() to keep the
        // membership state and ring consistent with detected state changes.
        let mut event_rx = self.event_tx.subscribe();
        let event_membership = Arc::clone(self);
        let event_shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    event = event_rx.recv() => {
                        match event {
                            Ok(MembershipEvent {
                                node_id,
                                new_state,
                                incarnation,
                                address,
                                version,
                                origin,
                                ..
                            }) => {
                                // The event itself carries the incarnation,
                                // address, version, and origin (ADR-0022,
                                // ADR-0028 D3): a re-admitted node is absent
                                // from local state, so only the event can
                                // supply its fresh incarnation/address;
                                // deriving them from state.nodes would
                                // regress the incarnation to a stale value
                                // and block legitimate re-admission
                                // (t24/t43).
                                event_membership.upsert_node_attributed(
                                    node_id, new_state, incarnation, address, version, origin,
                                );
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!(skipped = n, "membership event handler lagged");
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    _ = event_shutdown.cancelled() => break,
                }
            }
            tracing::debug!("membership event handler shut down");
        });

        info!(node_id = %self.node_id, "membership background tasks started");

        Ok(())
    }

    /// Sets the connection pool for gRPC-based gossip and join operations.
    ///
    /// Must be called before [`Self::join`] if seed nodes are configured.
    /// The pool is shared with the gossip protocol for push/pull and with
    /// the failure detector for SWIM probes (ADR-0028 D1: the membership
    /// plane's dedicated pool).
    ///
    /// If the background tasks have already been started, this sends a
    /// `SetPool` command to each to update them asynchronously.
    pub fn set_pool(&self, pool: Arc<ConnectionPool>) {
        // Update the gossip protocol if it's already running.
        if let Some(tx) = self.gossip_tx.read().as_ref() {
            let _ = tx.try_send(GossipCommand::SetPool { pool: pool.clone() });
        }
        // Update the failure detector if it's already running.
        if let Some(tx) = self.detector_tx.read().as_ref() {
            let _ = tx.try_send(DetectorCommand::SetPool { pool: pool.clone() });
        }
        *self.pool.write() = Some(pool);
    }

    /// Registers gossip + probe counters with a metrics registrar.
    pub fn register_gossip_metrics(&self, registrar: &dyn oceanfs_core::MetricRegistrar) {
        if let Some(ref c) = *self.gossip_sent.read() {
            registrar.register_counter(c.clone());
        }
        if let Some(ref c) = *self.gossip_received.read() {
            registrar.register_counter(c.clone());
        }
        if let Some(ref c) = *self.gossip_dropped.read() {
            registrar.register_counter(c.clone());
        }
        if let Some(ref h) = *self.gossip_round_duration.read() {
            registrar.register_histogram(h.clone());
        }
        if let Some(ref h) = *self.gossip_push_duration.read() {
            registrar.register_histogram(h.clone());
        }
        if let Some(ref h) = *self.gossip_delta_entries.read() {
            registrar.register_histogram(h.clone());
        }
        // SWIM probe metrics (ADR-0028 D2): the liveness plane's own
        // observability — the fleet churn campaign measured the proxy
        // push at 195 ms p99; probe latency is the detection-bound
        // signal now.
        if let Some(ref h) = *self.probe_duration.read() {
            registrar.register_histogram(h.clone());
        }
        if let Some(ref c) = *self.probe_failures.read() {
            registrar.register_counter(c.clone());
        }
        if let Some(ref c) = *self.indirect_probes.read() {
            registrar.register_counter(c.clone());
        }
        registrar.register_gauge(self.ring_version.clone());
    }

    /// Joins the cluster by contacting seed nodes via gRPC.
    ///
    /// 1. Contacts each configured seed node via `GossipRpcClient::pull` to
    ///    receive the current membership state.
    /// 2. If no configured seed is reachable (or none is configured),
    ///    contacts the fallback seeds — last-known member addresses
    ///    persisted by the composition root (ADR-0022 Decision 3, t43).
    /// 3. Announces self as ALIVE to the joined seed via
    ///    `GossipRpcClient::push`, using `self_incarnation`.
    /// 4. Adds self to the ring at `self_incarnation`.
    ///
    /// `self_incarnation` is the incarnation to announce with: the
    /// composition root computes `persisted + 1` on restart, or `1` on
    /// first boot (spec §13.1). `fallback_seeds` is the persisted list
    /// of last-known member addresses; it may be empty.
    ///
    /// # Errors
    ///
    /// Returns [`Error::JoinFailed`] if seed nodes are configured but
    /// none (configured or fallback) are reachable, or if no connection
    /// pool has been set while seeds must be contacted.
    pub async fn join(
        &self,
        self_incarnation: Incarnation,
        fallback_seeds: &[String],
    ) -> Result<()> {
        let seed_nodes = &self.config.seed_nodes;

        let mut joined_seed_addr: Option<SocketAddr> = None;

        let must_contact = !seed_nodes.is_empty() || !fallback_seeds.is_empty();
        if must_contact {
            // Contact seed nodes via gRPC to receive initial state.
            let pool = {
                self.pool
                    .read()
                    .as_ref()
                    .ok_or_else(|| Error::JoinFailed("no connection pool set".into()))?
                    .clone()
            };

            // Primary: configured seed nodes.
            if !seed_nodes.is_empty() {
                joined_seed_addr = self.pull_membership_from_seeds(&pool, seed_nodes).await;
            }

            // Fallback: persisted last-known member addresses, used when
            // configured seeds are unreachable or empty (ADR-0022 D3).
            // Covers the seedless bootstrap-node restart (t43).
            if joined_seed_addr.is_none() && !fallback_seeds.is_empty() {
                warn!(
                    node_id = %self.node_id,
                    count = fallback_seeds.len(),
                    "configured seed nodes unreachable or empty; \
                     trying persisted fallback seeds"
                );
                joined_seed_addr = self.pull_membership_from_seeds(&pool, fallback_seeds).await;
            }

            if joined_seed_addr.is_none() && !seed_nodes.is_empty() {
                return Err(Error::JoinFailed(
                    "could not contact any seed node (configured or fallback)".into(),
                ));
            }

            // PR5: After receiving membership list, announce self to the seed
            // via push. This lets the seed learn about the joiner and add it
            // to its ring.
            if let Some(seed_addr) = joined_seed_addr {
                debug!(seed = %seed_addr, "announcing self to seed via gossip push");

                let pooled = pool.get_channel(seed_addr).await.map_err(|e| {
                    Error::JoinFailed(format!("failed to connect to seed for push: {e}"))
                })?;
                let channel = pooled.channel().clone();
                drop(pooled);

                let mut push_client = oceanfs_network::GossipRpcClient::new(channel);
                let proto_entry = oceanfs_core::proto::membership::MembershipEntry {
                    node_id: Some(oceanfs_core::proto::common::NodeId {
                        id: self.node_id.to_string(),
                    }),
                    state: 0, // ALIVE
                    incarnation: self_incarnation.value(),
                    address: self.address.to_string(),
                    last_seen: None,
                    // ADR-0028 D3: attribution lands in f4.
                    version: 0,
                    origin: String::new(),
                };
                let delta =
                    oceanfs_core::proto::membership::MembershipList { entries: vec![proto_entry] };
                let msg = oceanfs_network::gossip::GossipMessage {
                    delta: Some(delta),
                    version_vector: std::collections::HashMap::new(),
                };
                let stream = tokio_stream::iter(vec![msg]);

                match push_client.push(tonic::Request::new(stream)).await {
                    Ok(response) => {
                        let ack = response.into_inner();
                        if ack.accepted {
                            info!(
                                seed = %seed_addr,
                                "seed accepted self-announcement"
                            );
                        }
                    }
                    Err(status) => {
                        warn!(
                            seed = %seed_addr,
                            error = %status,
                            "failed to announce self to seed"
                        );
                    }
                }
            }
        } else {
            info!(
                node_id = %self.node_id,
                "no seed nodes configured, starting as first node"
            );
        }

        // Announce self as ALIVE via upsert_node so the gossip protocol is
        // notified. The incarnation is the announcement value (persisted + 1
        // on restart, 1 on first boot) — never a hardcoded 1 (ADR-0022 D1).
        // The probe service answers with `Membership::incarnation_of` —
        // this upsert keeps that value in sync (ADR-0028 D2). The entry
        // carries origin = self with a fresh version (ADR-0028 D3).
        let self_version = self.self_version.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        self.upsert_node_attributed(
            self.node_id.clone(),
            NodeState::Alive,
            self_incarnation,
            Some(self.address),
            self_version,
            self.node_id.clone(),
        );

        info!(node_id = %self.node_id, "joined cluster successfully");
        Ok(())
    }

    /// Contacts each seed in `seeds` and merges its membership list.
    ///
    /// Returns the address of the first seed that answered, or `None`
    /// if none were reachable. Best-effort: individual failures are
    /// logged at `warn!` and skipped.
    async fn pull_membership_from_seeds(
        &self,
        pool: &Arc<ConnectionPool>,
        seeds: &[String],
    ) -> Option<SocketAddr> {
        for seed_str in seeds {
            let seed_addr: SocketAddr = match seed_str.parse() {
                Ok(a) => a,
                Err(e) => {
                    warn!(seed = %seed_str, error = %e, "invalid seed address");
                    continue;
                }
            };

            debug!(
                node_id = %self.node_id,
                seed = %seed_addr,
                "contacting seed node via gRPC"
            );

            let pooled = match pool.get_channel(seed_addr).await {
                Ok(p) => p,
                Err(e) => {
                    warn!(seed = %seed_addr, error = %e, "failed to connect to seed");
                    continue;
                }
            };

            let channel = pooled.channel().clone();
            drop(pooled);

            // Pull the full membership list from the seed.
            let mut client = oceanfs_network::GossipRpcClient::new(channel);
            let request = tonic::Request::new(oceanfs_network::gossip::GossipPullRequest {
                node_id: Some(oceanfs_core::proto::common::NodeId { id: self.node_id.to_string() }),
                // Empty version vector = "send everything" (join, ADR-0028 D4).
                version_vector: std::collections::HashMap::new(),
            });

            match client.pull(request).await {
                Ok(response) => {
                    let mut stream = response.into_inner();
                    while let Some(Ok(msg)) = tokio_stream::StreamExt::next(&mut stream).await {
                        if let Some(delta) = msg.delta {
                            for entry in &delta.entries {
                                let nid = entry.node_id.as_ref().map(|n| NodeId::new(&n.id));
                                let state = match entry.state {
                                    0 => NodeState::Alive,
                                    1 => NodeState::Suspect,
                                    2 => NodeState::Dead,
                                    3 => NodeState::Leaving,
                                    4 => NodeState::Left,
                                    _ => continue,
                                };
                                let inc = Incarnation::new(entry.incarnation);
                                let addr = entry
                                    .address
                                    .parse::<SocketAddr>()
                                    .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], 9001)));
                                if let Some(id) = nid {
                                    // Join-time pull: entries carry no
                                    // attribution (version 0, empty
                                    // origin — the joiner has no local
                                    // facts yet, so the incarnation and
                                    // F1d gates alone decide).
                                    self.upsert_node_attributed(
                                        id,
                                        state,
                                        inc,
                                        Some(addr),
                                        0,
                                        NodeId::new(""),
                                    );
                                }
                            }
                        }
                    }
                    info!(seed = %seed_addr, "received membership state from seed");
                    return Some(seed_addr);
                }
                Err(status) => {
                    warn!(
                        seed = %seed_addr,
                        error = %status,
                        "pull from seed failed"
                    );
                }
            }
        }
        None
    }

    /// Gracefully leaves the cluster.
    ///
    /// 1. Announces LEAVING state via gossip.
    /// 2. Determines the ring successor for data handoff.
    /// 3. If a `leave_handler` is provided, seals and transfers WAL
    ///    data and segment shards to the successor.
    /// 4. Announces LEFT state and removes self from the ring.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotStarted`] if background tasks haven't been started.
    /// Returns [`Error::Leave`] if the leave handler reports a failure.
    pub async fn leave(
        &self,
        leave_handler: Option<&dyn crate::GracefulLeaveHandler>,
    ) -> Result<()> {
        if !*self.started.read() {
            return Err(Error::NotStarted);
        }

        let node_id = self.node_id.clone();

        // Determine the ring successor for data handoff.
        let successor =
            self.ring.snapshot().successor_of(&node_id).unwrap_or_else(|| node_id.clone());

        // Self incarnation for the leave events: use the recorded value
        // so the transition never regresses the incarnation (T8).
        let self_incarnation = {
            let state = self.state.read();
            state
                .incarnations
                .get(&node_id)
                .copied()
                .or_else(|| state.nodes.get(&node_id).map(|e| e.incarnation))
                .unwrap_or_else(|| Incarnation::new(1))
        };

        // The leaver's own version for itself (ADR-0028 D3): the
        // Leaving/Left entries carry origin = self so peers apply them
        // with the leaver-authority class.
        let leave_version =
            self.self_version.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;

        // Transition to LEAVING.
        let _ = self.event_tx.send(MembershipEvent {
            node_id: node_id.clone(),
            old_state: NodeState::Alive,
            new_state: NodeState::Leaving,
            incarnation: self_incarnation,
            address: Some(self.address),
            version: leave_version,
            origin: self.node_id.clone(),
        });

        info!(
            node_id = %node_id,
            successor = %successor,
            "node leaving cluster"
        );

        // Execute graceful leave data handoff.
        if let Some(handler) = leave_handler {
            if successor != node_id {
                info!(successor = %successor, "handing off WAL to successor");
                handler
                    .handoff_wal_to(&successor)
                    .await
                    .map_err(|e| Error::Leave(format!("WAL handoff failed: {e}")))?;

                info!(successor = %successor, "transferring segment shards to successor");
                let count = handler
                    .transfer_segment_shards_to(&successor)
                    .await
                    .map_err(|e| Error::Leave(format!("segment shard transfer failed: {e}")))?;
                info!(count, "segment shard transfer complete");
            } else {
                info!("no successor found; skipping data handoff");
            }
        } else {
            // No leave handler provided — drain period only.
            info!("no leave handler configured; draining in-flight requests");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Transition to LEFT.
        let _ = self.event_tx.send(MembershipEvent {
            node_id: node_id.clone(),
            old_state: NodeState::Leaving,
            new_state: NodeState::Left,
            incarnation: self_incarnation,
            address: Some(self.address),
            version: leave_version,
            origin: self.node_id.clone(),
        });

        // Remove self from ring.
        let mut ring_snapshot = (*self.ring.snapshot()).clone();
        if let Err(e) = ring_snapshot.remove_node(node_id.clone()) {
            warn!(
                node_id = %node_id,
                error = %e,
                "failed to remove self from ring"
            );
        }
        self.ring.update(ring_snapshot);
        self.ring_version.inc();

        info!(node_id = %node_id, "node left cluster");
        Ok(())
    }

    /// Adds or updates a node's state from external input (e.g., gossip merge).
    /// New ALIVE nodes are added to the ring; LEFT nodes are removed.
    /// DEAD nodes are RETAINED (state=Dead): the topology is the stable
    /// N-set — liveness is a quorum concern, not a topology concern.
    /// Removing dead nodes silently shrank the replica set, so a
    /// quorum-met write never targeted the returning node and no hint
    /// was created for it (the coordinator didn't know it existed) —
    /// the churn 404/404/200 divergence.
    ///
    /// The wrapper attributes the entry to the LOCAL node with version 0
    /// (a local observation, ADR-0028 D3): callers without their own
    /// version clocks — the write coordinator, hinted handoff, admin —
    /// report facts as "observed locally". The attributed entry point is
    /// [`Self::upsert_node_attributed`].
    ///
    /// `address` may be `None` when the caller (e.g. the failure detector)
    /// does not know it; the existing stored address is then preserved.
    ///
    /// # Panics
    ///
    /// Never panics (the null-address fallback is infallible).
    pub fn upsert_node(
        &self,
        node_id: NodeId,
        state: NodeState,
        incarnation: Incarnation,
        address: Option<SocketAddr>,
    ) {
        self.upsert_node_attributed(node_id, state, incarnation, address, 0, self.node_id.clone());
    }

    /// The attributed entry point (ADR-0028 D3): `version` is the
    /// observer's logical clock for this node and `origin` the observer
    /// — the authority-class merge rules use them to order facts.
    ///
    /// Enforces the F1d invariant: *if a node id is absent from
    /// `state.nodes` (or recorded Dead/Left) and recorded with
    /// incarnation `N`, only an entry with incarnation `> N` may
    /// (re)admit it.* An equal/lower incarnation for a removed or dead
    /// node is dropped — this closes the Dead↔Alive oscillation loop
    /// (t24) and permits the legitimate ADR-0022 self-rejoin (strictly
    /// higher incarnation with a fresh address, t21/t43).
    ///
    /// Attribution (ADR-0028 D3): entries are ordered at equal
    /// incarnation by the authority-class table (`authority_class` in
    /// `membership/mod.rs`): my own detector's facts beat remote
    /// detectors' facts, remote detector facts beat the target's own
    /// announcements, and entries about SELF must originate from self.
    /// Within the same class and origin, the higher `version` wins.
    /// These rules replace the historical terminality/stale-suspect/
    /// self-guard heuristics.
    pub fn upsert_node_attributed(
        &self,
        node_id: NodeId,
        state: NodeState,
        incarnation: Incarnation,
        address: Option<SocketAddr>,
        version: u64,
        origin: NodeId,
    ) {
        let mut inner = self.state.write();

        // Capture old state and the recorded incarnation before modifying.
        let old = inner.nodes.get(&node_id).map(|e| e.state);
        let stored_address = inner.nodes.get(&node_id).map(|e| e.address);
        let recorded = inner.incarnations.get(&node_id).copied();
        let old_state = old.unwrap_or(NodeState::Alive);
        let is_new = old.is_none();
        let self_id = self.node_id.clone();

        // F1d re-admission guard: a node that is absent, Dead, or Left
        // may only be re-admitted at a strictly higher incarnation.
        // Stale ALIVE gossip at equal/lower incarnation must not revive
        // it (the t24 Dead↔Alive oscillation loop). This applies to
        // RETAINED Dead entries too — Dead nodes stay in the table (the
        // topology is stable; liveness is a quorum concern), so
        // `is_new` alone no longer covers the re-admission case.
        let recorded_state = inner.nodes.get(&node_id).map(|e| e.state);
        let readmission_gated =
            is_new || matches!(recorded_state, Some(NodeState::Dead) | Some(NodeState::Left));
        if readmission_gated && recorded.is_some_and(|last| incarnation <= last) {
            trace!(
                node_id = %node_id,
                incoming = incarnation.value(),
                recorded = recorded.map(|i| i.value()).unwrap_or(0),
                "upsert_node: rejecting re-admission at incarnation <= recorded"
            );
            drop(inner);
            return;
        }

        // ADR-0028 D3 rule 2: an incarnation BELOW the recorded value is
        // stale for ANY entry — a Suspect/Dead downgrade fired with the
        // pre-rejoin incarnation must not regress the fresh Alive (the
        // fleet node-1 stuck Dead(5) class). The incarnation never
        // regresses (T8).
        if recorded.is_some_and(|last| incarnation < last) {
            trace!(
                node_id = %node_id,
                incoming = ?state,
                incoming_inc = incarnation.value(),
                recorded = recorded.map(|i| i.value()).unwrap_or(0),
                "upsert_node: rejecting entry below the recorded incarnation"
            );
            drop(inner);
            return;
        }

        // SELF-LIVENESS AUTHORITY (ADR-0028 D3): a node must never
        // accept any state for ITSELF from another origin — the node is
        // the only authority on its own liveness. The historical guard
        // rejected only Suspect/Dead for self; the attribution model
        // rejects every non-self origin outright (a self-origin Alive
        // with a stale version is still idempotent below).
        if node_id == self_id && origin != self_id {
            trace!(
                node_id = %node_id,
                incoming = ?state,
                "upsert_node: rejecting non-self-origin entry about self"
            );
            drop(inner);
            return;
        }

        // Equal-incarnation ordering by the authority class (D3).
        // A lower-class incoming entry never overwrites a higher-class
        // local fact; the same class with the same origin is ordered by
        // version; the same class with a different origin keeps the
        // local entry (no cross-origin churn — my own detector is the
        // authority to move the state forward).
        if let Some(local) = inner.nodes.get(&node_id) {
            if incarnation == local.incarnation {
                let incoming_class =
                    crate::membership::authority_class(&node_id, &origin, state, &self_id);
                let local_class = crate::membership::authority_class(
                    &node_id,
                    &local.origin,
                    local.state,
                    &self_id,
                );
                if incoming_class < local_class {
                    trace!(
                        node_id = %node_id,
                        incoming = ?state,
                        incoming_class,
                        current = ?local.state,
                        local_class,
                        "upsert_node: rejecting lower-authority entry at equal incarnation"
                    );
                    drop(inner);
                    return;
                }
                if incoming_class == local_class {
                    if origin == local.origin {
                        if version <= local.version {
                            trace!(
                                node_id = %node_id,
                                incoming = ?state,
                                version,
                                current = ?local.state,
                                current_version = local.version,
                                "upsert_node: rejecting same-origin entry at version <= local"
                            );
                            drop(inner);
                            return;
                        }
                    } else {
                        // Same class, different origin: keep the local
                        // entry (no churn between remote facts).
                        trace!(
                            node_id = %node_id,
                            incoming = ?state,
                            current = ?local.state,
                            "upsert_node: keeping local entry at equal authority class"
                        );
                        drop(inner);
                        return;
                    }
                }
            }
        }

        // Incarnation must never regress below the recorded value (T8).
        let effective_incarnation = recorded.map_or(incarnation, |last| last.max(incarnation));

        // Apply the transition. A LEFT node is removed (it is gone for
        // good — its data was handed off). A DEAD node is RETAINED with
        // state=Dead and its last-known address: the topology must stay
        // stable so the write/delete coordinators replicate to the FULL
        // N-set — failed attempts against the dead node become hint
        // debt that repays when it returns. Removing dead nodes from
        // the table (and the ring) silently shrank the replica set: a
        // quorum-met write never targeted the returning node, and NO
        // hint was created for it (the coordinator didn't know it
        // existed) — the churn 404/404/200 divergence.
        let effective_address: Option<SocketAddr>;
        if state == NodeState::Left {
            inner.nodes.remove(&node_id);
            // Retain the last-known incarnation so a later re-admission at
            // equal incarnation is rejected (F1d invariant).
            inner.incarnations.insert(node_id.clone(), effective_incarnation);
            effective_address = address.or(stored_address);
        } else if state == NodeState::Dead {
            // Retain the entry as Dead (liveness is a quorum concern,
            // not a topology concern — see the comment above). A Dead
            // transition with no known address uses a null placeholder:
            // replication attempts against it fail fast (connection
            // refused) and become hint debt.
            let dead_addr = address.or(stored_address).unwrap_or_else(|| {
                // Null placeholder: replication attempts against it
                // fail fast (connection refused) and become hint debt.
                "127.0.0.1:1"
                    .parse::<std::net::SocketAddr>()
                    .unwrap_or_else(|_| std::net::SocketAddr::from(([127, 0, 0, 1], 1)))
            });
            inner.nodes.insert(
                node_id.clone(),
                crate::membership::state::StoredEntry {
                    state,
                    incarnation: effective_incarnation,
                    address: dead_addr,
                    version,
                    origin: origin.clone(),
                },
            );
            inner.incarnations.insert(node_id.clone(), effective_incarnation);
            effective_address = Some(dead_addr);
        } else {
            let addr = match address.or(stored_address) {
                Some(addr) => addr,
                None => {
                    // Cannot admit a brand-new node without an address.
                    trace!(
                        node_id = %node_id,
                        state = ?state,
                        "upsert_node: dropping admission without a known address"
                    );
                    drop(inner);
                    return;
                }
            };
            effective_address = Some(addr);
            inner.nodes.insert(
                node_id.clone(),
                crate::membership::state::StoredEntry {
                    state,
                    incarnation: effective_incarnation,
                    address: addr,
                    version,
                    origin: origin.clone(),
                },
            );
            inner.incarnations.insert(node_id.clone(), effective_incarnation);
        }
        drop(inner);

        if is_new || old_state != state {
            let _ = self.event_tx.send(MembershipEvent {
                node_id: node_id.clone(),
                old_state: if is_new { NodeState::Alive } else { old_state },
                new_state: state,
                incarnation: effective_incarnation,
                address: effective_address,
                version,
                origin: origin.clone(),
            });

            // PR4: Update ring synchronously on membership changes.
            let mut ring_snapshot = (*self.ring.snapshot()).clone();

            if is_new {
                // New node — add to ring.
                ring_snapshot.add_node(node_id.clone());
                debug!(node_id = %node_id, "ring: added new node");
            } else if state == NodeState::Left {
                // A LEFT node is gone for good — remove from the ring.
                if ring_snapshot.remove_node(node_id.clone()).is_ok() {
                    debug!(
                        node_id = %node_id,
                        state = ?state,
                        "ring: removed left node"
                    );
                }
                // NOTE: DEAD nodes STAY in the ring. The topology is the
                // stable N-set; liveness is a quorum concern. Removing a
                // dead node silently shrank the replica set — a quorum-
                // met write never targeted the returning node and no
                // hint was created for it (the coordinator didn't know
                // it existed): the churn 404/404/200 divergence. With
                // the dead node retained, every write/delete attempts
                // the full N-set and the failures become hint debt.
            }

            self.ring.update(ring_snapshot);
            self.ring_version.inc();

            // F1c: stop the failure detector from probing a dead/left node.
            if state == NodeState::Dead || state == NodeState::Left {
                if let Some(tx) = self.detector_tx.read().as_ref() {
                    let _ = tx.try_send(DetectorCommand::RemoveNode { node_id: node_id.clone() });
                }
            }

            // Notify the gossip protocol of membership changes. For a Dead
            // entry the address is irrelevant to peers; fall back to the
            // loopback placeholder only in that degenerate case.
            if let Some(tx) = self.gossip_tx.read().as_ref() {
                let entry = NodeEntry {
                    node_id: node_id.clone(),
                    incarnation: effective_incarnation,
                    state,
                    address: effective_address
                        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 9001))),
                    version,
                    origin: origin.clone(),
                };
                let _ = tx.try_send(GossipCommand::AddNode { entry });
                debug!(
                    node_id = %node_id,
                    state = ?state,
                    "gossip: enqueued AddNode"
                );
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::net::SocketAddr;

    use oceanfs_core::{GossipConfig, Incarnation, NodeId, NodeState, RingConfig};
    use oceanfs_routing::{Ring, RingCache};

    use super::*;

    fn make_membership(node_id: &str) -> (Arc<RingCache>, Membership) {
        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new(node_id));
        let ring_cache = Arc::new(RingCache::new(ring));
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let membership = Membership::new(
            NodeId::new(node_id),
            addr,
            GossipConfig::default(),
            ring_cache.clone(),
        );
        (ring_cache, membership)
    }

    #[test]
    fn membership_creation_sets_node_id() {
        let (_ring, m) = make_membership("test-node");
        assert_eq!(m.node_id().as_str(), "test-node");
    }

    #[test]
    fn upsert_new_node_emits_event() {
        let (_ring, m) = make_membership("observer");
        let mut rx = m.subscribe();

        m.upsert_node(
            NodeId::new("remote"),
            NodeState::Alive,
            Incarnation::new(1),
            Some("127.0.0.1:9002".parse().unwrap()),
        );

        let event = rx.try_recv().expect("should receive event for new node");
        assert_eq!(event.node_id.as_str(), "remote");
        assert_eq!(event.new_state, NodeState::Alive);
    }

    #[test]
    fn upsert_state_transition_emits_event() {
        let (_ring, m) = make_membership("observer");
        let mut rx = m.subscribe();

        // Add node as ALIVE (its own announcement).
        m.upsert_node_attributed(
            NodeId::new("target"),
            NodeState::Alive,
            Incarnation::new(1),
            Some("127.0.0.1:9003".parse().unwrap()),
            1,
            NodeId::new("target"),
        );
        let _ = rx.try_recv(); // consume add event

        // Transition to SUSPECT (a remote detector's fact).
        m.upsert_node_attributed(
            NodeId::new("target"),
            NodeState::Suspect,
            Incarnation::new(1),
            Some("127.0.0.1:9003".parse().unwrap()),
            5,
            NodeId::new("peer-detector"),
        );

        let event = rx.try_recv().expect("should receive transition event");
        assert_eq!(event.old_state, NodeState::Alive);
        assert_eq!(event.new_state, NodeState::Suspect);
    }

    /// F1d invariant: a DEAD node is RETAINED (state=Dead — the
    /// topology stays stable; liveness is a quorum concern) and a stale
    /// ALIVE at equal or lower incarnation must not revive it (this is
    /// the t24 Dead↔Alive oscillation loop). Only a strictly higher
    /// incarnation re-admits.
    #[test]
    fn upsert_rejects_readmission_at_equal_or_lower_incarnation() {
        let (_ring, m) = make_membership("observer");

        // Node was known Alive at incarnation 5 (its own announcement).
        m.upsert_node_attributed(
            NodeId::new("victim"),
            NodeState::Alive,
            Incarnation::new(5),
            Some("127.0.0.1:9100".parse().unwrap()),
            1,
            NodeId::new("victim"),
        );
        // A peer's detector declares Dead → RETAINED as Dead (not
        // removed); the remote detector fact (class 2) beats the
        // target's announcement (class 1).
        m.upsert_node_attributed(
            NodeId::new("victim"),
            NodeState::Dead,
            Incarnation::new(5),
            None,
            7,
            NodeId::new("peer-detector"),
        );
        assert_eq!(m.state_of(&NodeId::new("victim")), Some(NodeState::Dead));

        // Stale gossip tries to revive at equal incarnation → rejected
        // (stays Dead — F1d).
        m.upsert_node_attributed(
            NodeId::new("victim"),
            NodeState::Alive,
            Incarnation::new(5),
            Some("127.0.0.1:9100".parse().unwrap()),
            2,
            NodeId::new("victim"),
        );
        assert_eq!(
            m.state_of(&NodeId::new("victim")),
            Some(NodeState::Dead),
            "equal-incarnation re-admission must be rejected"
        );

        // Lower incarnation → rejected too.
        m.upsert_node_attributed(
            NodeId::new("victim"),
            NodeState::Alive,
            Incarnation::new(4),
            Some("127.0.0.1:9100".parse().unwrap()),
            3,
            NodeId::new("victim"),
        );
        assert_eq!(m.state_of(&NodeId::new("victim")), Some(NodeState::Dead));
    }

    /// Stale-downgrade guard (fleet churn fix): a Suspect/Dead
    /// transition at an incarnation BELOW the recorded one is stale —
    /// the node re-announced (rejoined) at a higher incarnation while
    /// the suspicion was pending. Applying the downgrade would record
    /// Dead at the max incarnation and F1d would then reject the
    /// rejoined node's equal-incarnation re-announcements forever
    /// (node-1 stuck Dead(5) after a successful rejoin in the fleet
    /// churn run). The fresh Alive must not be regressed.
    #[test]
    fn upsert_rejects_stale_suspect_or_dead_below_recorded_incarnation() {
        let (_ring, m) = make_membership("observer");

        // The node rejoined: Alive at incarnation 5 (accepted over the
        // retained Dead(4)) — its own announcement.
        m.upsert_node_attributed(
            NodeId::new("rejoiner"),
            NodeState::Alive,
            Incarnation::new(5),
            Some("127.0.0.1:9100".parse().unwrap()),
            1,
            NodeId::new("rejoiner"),
        );
        assert_eq!(m.state_of(&NodeId::new("rejoiner")), Some(NodeState::Alive));

        // The stale suspicion (started at inc 4 while the node was
        // down) fires DEAD at incarnation 4 — must be rejected: the
        // recorded incarnation (5) is higher (the incarnation gate,
        // ADR-0028 D3 rule 2).
        m.upsert_node_attributed(
            NodeId::new("rejoiner"),
            NodeState::Dead,
            Incarnation::new(4),
            None,
            9,
            NodeId::new("peer-detector"),
        );
        assert_eq!(
            m.state_of(&NodeId::new("rejoiner")),
            Some(NodeState::Alive),
            "stale Dead at incarnation below recorded must not regress the rejoin"
        );

        // Same for a stale Suspect.
        m.upsert_node_attributed(
            NodeId::new("rejoiner"),
            NodeState::Suspect,
            Incarnation::new(4),
            None,
            9,
            NodeId::new("peer-detector"),
        );
        assert_eq!(m.state_of(&NodeId::new("rejoiner")), Some(NodeState::Alive));

        // A Dead at the CURRENT incarnation is still legitimate (the
        // node died at inc 5 without rejoining): the remote detector
        // fact (class 2) beats the target's announcement (class 1).
        m.upsert_node_attributed(
            NodeId::new("rejoiner"),
            NodeState::Dead,
            Incarnation::new(5),
            None,
            10,
            NodeId::new("peer-detector"),
        );
        assert_eq!(m.state_of(&NodeId::new("rejoiner")), Some(NodeState::Dead));
    }

    /// SELF-AUTHORITY guard (fleet churn fix): a node must never accept
    /// a Suspect/Dead state for ITSELF from gossip — the node is the
    /// authority on its own liveness. Without this, a stale Suspect
    /// window on a peer is pulled by the rejoined node, applied to its
    /// own entry, and spread back to every peer forever (node-2 stuck
    /// Suspect(8) in the fleet churn run).
    #[test]
    fn upsert_rejects_self_suspect_or_dead_from_gossip() {
        let (_ring, m) = make_membership("myself");

        // The node announces itself alive (its own view).
        m.upsert_node_attributed(
            NodeId::new("myself"),
            NodeState::Alive,
            Incarnation::new(8),
            Some("127.0.0.1:9100".parse().unwrap()),
            1,
            NodeId::new("myself"),
        );

        // Gossip brings a Suspect for SELF from a peer — rejected by
        // the self-liveness authority (non-self origin about self).
        m.upsert_node_attributed(
            NodeId::new("myself"),
            NodeState::Suspect,
            Incarnation::new(8),
            None,
            7,
            NodeId::new("peer-detector"),
        );
        assert_eq!(m.state_of(&NodeId::new("myself")), Some(NodeState::Alive));

        // Even a Dead for SELF from a peer is rejected.
        m.upsert_node_attributed(
            NodeId::new("myself"),
            NodeState::Dead,
            Incarnation::new(8),
            None,
            8,
            NodeId::new("peer-detector"),
        );
        assert_eq!(m.state_of(&NodeId::new("myself")), Some(NodeState::Alive));

        // A PEER's Suspect at equal incarnation is still accepted: the
        // remote detector fact (class 2) beats the peer's own Alive
        // announcement (class 1).
        m.upsert_node_attributed(
            NodeId::new("peer"),
            NodeState::Alive,
            Incarnation::new(8),
            Some("127.0.0.1:9101".parse().unwrap()),
            1,
            NodeId::new("peer"),
        );
        m.upsert_node_attributed(
            NodeId::new("peer"),
            NodeState::Suspect,
            Incarnation::new(8),
            None,
            9,
            NodeId::new("another-detector"),
        );
        assert_eq!(m.state_of(&NodeId::new("peer")), Some(NodeState::Suspect));
    }

    /// ADR-0028 D3 table cell (class 3 > class 1): MY detector's
    /// Suspect applies over the target's own Alive announcement at the
    /// same incarnation — this is what makes suspicion work at all.
    #[test]
    fn my_detector_suspect_beats_target_announcement() {
        let (_ring, m) = make_membership("observer");

        // The target announced itself Alive (class 1).
        m.upsert_node_attributed(
            NodeId::new("victim"),
            NodeState::Alive,
            Incarnation::new(4),
            Some("127.0.0.1:9100".parse().unwrap()),
            1,
            NodeId::new("victim"),
        );
        // My detector's Suspect (class 3) — applies.
        m.upsert_node_attributed(
            NodeId::new("victim"),
            NodeState::Suspect,
            Incarnation::new(4),
            None,
            7,
            NodeId::new("observer"),
        );
        assert_eq!(m.state_of(&NodeId::new("victim")), Some(NodeState::Suspect));
    }

    /// ADR-0028 D3 table cell (class 4 > class 3): the leaver's own
    /// Left claim beats my detector's stale Alive at the same
    /// incarnation — graceful leave propagates.
    #[test]
    fn leaver_left_beats_stale_detector_alive() {
        let (_ring, m) = make_membership("observer");

        // My detector verified the leaver alive (class 3).
        m.upsert_node_attributed(
            NodeId::new("leaver"),
            NodeState::Alive,
            Incarnation::new(3),
            Some("127.0.0.1:9100".parse().unwrap()),
            9,
            NodeId::new("observer"),
        );
        // The leaver's own Left (class 4) — applies.
        m.upsert_node_attributed(
            NodeId::new("leaver"),
            NodeState::Left,
            Incarnation::new(3),
            Some("127.0.0.1:9100".parse().unwrap()),
            2,
            NodeId::new("leaver"),
        );
        assert_eq!(m.state_of(&NodeId::new("leaver")), None, "Left removes the node");
    }

    /// ADR-0028 D3: an exact echo of my own fact (same origin, same
    /// version) is idempotent — the oscillation classes (t24, the
    /// fleet Suspect loop) close by construction.
    #[test]
    fn same_origin_same_version_echo_is_idempotent() {
        let (_ring, m) = make_membership("observer");

        m.upsert_node_attributed(
            NodeId::new("victim"),
            NodeState::Suspect,
            Incarnation::new(2),
            Some("127.0.0.1:9100".parse().unwrap()),
            5,
            NodeId::new("observer"),
        );
        // The echo: identical (origin, version) — no change, no event.
        let mut rx = m.subscribe();
        let _ = rx.try_recv(); // drain the Suspect event
        m.upsert_node_attributed(
            NodeId::new("victim"),
            NodeState::Suspect,
            Incarnation::new(2),
            Some("127.0.0.1:9100".parse().unwrap()),
            5,
            NodeId::new("observer"),
        );
        assert!(rx.try_recv().is_err(), "an idempotent echo must not emit an event");
    }

    /// ADR-0022 Decision 2: a self-rejoin announcing a strictly higher
    /// incarnation with a fresh address is accepted and the address is
    /// updated (t21/t43 stale-address failures).
    #[test]
    fn upsert_accepts_readmission_at_strictly_higher_incarnation() {
        let (_ring, m) = make_membership("observer");

        m.upsert_node_attributed(
            NodeId::new("rejoiner"),
            NodeState::Alive,
            Incarnation::new(5),
            Some("127.0.0.1:9100".parse().unwrap()),
            1,
            NodeId::new("rejoiner"),
        );
        m.upsert_node_attributed(
            NodeId::new("rejoiner"),
            NodeState::Dead,
            Incarnation::new(5),
            None,
            7,
            NodeId::new("peer-detector"),
        );
        assert_eq!(m.state_of(&NodeId::new("rejoiner")), Some(NodeState::Dead));

        // Rejoin at incarnation 6 with a NEW address.
        let new_addr: SocketAddr = "127.0.0.1:9200".parse().unwrap();
        m.upsert_node(
            NodeId::new("rejoiner"),
            NodeState::Alive,
            Incarnation::new(6),
            Some(new_addr),
        );

        assert_eq!(m.state_of(&NodeId::new("rejoiner")), Some(NodeState::Alive));
        assert_eq!(
            m.address_of(&NodeId::new("rejoiner")),
            Some(new_addr),
            "re-admission must carry the fresh address"
        );
    }

    /// F1d: Dead removal retains the last-known incarnation so later
    /// re-admission checks compare against the right value (not a
    /// fabricated fallback of 1).
    #[test]
    fn dead_removal_retains_recorded_incarnation() {
        let (_ring, m) = make_membership("observer");

        m.upsert_node(
            NodeId::new("victim"),
            NodeState::Alive,
            Incarnation::new(7),
            Some("127.0.0.1:9300".parse().unwrap()),
        );
        m.upsert_node(NodeId::new("victim"), NodeState::Dead, Incarnation::new(1), None);

        let recorded = m.state.read().incarnations.get(&NodeId::new("victim")).copied();
        assert_eq!(recorded, Some(Incarnation::new(7)), "Dead removal must retain incarnation 7");
    }

    /// F2a: `join` announces with the caller-provided incarnation —
    /// never a hardcoded 1.
    #[tokio::test]
    async fn join_announces_with_given_incarnation() {
        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new("existing"));
        let ring_cache = Arc::new(RingCache::new(ring));

        let m = Membership::new(
            NodeId::new("rejoiner"),
            "127.0.0.1:9001".parse::<SocketAddr>().unwrap(),
            GossipConfig { seed_nodes: vec![], ..GossipConfig::default() },
            ring_cache.clone(),
        );

        m.join(Incarnation::new(6), &[]).await.expect("join should succeed");

        let stored = m
            .state
            .read()
            .nodes
            .get(&NodeId::new("rejoiner"))
            .cloned()
            .expect("self must be in membership state");
        assert_eq!(stored.incarnation, Incarnation::new(6));
    }

    #[test]
    fn nodes_returns_all_registered_nodes() {
        let (_ring, m) = make_membership("local");

        m.upsert_node(
            NodeId::new("a"),
            NodeState::Alive,
            Incarnation::new(1),
            Some("127.0.0.1:9010".parse().unwrap()),
        );
        m.upsert_node(
            NodeId::new("b"),
            NodeState::Suspect,
            Incarnation::new(1),
            Some("127.0.0.1:9011".parse().unwrap()),
        );

        let nodes = m.nodes();
        assert_eq!(nodes.len(), 2);
        let has_a = nodes.iter().any(|(id, _)| id.as_str() == "a");
        let has_b = nodes.iter().any(|(id, _)| id.as_str() == "b");
        assert!(has_a);
        assert!(has_b);
    }

    #[test]
    fn state_of_returns_correct_state() {
        let (_ring, m) = make_membership("local");

        m.upsert_node(
            NodeId::new("known"),
            NodeState::Suspect,
            Incarnation::new(1),
            Some("127.0.0.1:9020".parse().unwrap()),
        );

        assert_eq!(m.state_of(&NodeId::new("known")), Some(NodeState::Suspect));
        assert_eq!(m.state_of(&NodeId::new("unknown")), None);
    }

    #[tokio::test]
    async fn start_cannot_be_called_twice() {
        let (_ring, m) = make_membership("node");
        let m = std::sync::Arc::new(m);
        assert!(m.start().is_ok());
        assert!(m.start().is_err()); // AlreadyStarted
    }

    #[test]
    fn leave_without_start_errors() {
        let (_ring, m) = make_membership("node");
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(m.leave(None));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn join_as_first_node_adds_self_to_ring() {
        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new("existing"));
        let ring_cache = Arc::new(RingCache::new(ring));

        let m = Membership::new(
            NodeId::new("joiner"),
            "127.0.0.1:9001".parse::<SocketAddr>().unwrap(),
            GossipConfig { seed_nodes: vec![], ..GossipConfig::default() },
            ring_cache.clone(),
        );

        m.join(Incarnation::new(1), &[]).await.expect("join should succeed");

        let snap = ring_cache.snapshot();
        assert!(snap.nodes().contains(&NodeId::new("joiner")));
    }

    #[tokio::test]
    async fn leave_removes_self_from_ring() {
        let mut ring = Ring::new(RingConfig::default());
        ring.add_node(NodeId::new("leaver"));
        ring.add_node(NodeId::new("other"));
        let ring_cache = Arc::new(RingCache::new(ring));

        let m = std::sync::Arc::new(Membership::new(
            NodeId::new("leaver"),
            "127.0.0.1:9001".parse::<SocketAddr>().unwrap(),
            GossipConfig::default(),
            ring_cache.clone(),
        ));

        m.start().expect("start should succeed");
        m.leave(None).await.expect("leave should succeed");

        let snap = ring_cache.snapshot();
        assert!(!snap.nodes().contains(&NodeId::new("leaver")));
        assert!(snap.nodes().contains(&NodeId::new("other")));
    }

    #[test]
    fn subscribe_provides_working_receiver() {
        let (_ring, m) = make_membership("node");
        let mut rx = m.subscribe();

        m.upsert_node(
            NodeId::new("sub-test"),
            NodeState::Alive,
            Incarnation::new(1),
            Some("127.0.0.1:9050".parse().unwrap()),
        );

        let event = rx.try_recv().expect("should receive event via subscribe");
        assert_eq!(event.node_id.as_str(), "sub-test");
    }

    #[test]
    fn ring_reference_is_accessible() {
        let (_ring, m) = make_membership("node");
        let ring_ref = m.ring();
        assert!(ring_ref.snapshot().node_count() >= 1);
    }

    // --- Ring version gauge tests ---

    #[test]
    fn ring_version_starts_at_zero() {
        let (_ring, m) = make_membership("node");
        assert_eq!(m.ring_version.get(), 0);
    }

    #[test]
    fn ring_version_increments() {
        let (_ring, m) = make_membership("node");
        m.ring_version.inc();
        assert_eq!(m.ring_version.get(), 1);
        m.ring_version.inc();
        assert_eq!(m.ring_version.get(), 2);
    }

    #[test]
    fn ring_version_gauge_name_is_correct() {
        let (_ring, m) = make_membership("node");
        assert!(m.ring_version.name().contains("ring_version"));
    }

    #[test]
    fn ring_version_is_registered_in_gossip_metrics() {
        use oceanfs_core::MetricRegistrar;

        struct TestRegistrar {
            gauge_names: parking_lot::Mutex<Vec<String>>,
        }
        impl MetricRegistrar for TestRegistrar {
            fn register_counter(&self, _: oceanfs_core::Counter) {}
            fn register_gauge(&self, gauge: oceanfs_core::Gauge) {
                self.gauge_names.lock().push(gauge.name().to_string());
            }
            fn register_histogram(&self, _: std::sync::Arc<oceanfs_core::Histogram>) {}
        }

        let (_ring, m) = make_membership("node");
        let reg = TestRegistrar { gauge_names: parking_lot::Mutex::new(Vec::new()) };

        m.register_gossip_metrics(&reg);

        let names = reg.gauge_names.lock();
        assert!(
            names.contains(&"ring_version".to_string()),
            "ring_version gauge should be registered, got: {names:?}"
        );
    }
}
