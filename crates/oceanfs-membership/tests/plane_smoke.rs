//! Integration smoke tests for the membership plane wire (ADR-0028 f1).
//!
//! Verifies the generated code end to end:
//! 1. `MembershipEntry` carries `version` + `origin` through a prost
//!    wire round-trip (the attribution fields D3 introduces).
//! 2. The `ProbeRpc` service (spec §12.3, D2) serves direct and
//!    indirect probe requests over a real tonic server.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use oceanfs_core::proto::membership::{MembershipEntry, ProbeRequest, ProbeResponse};
use oceanfs_network::gossip::{probe_rpc_client::ProbeRpcClient, probe_rpc_server::ProbeRpc};
use prost::Message;
use tonic::transport::Server;

// ---------------------------------------------------------------------------
// 1. Attributed entry wire round-trip
// ---------------------------------------------------------------------------

#[test]
fn attributed_membership_entry_survives_wire_roundtrip() {
    let entry = MembershipEntry {
        node_id: Some(oceanfs_core::proto::common::NodeId { id: "node-1".to_string() }),
        state: 1, // SUSPECT
        incarnation: 5,
        address: "10.0.0.2:9002".to_string(),
        last_seen: None,
        version: 7,
        origin: "node-2".to_string(),
        grpc_address: "10.0.0.2:9001".to_string(),
        // ADR-0029 D2: the manifest field is an optional schema
        // addition — absent here (a pre-manifest entry).
        manifest: None,
    };

    let mut buf = Vec::new();
    prost::Message::encode(&entry, &mut buf).expect("encode");

    let decoded = MembershipEntry::decode(buf.as_slice()).expect("decode");
    assert_eq!(decoded.version, 7, "version must survive the wire");
    assert_eq!(decoded.origin, "node-2", "origin must survive the wire");
    assert_eq!(decoded.incarnation, 5);
    assert_eq!(decoded.state, 1);
    assert_eq!(
        decoded.node_id.as_ref().map(|n| n.id.as_str()),
        Some("node-1"),
        "node_id must survive the wire"
    );
}

// ---------------------------------------------------------------------------
// 2. Probe RPC over a real tonic server
// ---------------------------------------------------------------------------

/// Trivial probe handler: acks every probe with the local incarnation.
#[derive(Clone, Default)]
struct EchoProbeHandler {
    incarnation: u64,
}

#[tonic::async_trait]
impl ProbeRpc for EchoProbeHandler {
    async fn probe(
        &self,
        _request: tonic::Request<ProbeRequest>,
    ) -> Result<tonic::Response<ProbeResponse>, tonic::Status> {
        Ok(tonic::Response::new(ProbeResponse { ack: true, incarnation: self.incarnation }))
    }
}

async fn start_probe_server() -> ProbeRpcClient<tonic::transport::Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(oceanfs_network::gossip::probe_rpc_server::ProbeRpcServer::new(
                EchoProbeHandler { incarnation: 42 },
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    ProbeRpcClient::connect(format!("http://{addr}")).await.unwrap()
}

#[tokio::test]
async fn direct_probe_roundtrips_over_tonic() {
    let mut client = start_probe_server().await;

    let response = client
        .probe(tonic::Request::new(ProbeRequest {
            target: Some(oceanfs_core::proto::common::NodeId { id: "node-1".to_string() }),
            origin: Some(oceanfs_core::proto::common::NodeId { id: "node-0".to_string() }),
            is_indirect: false,
        }))
        .await
        .expect("direct probe RPC");

    let body = response.into_inner();
    assert!(body.ack, "direct probe must be acknowledged");
    assert_eq!(body.incarnation, 42, "ack must carry the target's incarnation");
}

#[tokio::test]
async fn indirect_probe_roundtrips_over_tonic() {
    let mut client = start_probe_server().await;

    let response = client
        .probe(tonic::Request::new(ProbeRequest {
            target: Some(oceanfs_core::proto::common::NodeId { id: "node-2".to_string() }),
            origin: Some(oceanfs_core::proto::common::NodeId { id: "node-0".to_string() }),
            is_indirect: true,
        }))
        .await
        .expect("indirect probe RPC");

    let body = response.into_inner();
    assert!(body.ack, "relay probe must be acknowledged");
    assert_eq!(body.incarnation, 42);
}
