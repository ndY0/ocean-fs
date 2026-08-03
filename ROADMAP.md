# ROADMAP

- audit the for concision, code smells code reuse, class graph, complexity, ...
- audit for performance on hot path
- dont forget about encryption !
- discuss implementing real production backend store for production and load tests
- dicuss GC strategy : allow to choose a strategy prioritize either compaction / cleanup speed, or throughput
- audit for performance improvement on the critical path
- lots of networking : brainstorm network optimisations : batching, compression, load/dc aware router optimisation, maybe introduce network topologies ? maybe better off with a weighted routing, based on cyclic connection performance checks
- audit solution security
- incorporate tracing, with agnostic tracing backend (pluggable)
- work on user authentication (scopes, per bucket, mechanisms offered)
- add event hook on bucket, prefix, blob and some system events (wich ones tbd)
- work on cloud optimisations : server plateform optimisations, bare metal opts, vm opts, contenerization opts
- conceive a platform supervision
- work on a stress test suite
- implement complexe scenarios : test all degraded modes, extreme payloads, edge cases