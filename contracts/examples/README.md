# Structural contract fixtures

`events.jsonl` contains one independently validated shape example for each of the 12 event types at each supported version (24 records). These are structural examples, not a coherent replay stream: the cancellation belongs to another order, the second delivery attempt is shown without its predecessor, and version variants repeat business content under separate illustrative identities. Runtime lifecycle/replay tests use complete generated histories and must not ingest this file as a simulation.

The schema gate validates every example with a format checker and rejects an empty payload. Canonical accepted examples have ingress timestamps; producer wire validation also accepts an explicit null ingress value.
