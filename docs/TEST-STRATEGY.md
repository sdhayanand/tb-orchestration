# Test strategy across the platform

| Level | Tool | Where | What it proves |
|---|---|---|---|
| Unit | JUnit 5 / pytest | every repo | mapping (XSLT, XML⇄JSON), validation, HMAC, id generation, Beam DoFns (`TestPipeline`, `PAssert`) |
| Integration | Testcontainers (Postgres, Pub/Sub emulator, Artemis, IBM MQ) | Java repos, `mvn verify` | outbox → Pub/Sub, subscriber → DB, JMS ⇄ Pub/Sub bridges |
| Contract | Postman/Newman, SoapUI | this repo | REST + SOAP contracts incl. error shapes |
| End-to-end | `tb-platform-infra/local/e2e.sh` | compose stack, CI nightly | every integration path once, including the DirectRunner Beam job |
| Load | JMeter | this repo | latency SLO and error rate under POS-like concurrency |
| Pipeline | Beam `TestStream` | tb-order-events-dataflow | windows, triggers, late data, DLQ |
| Data | `BigQueryCheckOperator`, reconciler | DAGs, migration repo | data-level correctness (legacy vs new side) |
| Infra | `terraform validate/plan` on PR | tb-platform-infra | drift-free infrastructure changes |
