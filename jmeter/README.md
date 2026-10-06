# JMeter — order intake load test

`order-intake-load.jmx` models store POS terminals submitting orders: a thread per terminal,
`|`-delimited CSV of stores/SKUs/channels (the last column is a JSON fragment: `shipTo` for SHIP_TO_HOME,
`rental` for RENTAL orders, otherwise the currency), think time, `POST /v1/orders` → assert 201 + `status == CREATED` + < 500 ms,
then `GET /v1/orders/{id}`. `-JmaxMs=` sets the latency budget (default 500).

```bash
# local stack (tb-platform-infra/local) — 20 terminals, 20 orders each
jmeter -n -t order-intake-load.jmx -l results.jtl -e -o report -Jhost=localhost -Jport=8080 -Jusers=20 -Jloops=20
# GKE (external LB) through Apigee
jmeter -n -t order-intake-load.jmx -l results.jtl -Jprotocol=https -Jhost=api.tb-otd.example -Jport=443 -JapiKey=$KEY -Jusers=100 -Jramp=60 -Jloops=100
```
CI runs a short smoke profile (5 users × 5 loops) against the compose stack and publishes the HTML report as an artifact.

What to watch: p95 of `POST /v1/orders` (the DB transaction + outbox insert), error % (should be 0 — the
outbox decouples Pub/Sub from the request path), and in the stack: `outbox` relay lag, Pub/Sub publish
rate, inventory-service ack latency, HPA scale-out on GKE.
