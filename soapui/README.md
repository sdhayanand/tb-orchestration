# SoapUI — SOAP contract tests

`tb-legacy-oms-soapui-project.xml` (SoapUI 5.7 open source) covers:

| Suite | Steps | Asserts |
|---|---|---|
| Legacy OMS contract | `SubmitOrder` → `GetOrderStatus` → invalid store | SOAP Response, Not SOAP Fault, **Schema Compliance** against the WSDL/XSD, XPath `Status = NEW`, SLA 1000 ms, SOAP Fault on bad input |
| Order intake SOAP adapter | legacy `SubmitOrderRequest` to `order-intake-api /ws` | the XSLT path accepts the same XML the OMS does |

```bash
# GUI: File → Import Project. Project properties omsEndpoint / intakeEndpoint point at the local stack.
# Headless (open-source runner):
docker run --rm -v "$PWD":/project --network host smartbear/soapuios-testrunner:latest \
  -s "Legacy OMS contract" -r -j -f /project/reports /project/tb-legacy-oms-soapui-project.xml
```

Why it matters for the role: the legacy estate is SOAP/XSD; contract tests against the *WSDL*
(schema compliance, faults) are what catch an XSD change in the OMS before the XSLT in the
adapter silently maps nulls.
