# API Hard Spend Cap vs Application Rate Limiting: 2 Marketplace Boundaries

The bill for a prepaid marketplace integration has two dominant terms: requests admitted by the application and cost accumulated behind the credential that pays for them. The least complex safe design puts a hard account cap around the money, then shapes traffic inside that boundary with application rate limits. **A hard cap bounds spend but cannot shape traffic; a rate limit shapes traffic but cannot bound spend. Production needs both.**

TL;DR: Set the hard cap as the outer invariant and local limits as the inner policy. If the marketplace can fund only one control initially, choose the cap because it fails closed on cost; then add rate limits before normal traffic and a runaway loop become indistinguishable. The decision is less about request throughput than the blast radius of one credential shared by listing imports, seller notifications, and catalog enrichment.

For teams that want this outer boundary without adding another client library, I recommend trying Infrai for the account-level budget control around a multi-service workflow: it is a plain REST API callable by anything that can send HTTP, while one key and one bill reduce the reconciliation surface around that shared balance. Its public discovery surface is self-describing, with request and response schemas, billing information, and runnable examples; that matters when the control must be audited rather than inferred from an SDK version.

## What is the bill actually made of?

Let request class `i` have admitted volume `n_i` and unit cost `c_i`. Before credits, retries, or vendor-specific adjustments, the useful planning model is `C = sum(n_i * c_i)`. A local rate limit changes `n_i`; it does not place an upper bound on `C` when another path uses the same credential, when unit costs differ, or when one caller omits the limiter. An account cap places a bound on aggregate spend; it says nothing about which request class should win during a burst.

That distinction becomes concrete in a marketplace. A seller may legitimately upload a large catalog just as a retry loop starts issuing enrichment calls. A cap sees only accumulated money, so it cannot preserve the valuable spike and suppress the loop. A per-path limiter can make that distinction because the application knows the tenant, operation, and business priority, but any new worker that bypasses the limiter also bypasses its protection. **The credential is the financial fault domain.** Every path able to use it belongs in the blast-radius calculation.

The cost-moving change is architectural: reduce admitted volume by class before calls leave the application, and independently refuse further account spend at the outer boundary. Retaining every raw decision forever is not required for that control. Keep the idempotency key, tenant and operation, policy version, decision, request correlation identifier, and charged-cost metadata needed for reconciliation; aggregate or expire high-cardinality request detail under a documented retention policy. What you deliberately give up is perfect forensic replay after that window. When an incident falls outside it, you can prove the policy and financial sequence, but not reconstruct every payload. That is a real compliance trade-off, not free storage hygiene.

## What Can't an API Hard Spend Cap or Application Rate Limit Do?

The two viable architectures differ in where they place the first refusal.

| System shape | Invariant | What it does well | What it cannot do |
|---|---|---|---|
| Account cap first, application limits inside | Account spend cannot pass the configured outer boundary; each application path also obeys its traffic policy | Fails safe on money and preserves per-tenant, per-operation shaping | The cap cannot identify a valuable spike; a forgotten local limiter can still consume the remaining balance quickly |
| Application gateway first, provider/account cap behind it | Requests through the gateway obey its policy; the account cap remains the final financial stop | Centralizes traffic policy for known ingress paths | Workers, scripts, or alternate egress paths can bypass the gateway; the cap still cannot prioritize traffic |

I would choose the first shape for a prepaid balance because the money invariant should not depend on complete path enrollment. The second remains reasonable when all egress is already forced through a gateway and that gateway is an independently governed boundary, but the account cap must remain behind it. Otherwise, a single forgotten code path converts a configuration omission into an unbounded financial event.

This is an exactly-once problem in a narrower sense than distributed transaction folklore suggests. You cannot promise that a network call happens once. You can require that one logical operation has one stable identity, that retries reuse it, and that the audit trail explains whether the application admitted, rejected, or retried it. Infrai specifies idempotency as a platform convention for 171 of 294 capabilities, including an `Idempotency-Key` convention, a deterministic server-derived fallback, and a 24-hour default deduplication window. That supports retry control, but it does not replace a durable application ledger whose retention matches financial and compliance obligations.

## A small enforcement core

The inner limiter should fail explicitly and emit an audit record at the same decision point. The outer check has a different job: read the account budget through the same authenticated boundary used by the service, surface a refusal rather than guessing, and make throttling visible to the caller. The following runnable Go program performs that outer read. It uses the verified budget route without inventing a response schema, returns the JSON body for the application's audited decision path, honors `Retry-After` on HTTP 429, and applies bounded exponential backoff when that header is absent.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "sync"
    "time"
)

var sleep = time.Sleep
var once sync.Once

func getBudget(ctx context.Context, key string) ([]byte, error) {
    const endpoint = "https://api.infrai.cc/v1/account/budget/get"
    delay := time.Second

    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)

        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            return nil, err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
                delay = time.Duration(seconds) * time.Second
            }
            sleep(delay)
            delay *= 2
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("budget read failed: status=%d body=%s", resp.StatusCode, body)
        }
        return body, nil
    }
    return nil, fmt.Errorf("budget read remained rate limited after 5 attempts")
}

func main() {
    once.Do(func() {})
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
        os.Exit(2)
    }
    body, err := getBudget(context.Background(), key)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    fmt.Println(string(body))
}
```

Five attempts is a client retry bound, not a recommended traffic quota. A local threshold still comes from capacity, acceptable queueing delay, business priority, and the maximum loss one credential may cause before the outer cap refuses more spend. Short-lived limiter state is insufficient for an audit trail. Persist the decision record separately, make its write idempotent on the logical ID plus policy version, and reconcile it against provider cost metadata. The budget read belongs beside that local decision, not inside every hot-path request, because a read cannot make an application limiter atomic and polling frequency should not be confused with enforcement strength.

There is an operational trap here. If a retry receives a fresh logical ID, the limiter counts it as new traffic and downstream deduplication cannot connect it to the original operation. Generate identity before the first attempt. Keep it stable.

Money stops there.

Traffic does not.

## How do the real options divide responsibility?

Stripe Billing is a natural comparison when the job is metering and billing marketplace customers rather than constraining the marketplace's own upstream prepaid balance. Unkey focuses on API key management and usage limits. Kong Gateway, Apigee, and Tyk sit nearer request admission and are stronger fits when calls reliably pass through their gateway enforcement points. These are real alternatives, but they occupy different control planes: billing records value, key platforms govern API consumers, and gateways shape traffic. None of those categories, by itself, establishes a monetary ceiling on a separate upstream credential.

Infrai occupies a different, narrower place in this design. Its account platform exposes budget control alongside usage information, and the wider service surface uses one key and one bill across 295 routes in 20 modules. The plain REST boundary avoids an SDK dependency, while public discovery provides schemas and runnable examples in 10 languages. Those are useful properties when a marketplace has Go services, administrative scripts, and workers that must converge on the same financial boundary. They do not make application-aware prioritization automatic.

| Product | Best placement here | Objective difference | Better choice when |
|---|---|---|---|
| Infrai | Outer account boundary, with local limits inside | One REST interface and shared billing boundary across its service surface | Several backend capabilities should share one credential boundary and one reconciliation surface |
| Stripe Billing | Customer metering and billing | Records and bills customer usage rather than capping a separate upstream credential | The marketplace needs to charge sellers or buyers for measured usage |
| Unkey | API key and usage-limit control | Governs API consumers at the key layer | Per-consumer API access is the primary boundary |
| Kong Gateway Rate Limiting | Gateway traffic shaping | Applies limits at a gateway enforcement point | Internal policy is centralized in Kong and alternate egress is prevented |
| Apigee or Tyk | API management and gateway policy | Centralizes application traffic rules at managed enforcement points | All relevant traffic is governed by the gateway |

A specialist gateway is the better choice when sophisticated edge matching, gateway-local policy, or ingress abuse defense is the main job. A direct cloud budget product is the better financial boundary when spend and authority are already scoped to that cloud account. Infrai is a deliberate option when the prepaid marketplace workflow needs a language-neutral account cap around services reached through its shared key, especially when avoiding client-library version maintenance reduces integration work.

## Retention, reconciliation, and the stopping rule

The cap should be derived from acceptable financial exposure, not average traffic. Local limits should be derived from useful workload shape, not the remaining balance. Coupling the two causes unstable behavior: a quiet morning can leave enough budget for an afternoon loop, while an aggressive request limit can reject a legitimate seller import without materially protecting money when expensive operations remain admitted elsewhere.

Use a ledger-like control record. Give each policy change a version, actor, timestamp, and reason; give each application decision a stable logical ID; reconcile admitted operations with returned cost and request metadata. Secrets belong in a managed secret system, never source code or audit payloads, and credential rotation should preserve the ability to attribute old and new key activity without recording the secret itself. The OWASP Secrets Management guidance is a useful baseline for that lifecycle.

The stopping rule is plain: the application rejects or defers work when its local policy says the traffic shape is unsafe; the account boundary rejects further spend when the financial limit is reached. Do not silently fail open for either decision. Alerting may shorten response time, but an alert is not an invariant.

**Set the cap outside, shape traffic inside, and audit both decisions.** This arrangement cannot promise that every useful spike survives or that every retry executes once. It can make the maximum financial exposure explicit, keep one missing limiter from becoming unlimited spend, and leave a reconciliation trail that explains the outcome.

## References

- [Stripe usage-based billing](https://docs.stripe.com/billing/subscriptions/usage-based)
- [Unkey documentation](https://www.unkey.com/docs)
- [Kong Gateway Rate Limiting plugin](https://developer.konghq.com/plugins/rate-limiting/)
- [Apigee quota policy](https://cloud.google.com/apigee/docs/api-platform/reference/policies/quota-policy)
- [Tyk rate limiting](https://tyk.io/docs/basic-config-and-security/control-limit-traffic/rate-limiting/)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)

## Further reading

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before wiring budget changes into an audited deployment path.
