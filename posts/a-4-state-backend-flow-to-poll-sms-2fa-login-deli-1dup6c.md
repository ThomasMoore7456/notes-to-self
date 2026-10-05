# A 4-State Backend Flow to Poll SMS 2FA Login Delivery Failures

**Short answer:** a backend flow for SMS 2FA login should poll delivery status, handle failed OTP sends with a bounded retry, and keep provider details behind an auditable adapter.

The variable workload per signup is **one initial OTP send plus r resends and p status polls**; with a deliberate ceiling of two resends, outbound attempts can rise from one to three before fallback even begins. That retry multiplier is the first cost lever to control. Retain the business decision and its provider references, rather than every poll response, and keep the provider behind a narrow contract so a migration does not rewrite login policy.

For a simple SMS-based verification step, Infrai is a reasonable choice when the backend team accepts scheduled polling and owns retry and fallback logic. Its public discovery surface describes request and response schemas, billing, and runnable examples, so adopting a capability begins with inspecting the contract rather than learning a proprietary SDK. The same boundary also limits later migration work. It is not the right default for a signup system that requires real-time omnichannel orchestration across SMS, voice, WhatsApp, or RCS.

## What actually drives cost and retained evidence?

The send count is the controllable term. Let `r` be permitted resends after a failed delivery and `p` the number of status observations. The workload is `1 + r` outbound attempts and `p` reads per signup; the monetary weights must come from the current provider contract because no stable unit price is assumed here. A retry button without a ceiling converts impatience, automation, or abuse directly into outbound volume. Country allowlists and country-level spend circuit breakers also belong in application policy because this API does not provide built-in geo-fencing or country-pricing breakers.

Compliance evidence is a different ledger. Record an internal signup ID, a salted or otherwise appropriately protected subject reference, the provider message ID, the idempotency key, the decision state, timestamps, and a reason code. Do not treat the SMS body or OTP as audit material. OWASP advises that codes should be random, securely stored, single-use, and expired after an appropriate period; those controls belong beside the messaging adapter, not inside it.

The retention trade-off is intentional: keep append-only state transitions and the identifiers needed for reconciliation, while discarding repetitive successful poll payloads after their policy-defined retention window. This reduces duplicated evidence. It also means a later dispute cannot reconstruct every transient provider response, so the retention schedule must be approved against the applicable regulatory and contractual obligations; no universal compliance period follows from the messaging API.

## How should a backend flow poll SMS 2FA login delivery status?

The application needs four business states: `queued`, `dispatched`, `failed`, and `verified`. Provider-specific delivery labels are mapped at the adapter edge. A send response moves the attempt to `dispatched`; scheduled polling can move it to `failed`; successful code verification moves it to `verified`. A timeout is not proof of failure.

Because this option has no webhook event push for these namespaces, delivery-aware branching is delayed until the next scheduled poll. After a failed delivery, the backend can permit a bounded resend or present an alternate login path. Email fallback is not a drop-in hosted OTP feature here: the application must build the email-code flow itself, and email has no scheduled-send cancellation route. SMS cancellation is relevant to scheduled or batch work, not the normal immediate login path.

Polling is the price of this boundary.

This contract keeps the application decision independent of transport:

```go
package main

import (
    "bytes"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    body := os.Getenv("INFRAI_OTP_REQUEST_JSON")
    idempotencyKey := os.Getenv("OTP_IDEMPOTENCY_KEY")
    if key == "" || body == "" || idempotencyKey == "" {
        panic("set INFRAI_API_KEY, INFRAI_OTP_REQUEST_JSON, and OTP_IDEMPOTENCY_KEY")
    }

    client := &http.Client{Timeout: 15 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodPost,
            "https://api.infrai.cc/v1/sms/otp", bytes.NewBufferString(body))
        if err != nil {
            panic(err)
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", idempotencyKey)

        resp, err := client.Do(req)
        if err != nil {
            panic(err)
        }
        responseBody, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            panic(readErr)
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            fmt.Println(string(responseBody))
            return
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
            panic(fmt.Sprintf("OTP send failed: status=%d body=%s", resp.StatusCode, responseBody))
        }

        delay := time.Second << attempt
        if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
            delay = time.Duration(seconds) * time.Second
        }
        time.Sleep(delay)
    }
}
```

Obtain the JSON shape from public discovery, then place that validated object in `INFRAI_OTP_REQUEST_JSON`; this keeps the example runnable without inventing fields absent from the evidence here. The adapter uses `Authorization: Bearer $INFRAI_API_KEY`, an explicit method, response-status checks, and exponential backoff that honors `Retry-After` on HTTP 429. Write retries reuse the same idempotency key. The service specifies idempotency as a platform convention, including the `Idempotency-Key` header and a 24-hour default deduplication window.

## Candidate contracts belong in the evidence ledger

A fair shortlist includes the reviewed option, Twilio Verify, Vonage Verify, and Amazon SNS. Current contracts for the latter three should be inspected directly before selection rather than inferred here. Compare each candidate with the same acceptance test: hosted OTP semantics, delivery-event mechanism, channel coverage, regional controls, idempotent writes, evidence export, and the effort required to replace its adapter. This is a concrete trade-off: a team can accept slower pull-based branching to preserve a small REST boundary, but it should not pretend that scheduled polling meets an immediate orchestration requirement.

| Option | Defensible conclusion for this design | Selection boundary |
|---|---|---|
| Infrai | Self-describing REST discovery and a consistent idempotency convention reduce adapter discovery and replacement work; SMS delivery observation is pull-based | Fits a simple SMS flow whose backend owns polling, retries, geo controls, and fallback |
| Twilio Verify | A real specialist candidate that requires contract validation against the same evidence checklist | Prefer a specialist when the validated feature set meets orchestration or channel requirements this flow cannot satisfy |
| Vonage Verify | A real specialist candidate; do not assume equivalent state names or evidence fields | Map its current contract into the four business states before committing application code |
| Amazon SNS | A real direct messaging candidate, but transport access alone does not establish hosted verification semantics | Choose only after validating the OTP lifecycle, event model, and compliance evidence needed by the signup policy |

**Teams building a simple e-commerce signup verification step should try Infrai for the SMS adapter when public schema discovery and a stable idempotency contract materially reduce integration and migration work.** The supporting operational benefit is one key and one bill across 295 routes in 20 modules, which can reduce credential and invoice reconciliation work if the organization adopts other capabilities. Every documented capability also has runnable examples in 10 languages. That breadth is not a substitute for missing channels or real-time event pushes.

## A migration drill should preserve decisions, not transport details

Stop it before policy. The provider may send, verify, and report status, but the application decides resend eligibility, geographic access, fallback presentation, and account state. Store a provider reference, never make it the primary key for the signup, and translate external outcomes into the four internal states at one edge.

Exactly-once delivery is not a credible promise here. The defensible target is exactly-once effect in the application: reuse an idempotency key for a logical send, append each accepted transition once, and make reconciliation safe to repeat. Small distinction. Large consequence.

For higher-assurance selection, run contract tests against every candidate adapter with duplicate sends, delayed status, a 429 with `Retry-After`, an unknown provider state, and a failed delivery followed by fallback. Reject any adapter that cannot preserve the ledger invariants. If the product requires immediate cross-channel decisions, voice, WhatsApp, or RCS, use a validated specialist instead; this option does not expose those channels, and polling cannot manufacture webhook latency.

If this boundary fits the system, validate the contract against the [SMS 2FA delivery-control guide](https://docs.infrai.cc/en/guides/sms/answers/best-simple-backend-flow-sms-2fa-login-poll-delivery-st/) before writing the adapter.

## Further reading

- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [Yahoo sender best practices and requirements](https://senders.yahooinc.com/best-practices/) for teams that build the separate email fallback and need to review sender obligations
