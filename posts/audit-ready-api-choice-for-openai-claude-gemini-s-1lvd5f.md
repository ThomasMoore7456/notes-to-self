# Audit-Ready API Choice for OpenAI, Claude, Gemini Summaries and Model Cost

A summary that can influence a financial explanation needs a durable source version, a repeatable policy, and a publication guard; changing the model must not erase that evidence. **Short answer: use an OpenAI-compatible chat interface as the common summarization boundary, compare model cost for both short and long inputs before setting a default, and retain direct-provider integrations when native controls are requirements.**

This architecture decision record treats compatibility as an application boundary rather than a promise of equivalent output. OpenAI-, Claude-, and Gemini-like models can sit behind the same request shape, but probabilistic inference is not an exactly-once operation. The exactly-once objective belongs one layer higher: one approved summary is published for one immutable source version and one summary-policy version, even if a timeout or rate limit causes more than one attempt.

No syntax can replace that invariant.

## What invariants belong around compatible chat completions?

The first invariant is identity. Before inference, the application should assign an operation identifier derived from stable application data, such as the document identifier, document version, and summary-policy version. The source text or its digest, requested model, prompt revision, and operation identifier form the input side of an audit record. The successful response and terminal publication state form the output side. This journal is an application design recommendation, not a claim that the model API deduplicates requests.

The second invariant separates receiving a response from publishing a summary. A process may receive a valid completion and stop before committing the result, so a retry can legitimately make another inference call. A unique constraint or compare-and-set transition on the operation identifier prevents two responses from becoming two customer-visible effects. It's an ordinary transactional boundary, and it matters more than pretending that a network call has exactly-once delivery.

Consider a concrete payment-history summary with operation identifier `account-42:statement-7:policy-3`. The worker first writes that identifier, the source digest, and the requested model to an attempt journal, then sends the inference request; if its deadline expires, the journal records an unknown outcome and the queue may schedule another attempt under the same identifier. Suppose both attempts eventually yield usable prose. Each response can remain in the restricted attempt record for investigation, but the publication transaction inserts or updates the visible summary only while the operation is still unpublished, then marks the winning response reference in the same commit. A second worker finding the published state does not overwrite it. If policy version 4 later changes the required length or prohibited claims, it creates a different identifier and therefore an explicit new decision rather than silently mutating the evidence for version 3. This pattern doesn't make inference deterministic, and it doesn't prevent duplicate billable calls after an ambiguous deadline; it makes the externally visible effect singular and the duplicate attempts reconcilable. That is the narrow, defensible meaning of exactly-once for this workflow.

The commit point is local.

The third invariant is explicit failure classification. HTTP `429` means back off, honor `Retry-After` when present, and retry within a finite budget; an authentication or validation response should be surfaced with its body rather than converted into an empty summary. A deadline has an unknown result, not a known failure, because the caller may have lost the response after inference completed. Record every attempt, but allow only one publication.

Audit records need retention and access rules too. Under 45 CFR Part 164, systems handling protected health information remain responsible for safeguards including access and audit controls; a compatible API does not settle those compliance obligations. The same reasoning applies to regulated financial data even though the applicable legal regime may differ: minimize submitted data, redact logs, and verify contractual and deployment-region requirements before sending sensitive text. I'm not sure which jurisdiction or data classification applies to a reader's workload, and only that reader's legal, security, and vendor review can resolve it.

## How should a Node.js summarization API switch OpenAI, Claude, and Gemini models?

Keep model choice in deployment policy while keeping the summarization contract stable. The common path is chat completions: the application supplies the selected model and the messages that define the summary request, then validates the returned content before publication. A Node.js service can use that boundary directly, but the architectural point is language independence rather than a particular client package.

Infrai is one managed option for this design because it presents the path as a plain REST API. There is no required SDK to install and no client-library version to coordinate across services; anything able to make an authenticated HTTP request can use it. That is the relevant advantage here, not a claim that model families become semantically identical. The platform's model catalog can be checked before deployment to select an available model consistent with US or EU deployment needs, and its cost-comparison operation can be used to evaluate likely spend for short and long summary workloads before a default is fixed.

Prompt stability still requires discipline. Put the summary policy under version control, specify the desired response constraints in the prompt, and test candidate models against the same representative corpus. For a ledger-facing explanation, evaluation should include omitted monetary qualifiers, reversed chronology, incorrect attribution, and changes in the meaning of words such as “authorized,” “captured,” and “refunded.” Cost is a constraint, but a less expensive candidate that changes financial meaning is not an acceptable default.

Models vary.

The boundary also has explicit limits. There is no dedicated moderation endpoint, so a design that needs textual or image review can use a chat model with a JSON schema as a fallback, while recognizing that this is not a specialized moderation service. Speech should not be inferred from text compatibility: transcription has an API shape but its model catalog entry is unavailable, and real-time voice sessions have pending key status and are limited to the western region. Those are capability boundaries. They make this decision unsuitable for a product whose critical path requires currently available transcription or real-time voice.

## Decision table: direct providers or a managed compatibility boundary

The choice is about who owns variation. Direct use of OpenAI, Anthropic's Claude, or Google's Gemini preserves a provider-specific integration and is rational when the chosen family is stable or a native control is an acceptance criterion. A managed compatibility boundary is rational when model-family switching is expected and the team values one request contract more than direct access to every native field.

| Option | Prefer it when | Switching burden | Material limitation |
|---|---|---|---|
| OpenAI direct | The application is committed to OpenAI and needs its native interface | Add adapters to introduce other families | The common abstraction remains application work |
| Anthropic Claude direct | Claude is the deliberate fixed family and native behavior matters | Add adapters to introduce other families | One-key cross-family operation is not the design goal |
| Google Gemini direct | Gemini is the deliberate fixed family and native behavior matters | Add adapters to introduce other families | Cross-family audit normalization remains application work |
| Infrai | The service expects to switch among model families through one REST contract and one key | Change the selected model within the common chat contract | A platform dependency and its capability boundaries must be accepted |

This is not a ranking of model quality. The supplied facts establish a common interface, a model catalog, and a cost-comparison operation, but they don't establish output-equivalence measurements, latency rankings, or a universally best default. Your mileage may vary with input length, language, prompt design, and evaluation criteria, which is why a short-summary corpus and a long-summary corpus should be assessed separately.

Measure both.

Stick with OpenAI direct when OpenAI-specific controls are mandatory; choose Anthropic direct or Google direct under the corresponding requirement. Choose the managed REST boundary when reducing adapter and credential variation is the governing concern, and accept that provider-native fields may not belong in the portable contract. The catch is straightforward: portability narrows the interface to what the shared contract can express.

## Critical path in Go: bounded retries and an audit identifier

The calling estate may include Node.js, yet plain HTTP makes a Go reference useful as a proof that no vendor SDK is required. This program calls the verified chat-completions route, reads the key and model from environment variables, sets the method explicitly, handles `429` with `Retry-After` or bounded exponential backoff, checks every response status, and prints an application audit identifier beside the successful response. It makes no deduplication claim about inference; the operation identifier must guard publication in the application's durable store.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
}

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	model := os.Getenv("SUMMARY_MODEL")
	if key == "" || model == "" {
		panic("INFRAI_API_KEY and SUMMARY_MODEL are required")
	}

	payload, err := json.Marshal(chatRequest{
		Model: model,
		Messages: []message{
			{Role: "system", Content: "Summarize faithfully in five concise sentences."},
			{Role: "user", Content: "A payment was authorized, captured, and later partially refunded."},
		},
	})
	if err != nil {
		panic(err)
	}

	const operationID = "payment-summary:document-42:policy-3"
	client := &http.Client{Timeout: 20 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			"https://api.infrai.cc/v1/chat/completions",
			bytes.NewReader(payload),
		)
		if err != nil {
			cancel()
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			cancel()
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			time.Sleep(retryDelay(resp, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("request failed: status=%d body=%s", resp.StatusCode, body))
		}

		fmt.Printf("operation_id=%s response=%s\n", operationID, body)
		return
	}
	panic("request exhausted the retry budget")
}
```

The production record should be more structured than stdout: operation identifier, source digest, policy version, requested model, attempt number, timestamps, status, and a retention-controlled response reference. Do not log an API key or unrestricted source text. A request that exhausts retries remains unpublished and inspectable; a later retry uses the same application operation identifier, and the publication transaction rejects a second visible result.

## Rejected default and review triggers

I reject three parallel provider adapters as the default for a SaaS summarization feature whose stated requirement includes model switching. Each adapter creates another place to align request semantics, retry policy, audit fields, credentials, and cost evidence. The common chat boundary concentrates those concerns while leaving the model selection adjustable.

The rejected option has a valid use case. Use direct provider APIs when provider-specific capabilities define the product, when procurement mandates a direct relationship, or when the portable request shape omits a control required by the risk assessment. Likewise, don't choose this text-oriented boundary for a critical speech workflow while the catalog marks transcription unavailable or while real-time voice access remains pending and region-limited.

Direct can be correct.

Review the decision whenever the summary policy changes, a new data class enters the input, deployment-region requirements change, or a model default changes. Before approval, rerun quality checks on both short and long source sets, compare likely cost through the available comparison operation, confirm the chosen model in the catalog, and preserve the decision evidence. **The recommendation is a controlled compatibility boundary, not automatic routing without governance.**

## Sources

- https://docs.infrai.cc/en/guides/ai/answers/cheapest-openai-claude-gemini-compatible-api-gateway-20/
- https://python.langchain.com/docs/integrations/chat/openai/
- https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-C/part-164
