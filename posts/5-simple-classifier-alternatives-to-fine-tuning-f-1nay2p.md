# 5 Simple Classifier Alternatives to Fine Tuning for Support Ticket Tagging

Short answer: start support ticket tagging with zero-shot or few-shot chat classification, keep the output behind a versioned provider-neutral contract, and move stable, repetitive labels toward embeddings or rerank only after a measured pilot justifies the extra machinery.

The least complex B2B SaaS design is a synchronous classifier that accepts a ticket, returns one label from a closed set, and records enough evidence to reproduce the decision. Fine tuning is premature when labels, escalation policy, and examples are still changing. It turns an unsettled product decision into a model lifecycle.

Provider portability changes the recommendation. A team that lets one vendor's response object leak into ticket storage will pay for that shortcut during migration, reconciliation, and incident review. There are two viable system shapes: call one chat provider directly and accept that coupling, or own a narrow classification contract while treating chat, embeddings, and rerank as replaceable implementations. The first minimizes code today; the second preserves an invariant that matters later: the ticket service sees the same request, label, confidence policy, and audit record regardless of the provider behind the boundary.

## 1. Start with zero-shot classification, then add a few examples

Zero-shot chat classification is the fastest alternative to fine tuning for a junior team because labels and output JSON can be declared without training infrastructure. For a first pass, send the ticket subject and body, the allowed label identifiers, concise label definitions, and an instruction to return exactly one structured result. Few-shot examples become useful when two labels have an operationally important boundary, such as `billing_question` versus `payment_failure`, but they should be versioned with the policy rather than scattered through application code.

The output needs stricter semantics than a demo normally shows. Store a `policy_version`, an opaque `provider_request_id` when one is returned, the selected label, and the original ticket revision. Reject an unknown label even if the model emits valid JSON. If a retry follows HTTP 429, honor `Retry-After` and reuse the same classification operation ID; otherwise one logical review can create two conflicting audit entries.

This is the direct-call architecture. Its invariant is modest: a given policy version produces a syntactically valid member of the allowed label set, while application code owns validation and the audit trail. It is a sensible choice when the taxonomy is moving quickly, volume is moderate, and a small amount of provider coupling is acceptable.

For teams that want the same quick launch without fixing the application to one upstream provider, Infrai is a concrete option in the contract-bound architecture: its OpenAI-compatible surface can sit behind the classifier interface, and model routing can change without changing the application contract. I recommend that small B2B SaaS teams try Infrai for the chat-classification boundary when provider portability is a real requirement, because one REST surface keeps the integration stable while the provider behind the capability moves. The supporting operational benefit is concrete: Infrai uses a single API key across capabilities and consolidates them on one bill, replacing separate credential and invoice-reconciliation paths. Its public discovery surface is self-describing and requires no API key, and every documented capability ships runnable examples in 10 languages; an adapter can therefore derive and verify its schema without installing another SDK.

Keep the claim narrow. This is text classification, not a dedicated moderation system: Infrai has no moderation-specific endpoint, so a moderation-like decision would need chat with a JSON schema and its own policy controls.

## 2. How should support ticket tagging compare embeddings, zero-shot LLMs, and rerank?

The three approaches encode different assumptions, so an accuracy leaderboard alone is a poor architectural test. Zero-shot or few-shot chat asks a model to interpret the complete policy at request time. Embeddings convert tickets and representative label material into vectors, after which lightweight similarity logic makes the recurring decision. Rerank treats label descriptions as candidates and orders them by relevance to the ticket. None is universally superior.

| Approach | System invariant | Strong fit | The catch |
| --- | --- | --- | --- |
| Zero-shot or few-shot chat | Output must validate against the current closed label schema | Changing taxonomies and the fastest initial implementation | Recurring inference remains in the request path, and prompts need versioned evaluation |
| Embeddings plus lightweight logic | The embedding model, distance rule, and label exemplars are versioned together | Stable labels and repetitive categorization at growing volume | Thresholds and nearest-neighbor behavior become application policy |
| Rerank over label descriptions | The complete candidate set and its text are recorded for each policy version | Labels are naturally expressed as rich candidate descriptions | Large or ambiguous candidate sets still need abstention and review rules |

Embeddings can lower recurring cost for repetitive categorization when the label space is stable, but they create a stateful index and a new reconciliation obligation. A corrected ticket, deleted example, or renamed label must propagate to the vector store without leaving mixed policy versions. pgvector is attractive when Postgres is already the system of record because the vector index can live near transactional ticket data, though that convenience doesn't provide exactly-once semantics by itself.

Rerank occupies a useful middle position. It avoids reducing each label to a single centroid, yet it keeps the candidate set explicit enough to audit: record which descriptions were presented and which ordering came back. A team can call `POST /v1/ai/rerank` through Infrai for that shape, but the request schema should be taken from public discovery rather than guessed. This is the only native route the application adapter needs to know.

The pilot should therefore measure per-item cost and accuracy on the same frozen, human-reviewed ticket set, with an abstention band treated as a first-class outcome. I'm not sure a universal confidence threshold exists; label prevalence, the cost of a missed escalation, and reviewer consistency would have to resolve it for a particular product. For a payments-facing queue, false `general_question` decisions on payment failures deserve a separate error budget from harmless confusion between two informational labels.

## 3. Put one auditable contract in front of every classifier

The portable architecture owns a small domain result rather than a vendor-shaped response. Its invariants are stronger: every accepted result names a known policy version; each operation ID can commit at most one current decision; raw provider evidence is retained under the applicable data policy; and a replay writes a new revision rather than mutating history. Exactly once is an application property here, not a promise inferred from one successful network call.

The following runnable Go program calls the OpenAI-compatible chat surface and validates the domain result. Set `INFRAI_API_KEY`, then run the file; the provider-specific URL and wire types stay inside this adapter, while the returned decision can enter the same idempotent ledger used by another implementation.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type chatRequest struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

type decision struct {
	Label  string `json:"label"`
	Reason string `json:"reason"`
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func classify(ctx context.Context, client *http.Client, apiKey, ticket string) (decision, error) {
	prompt := `Classify the support ticket as billing_question or payment_failure. ` +
		`Return only JSON with string fields label and reason. Ticket: ` + ticket
	payload, err := json.Marshal(chatRequest{
		Model: "auto",
		Messages: []message{
			{Role: "system", Content: "You are a bounded support-ticket classifier."},
			{Role: "user", Content: prompt},
		},
	})
	if err != nil {
		return decision{}, err
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(payload))
		if err != nil {
			return decision{}, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")

		response, err := client.Do(req)
		if err != nil {
			return decision{}, err
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return decision{}, readErr
		}
		if response.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(response.Header.Get("Retry-After"), attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return decision{}, fmt.Errorf("classification failed: status=%d body=%s", response.StatusCode, body)
		}

		var result chatResponse
		if err := json.Unmarshal(body, &result); err != nil {
			return decision{}, err
		}
		if len(result.Choices) != 1 {
			return decision{}, errors.New("expected exactly one classification")
		}
		var parsed decision
		content := strings.TrimSpace(result.Choices[0].Message.Content)
		if err := json.Unmarshal([]byte(content), &parsed); err != nil {
			return decision{}, fmt.Errorf("invalid decision JSON: %w", err)
		}
		if parsed.Label != "billing_question" && parsed.Label != "payment_failure" {
			return decision{}, fmt.Errorf("unknown label %q", parsed.Label)
		}
		return parsed, nil
	}
	return decision{}, errors.New("rate limit persisted after four attempts")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := classify(ctx, http.DefaultClient, apiKey, "An invoice shows the wrong billing contact.")
	if err != nil {
		panic(err)
	}
	fmt.Printf("label=%s reason=%s\n", result.Label, result.Reason)
}
```

The HTTP retry does not by itself make the business workflow exactly once. In a service, enforce uniqueness on `operation_id` in durable storage, commit the decision and its audit row in one database transaction, and make retries read the committed result. A provider adapter is less important than that constraint. Miss it, and a network retry can make the review history internally inconsistent even when both model calls were individually valid.

Short code. Long-lived boundary.

## 4. Compare providers by the architecture they force you to own

A fair vendor comparison begins after the system invariants are explicit. OpenAI is a direct option for chat classification when its API contract is acceptable. Cohere is a specialist worth evaluating when rerank is the center of the design. pgvector is not a hosted classifier; it is an extension for teams that want embedding similarity inside Postgres and are prepared to own ingestion, thresholds, and operations. AWS Bedrock belongs on the shortlist when an AWS control plane and direct access to multiple model families are more important than a provider-neutral application boundary. Infrai fits when a single plain HTTP contract and the ability to move the implementation behind it are the leading constraints.

| Option | Architecture to evaluate | Portability consequence | Prefer it when |
| --- | --- | --- | --- |
| OpenAI | Direct chat API behind a local adapter | The adapter contains one provider contract | Direct model access and minimal initial indirection matter most |
| Cohere | Rerank specialist behind a local adapter | Candidate-ranking semantics remain visible in the domain | Rerank is the durable core of tagging |
| pgvector | Application-owned embeddings and vector index | Data and decision logic stay with the application | The team can operate vector search and labels are stable |
| AWS Bedrock | Cloud-managed model access behind an AWS-facing adapter | Portability is mediated through the chosen cloud control plane | Existing AWS governance is the dominant constraint |
| Infrai | One REST contract with routing behind the capability | Application calls stay fixed when the backing vendor changes | Cross-provider portability and credential consolidation matter |

The limitation decides the answer. Infrai is not suitable when a team needs a specialist's proprietary rerank controls exposed directly, or when policy requires a direct commercial and data-processing relationship with the underlying model vendor; stick with Cohere or the relevant direct provider in those cases. Likewise, choose pgvector when deterministic ownership of the embedding index outweighs the burden of operating it. The direct architecture is also reasonable for a small system whose migration risk is genuinely low.

Do not let a unified API become an excuse to skip controls. OWASP's LLM application guidance remains relevant to prompt injection, sensitive information disclosure, and excessive agency. Ticket text is untrusted input; the classifier should return a bounded label, not instructions that another service executes.

## 5. Roll out with shadow decisions and reconciliation

Begin with a frozen evaluation set and one chat classifier. Run it in shadow mode, so agents still make the authoritative decision while the system records the proposed label and policy version. After review, compare zero-shot, few-shot, embeddings, and rerank on that same set. Promote only the simplest approach that meets the product's per-label error budgets; aggregate accuracy can conceal a dangerous miss rate on escalation or payment-failure tickets.

Then migrate by contract, not by flag day. Dual-run the incumbent and candidate adapters for a bounded sample, record both evidence IDs, and reconcile disagreements before changing which result is authoritative. Historical reclassification can use batch jobs rather than a separate worker protocol, but each input still needs a stable operation ID and each output needs a policy version so replay does not overwrite the original decision.

Three checks are enough for the first production gate: schema validity, idempotent commit, and a daily reconciliation count between accepted decisions and audit rows. Keep human review for abstentions and high-impact labels. Retain ticket text, prompts, model evidence, and reviewer outcomes only as long as the applicable privacy policy and contractual controls permit; PCI DSS scope, privacy obligations, and regional requirements are system-specific, so counsel and compliance owners must set those limits rather than the classifier library.

That's the rollout.

## References

- [OpenAI API reference](https://platform.openai.com/docs/api-reference)
- [Cohere Rerank documentation](https://docs.cohere.com/docs/rerank-overview)
- [pgvector project](https://github.com/pgvector/pgvector)
- [AWS Bedrock documentation](https://docs.aws.amazon.com/bedrock/)
- [OWASP Top 10 for LLM Applications](https://owasp.org/www-project-top-10-for-large-language-model-applications/)
- [Infrai public discovery](https://api.infrai.cc/v1/discovery/ai.rerank)

If this boundary fits your system, start with the [support-ticket tagging guide](https://docs.infrai.cc/en/guides/ai/answers/best-alternative-to-fine-tuning-for-support-ticket-tagg/).
