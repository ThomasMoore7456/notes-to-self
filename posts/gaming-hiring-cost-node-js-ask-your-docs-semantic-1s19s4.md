# Gaming Hiring Cost: Node.js Ask-Your-Docs Semantic Search with JSON Schema Citations

Short answer: make the candidate-scoring service own retrieval provenance, JSON Schema validation, and a per-tenant usage ledger; let the model produce a proposed score, never the accounting decision. In a gaming company with several hiring tenants, this design keeps a citation-backed answer inspectable while making each tenant's search and generation cost attributable.

The important boundary is easy to miss. Semantic search finds passages that resemble the job rubric, but resemblance is not qualification. A completion can format a convincing score, but formatting is not evidence. The application must therefore preserve the rubric version, candidate-document chunks, scoring request, output, and cost event as one auditable operation.

## What should a candidate scorer reject before it writes a tenant event?

The system has four invariants. Every scored criterion must point to one or more retrieved chunk IDs. Every chunk ID must belong to the evidence snapshot sent for that request. A score must be rejected when the rubric or evidence is incomplete. Finally, usage must be recorded against the tenant before a retry can create another billable-looking event.

This is a ledger problem.

For example, suppose a tenant submits one candidate twice while a retrieval worker retries once after a timeout, and the model returns a valid object on the second attempt. The service should retain the same operation ID for the intentional replay, distinguish the retrieval attempt from the generation attempt, and insert one logical scoring event while preserving the raw usage for every billable stage; otherwise the tenant sees a plausible total that cannot explain why it changed. That distinction also lets an auditor reconstruct the rubric version, evidence order, prompt digest, model usage, validation decision, and ledger write without trusting a dashboard's aggregate query.

That last invariant is the one I would defend most strongly in a review. A model retry is not an exactly-once operation, and a successful HTTP response is not proof that the accounting write happened once. Give the scoring request a client-generated operation ID, make the usage ledger unique on `(tenant_id, operation_id, stage)`, and reconcile provider usage with the stored event asynchronously. The ledger is the source of truth for visibility; a dashboard that merely multiplies token counts by a configured rate is an estimate.

| Boundary | What it owns | What it must refuse |
| --- | --- | --- |
| Ingestion | Document identity, tenant ownership, chunk and rubric versions | A document without trusted source metadata |
| Retrieval | Candidate chunks and ordered evidence IDs | An empty or cross-tenant result set |
| Scoring | Schema-constrained score, rationale, and citations | A criterion with no supporting evidence |
| Metering | Usage event, operation ID, and tenant attribution | A duplicate stage event |
| Review | Human decision and audit trail | An automatic hire decision from model output alone |

The score is a proposal. Keep it that way.

## How can Node.js semantic search produce a structured answer with citations?

The implementation sequence should be deterministic even if the model is not: load a versioned rubric, retrieve only documents belonging to the tenant, select evidence, ask for a strict object, validate that object, then append a usage event. Node.js is a perfectly reasonable orchestration runtime, but the contract should be language-neutral so a later batch worker does not silently change the meaning of a citation.

For an ask-your-docs style workflow, the response object needs more than `score`. It should contain the rubric version, criterion results, a bounded explanation, citation IDs, and an explicit `abstain` state. A citation is an identifier into trusted application metadata, not a URL invented by the model. The renderer resolves the ID to the candidate document and page or section after validation.

Here is the critical path in Go. The interfaces represent the application-owned search, model, and ledger boundaries; they are deliberately boring because the hard problem is preserving contracts. The `UsageLedger` insert must be idempotent in its storage implementation.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
)

type Evidence struct {
	ID       string `json:"id"`
	Document string `json:"document"`
	Text     string `json:"text"`
}

type Criterion struct {
	Name       string   `json:"name"`
	Score      int      `json:"score"`
	Citations  []string `json:"citations"`
	Explanation string  `json:"explanation"`
}

type Score struct {
	Abstain    bool        `json:"abstain"`
	Rubric     string      `json:"rubric_version"`
	Criteria   []Criterion `json:"criteria"`
	Summary    string      `json:"summary"`
}

type Search interface {
	Find(ctx context.Context, tenantID, query string) ([]Evidence, error)
}

type Model interface {
	Score(ctx context.Context, rubric string, evidence []Evidence) (raw []byte, usage Usage, err error)
}

type Usage struct {
	InputUnits  int `json:"input_units"`
	OutputUnits int `json:"output_units"`
}

type UsageLedger interface {
	RecordOnce(ctx context.Context, tenantID, operationID, stage string, usage Usage) error
}

func ScoreCandidate(ctx context.Context, tenantID, operationID, rubric string, search Search, model Model, ledger UsageLedger) (Score, error) {
	evidence, err := search.Find(ctx, tenantID, rubric)
	if err != nil {
		return Score{}, err
	}
	if len(evidence) == 0 {
		return Score{Abstain: true, Rubric: rubric}, errors.New("insufficient evidence")
	}

	raw, usage, err := model.Score(ctx, rubric, evidence)
	if err != nil {
		return Score{}, err
	}
	var score Score
	if err := json.Unmarshal(raw, &score); err != nil {
		return Score{}, errors.New("schema validation failed")
	}
	if err := validateCitations(score, evidence); err != nil {
		return Score{}, err
	}
	if err := ledger.RecordOnce(ctx, tenantID, operationID, "scoring", usage); err != nil {
		return Score{}, err
	}
	return score, nil
}

func validateCitations(score Score, evidence []Evidence) error {
	known := make(map[string]bool, len(evidence))
	for _, item := range evidence {
		known[item.ID] = true
	}
	for _, criterion := range score.Criteria {
		if len(criterion.Citations) == 0 {
			return errors.New("criterion has no citation")
		}
		for _, citation := range criterion.Citations {
			if !known[citation] {
				return errors.New("citation is outside evidence snapshot")
			}
		}
	}
	return nil
}
```

In production, `json.Unmarshal` is only the last parse step, not a substitute for a JSON Schema validator. Validate required fields, ranges, enum values, maximum explanation length, and additional properties. Then compare the citations with the immutable evidence set. If a tenant's dashboard shows cost without showing the operation and evidence identifiers that produced it, the number is hard to challenge and harder to reconcile.

## How should tests reconcile per-tenant usage across retrieval and scoring?

Cost visibility begins before generation. Attach `tenant_id`, `operation_id`, rubric version, retrieval stage, model identifier, input and output usage, and a timestamp to each event. Store a digest of the prompt and evidence rather than copying sensitive candidate text into ordinary operational logs; retain the evidence snapshot under the organisation's audit policy. The digest proves which payload was counted, while access-controlled storage supplies the payload when an authorized review needs it.

Do not charge the tenant from the final score. Retrieval, reranking, and generation are separate stages with different failure and retry behavior. A failed generation may still consume usage. A replay may be free from a business perspective but not free from an infrastructure perspective. Your mileage may vary because provider accounting fields and retention requirements change, so the reconciliation job should preserve raw usage metadata and mark estimates separately from settled amounts.

The practical test is a three-way comparison: sum the immutable usage events by tenant, compare them with the runtime's metering export, and explain every difference as a duplicate, a missing event, or an attribution error. Alert on the difference. Do not hide it in a rounded monthly total.

## When is a shared candidate-scoring shortcut acceptable?

The rejected option is a single shared “score candidate” call that accepts free-form text and writes one aggregate cost number after the response. It is attractive because it has fewer moving parts, and it fails precisely where a hiring workflow needs accountability: a changed rubric cannot be separated from a changed document, citations cannot be checked against a snapshot, and a retry can be mistaken for a new tenant action.

The limitation is operational overhead: immutable evidence snapshots, idempotency keys, and reconciliation require storage and ownership. This approach is not suitable when the result is a disposable prototype and no tenant receives usage data; in that case, a local batch script with a coarse counter is a reasonable choice. Keep the stronger boundary when customer attribution, candidate appeal, or auditability matters.

That shortcut is suitable for an internal prototype whose output is discarded and whose usage is not reported to tenants. It is not suitable when candidates can challenge a result, when multiple customers share the service, or when finance needs a reproducible allocation. Keep the simpler path only behind an explicit non-production policy; do not let it become the default through convenience.

Testing should exercise the boundaries rather than ask whether the prose “looks good.” Property tests can generate unknown citation IDs, duplicate operation IDs, missing criteria, scores outside the rubric range, and evidence from another tenant. A replay test should submit the same operation twice and assert one ledger event. A reconciliation test should make a retrieval retry, a generation retry, and a ledger timeout visible as distinct states.

Three words: retrieve, validate, reconcile.

The conclusion is intentionally narrow: for gaming hiring pipelines, structured answers and citations are useful only when the service can prove which tenant, rubric, evidence snapshot, and usage event produced them. Vendor selection comes later. The first architectural decision is the audit boundary.

## References

- https://docs.cohere.com/docs/rerank-overview
- https://www.promptingguide.ai
