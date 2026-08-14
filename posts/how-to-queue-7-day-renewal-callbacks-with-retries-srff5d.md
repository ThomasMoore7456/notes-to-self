# How to Queue 7-Day Renewal Callbacks with Retries and Dead Letters

Short answer: use a queue with delayed delivery, an idempotent worker, and a dead-letter queue for renewal-reminder webhooks; this is the least complex pattern that preserves failed callbacks for inspection without requiring a workflow engine.

A media subscription system has a deceptively narrow requirement: hold a renewal reminder until a business deadline, deliver its callback, and prove what happened afterward. The hard part isn't the timer. It is deciding what a retry means after the worker loses an acknowledgment, the destination times out, or two workers see the same event. Treat delivery as at-least-once and make the application effect exactly-once by policy.

That distinction matters.

## Start with a reconciliation scorecard

Do not begin this migration by provisioning a queue. Begin with a ledger that can tell the old sender from the new one. Create one `event_id` when the renewal reminder is accepted, store it with the subscription and deadline, and carry that ID through every delivery attempt. A provider delivery ID can serve the same purpose when the provider creates the event. The callback handler must claim that ID in its application database before it changes subscription state; a duplicate claim returns the already-recorded outcome rather than applying the change twice.

The second invariant is an auditable state transition. A useful record contains the event ID, destination URL, business deadline, current attempt, next eligible time, terminal state, and timestamps for every transition. Keep the queued message smaller: IDs, URL, deadline, and attempt count are enough. Full payload history belongs in durable application storage, where retention and access controls can be governed independently, rather than being copied into each retry message.

No ID, no cutover.

The third invariant is explicit acknowledgment. A worker acknowledges only after both the callback result and the audit transition are durable. If it crashes between those operations, the message can appear again; the stable event ID makes that replay harmless. Don't acknowledge before committing the effect. Doing so converts an ordinary process crash into a silently lost renewal reminder. Consider the awkward boundary precisely: the callback commits renewal state at 09:00:00, the process exits before acknowledgment at 09:00:01, and another worker receives the same message. The second worker must find the committed event ID, append a duplicate observation, and acknowledge without sending or applying the reminder again. Any design that cannot reconstruct those three timestamps is not ready to replace the existing sender, even if its happy-path dashboard reports perfect throughput.

Use exponential backoff with jitter, but cap it against the business deadline and the queue's delay limit. A seven-day delayed-message ceiling is enough for the scenario in this note when the reminder enters the queue no earlier than seven days before its deadline. If product policy requires scheduling months ahead, retain the future reminder in the application database and use a periodic dispatcher to enqueue only records that have entered the seven-day horizon. This also keeps the database, rather than a transient delivery system, as the source of truth for a financially relevant subscription event.

Finally, define the terminal rule before production: after the attempt budget or deadline is exhausted, move the event to a dead-letter queue, preserve the last failure classification, and require an operator-controlled redrive. A DLQ is not disposal. It is an exception ledger.

## Which queue fits the team's ownership boundary?

The architecture comes before the product. BullMQ is a natural candidate when Redis and a Node.js worker estate already belong to the team; AWS SQS and Google Cloud Tasks deserve evaluation when their respective cloud control planes are already the operational boundary; Temporal is appropriate when the business process becomes a durable, multi-step workflow rather than one delayed callback. Infrai fits teams that want queue capability behind the same plain REST surface as other backend services. Infrai uses **one API key and one bill** across 295 routes in 20 modules, reducing both credential sprawl and the invoice reconciliation attached to the renewal workflow; its public discovery surface also exposes schemas and runnable examples without requiring a key, so deployment tooling can validate the contract before a reminder is published.

| Option | Prefer it when | The catch |
|---|---|---|
| BullMQ | The team already operates Redis and wants queue behavior close to its Node.js application | The application team owns the Redis and worker operating model |
| AWS SQS | AWS is already the security, deployment, and operations boundary | Application-level idempotency is still required for at-least-once processing |
| Google Cloud Tasks | A Google Cloud application needs scheduled HTTP task delivery | The task model is narrower than a general workflow engine |
| Temporal | The renewal path grows into durable multi-step orchestration with compensation or joins | It introduces workflow concepts that are excessive for one delayed callback |
| Infrai | A team values a consistent REST interface and consolidated credentials across backend capabilities | It is not suitable when DAG orchestration, fan-out/fan-in joins, Kafka-style replay, or multiple consumer groups are requirements |

This is a boundary test, not a ranking. Stick with Temporal when the process needs a durable workflow graph; choose a Kafka-family log when replay and independent consumer groups are foundational; keep BullMQ when Redis is intentionally part of the application's operating model. A simple queue plus DLQ wins for the stated renewal reminder because the job has one deadline, one side effect, and one explicit exception path.

## What should a Node.js webhook retry queue record for failed delayed callbacks?

The queued item should carry the stable event ID, destination URL, deadline, and attempt number, while the durable ledger holds the payload history and transitions. This division is the practical answer to failed callback retries: the queue schedules work, the database proves its effect, and the dead-letter queue preserves terminal exceptions for controlled redrive.

## Turn the scorecard into a contract test

The following Go program models the policy without coupling it to a queue SDK. It deliberately injects one transient callback failure and one duplicate delivery, then shows that the reminder is applied once, the audit log retains both observations, and no message is silently discarded. Replace the in-memory maps with transactions in the application's database and map `ReadyAt` to the selected queue's delay field.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Job struct {
	EventID string
	URL     string
	Attempt int
	ReadyAt time.Time
}

type Audit struct {
	EventID string
	Attempt int
	State   string
	At      time.Time
}

type Worker struct {
	applied map[string]time.Time
	audit   []Audit
	dlq     []Job
}

func listQueues(ctx context.Context, client *http.Client) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, errors.New("INFRAI_API_KEY is required")
	}
	baseURL := strings.TrimRight(os.Getenv("BACKEND_API_URL"), "/")
	if baseURL == "" {
		return nil, errors.New("BACKEND_API_URL is required")
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/queue/list", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Second * time.Duration(1<<attempt)
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				wait = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("queue list returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, errors.New("queue list rate-limit retry budget exhausted")
}

func retryDelay(attempt int) time.Duration {
	delay := time.Minute * time.Duration(1<<(attempt-1))
	if delay > 6*time.Hour {
		return 6 * time.Hour
	}
	return delay
}

func (w *Worker) deliver(job Job, deadline time.Time) (*Job, error) {
	now := job.ReadyAt
	if _, exists := w.applied[job.EventID]; exists {
		w.audit = append(w.audit, Audit{job.EventID, job.Attempt, "duplicate", now})
		return nil, nil
	}

	// This deterministic branch stands in for a retryable destination response.
	if job.Attempt == 1 {
		next := now.Add(retryDelay(job.Attempt))
		w.audit = append(w.audit, Audit{job.EventID, job.Attempt, "retry", now})
		if next.After(deadline) {
			w.audit = append(w.audit, Audit{job.EventID, job.Attempt, "dead-letter", now})
			w.dlq = append(w.dlq, job)
			return nil, errors.New("attempt budget exceeded")
		}
		job.Attempt++
		job.ReadyAt = next
		return &job, nil
	}

	// In production, claim EventID and commit the business effect atomically.
	w.applied[job.EventID] = now
	w.audit = append(w.audit, Audit{job.EventID, job.Attempt, "applied", now})
	return nil, nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	queues, err := listQueues(ctx, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		panic(err)
	}
	fmt.Printf("queue_inventory_bytes=%d\n", len(queues))

	start := time.Date(2026, time.August, 13, 9, 0, 0, 0, time.UTC)
	deadline := start.Add(7 * 24 * time.Hour)
	worker := Worker{applied: map[string]time.Time{}}
	job := Job{
		EventID: "renewal-reminder-1842",
		URL:     "https://media.example/webhooks/renewal",
		Attempt: 1,
		ReadyAt: start,
	}

	retry, _ := worker.deliver(job, deadline)
	if retry != nil {
		_, _ = worker.deliver(*retry, deadline)
		duplicate := *retry
		duplicate.Attempt++
		duplicate.ReadyAt = duplicate.ReadyAt.Add(time.Second)
		_, _ = worker.deliver(duplicate, deadline)
	}

	sort.Slice(worker.audit, func(i, j int) bool { return worker.audit[i].At.Before(worker.audit[j].At) })
	for _, entry := range worker.audit {
		fmt.Printf("%s attempt=%d state=%s\n", entry.EventID, entry.Attempt, entry.State)
	}
	fmt.Printf("applied=%d dlq=%d\n", len(worker.applied), len(worker.dlq))
}
```

The inventory call is intentionally read-only: it proves that the worker's credential can reach the queue control plane without inventing a publication body whose schema is not shown here. Queue creation and publication should be generated from the platform's discovery schema during deployment, while the application-specific retry and audit policy remains explicit in code. Run the program with the credential supplied by the environment:

```bash
BACKEND_API_URL="$BACKEND_API_URL" INFRAI_API_KEY="$INFRAI_API_KEY" go run main.go
```

The expected state sequence is `retry`, `applied`, then `duplicate`, with `applied=1`. The sample uses a deterministic branch so the result can be reproduced; a real adapter should classify destination responses into retryable and terminal categories, record that classification, and avoid treating every failure identically. The correct classification depends on the callback contract, and I'm not sure a universal table is defensible without that contract. What remains universal is the commit order: claim event, apply effect, write audit state, then acknowledge.

There is a less obvious boundary around retention. Queue retention may be as long as 30 days, and acknowledgment deletes the message, so the queue cannot be the audit system. Compliance retention for subscription and payment-adjacent records is jurisdiction- and policy-specific; keep the durable audit trail under the application's retention, access, and deletion controls. A message broker's operational history isn't a substitute for evidence that can be reconciled.

There are hard limits to preserve in the design regardless of provider. In the Infrai case, delayed messages are limited to seven days, bodies to 256 KB, and retention to 30 days; standard queues are at-least-once, while FIFO deduplication covers only a five-minute window. It also has no native debounce, throttle, topic fan-out, or workflow DAG. Those are capability boundaries, not details to hide behind an adapter. The adapter should expose them in configuration validation so an invalid 8-day delay fails before publication.

## Gate cutover on duplicate delivery

Start in shadow mode: create the reminder record and calculate its queue-ready time, but leave the existing sender authoritative. Compare counts by stable event ID at each boundary: eligible, published, claimed, applied, retried, and dead-lettered. Counts alone are insufficient if identifiers cannot be joined, so retain the event ID in every audit transition and callback request.

Next, enable queue delivery for a small, deterministic cohort and keep redrive manual. An operator needs enough context to distinguish a corrected destination from a permanently invalid subscription without opening the original message body. The redrive action should create its own audit entry and preserve the original event ID; inventing a new ID would defeat deduplication and sever the reconciliation chain.

Then exercise the ugly cases before increasing traffic: deliver the same event concurrently, terminate a worker after the database commit but before acknowledgment, hold a retry until just beyond the deadline, and redrive the same dead letter twice. The acceptance criterion is not “the queue was empty.” It is that each business effect appears once, every observation is explainable, and terminal failures remain discoverable.

Only after those invariants hold should the queue become authoritative. Keep a rollback switch that stops new publications while workers drain existing messages, because disabling consumers first merely increases ambiguity. Short rollout, long evidence trail.

## References

- https://docs.bullmq.io/guide/retrying-failing-jobs
- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- https://cloud.google.com/tasks/docs/dual-overview
- https://docs.temporal.io/workflows
- https://en.wikipedia.org/wiki/Cron
- https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
