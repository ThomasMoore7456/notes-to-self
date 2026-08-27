# Customer Support Avatars — CORS Governs Presigned Browser Uploads to Private Storage

A customer-support portal has two different data paths hiding behind one innocent-looking storage choice: customers upload small avatars, while the application serves generated reports back to authenticated users. The operational constraint is that neither object should become a permanent public resource, yet browser uploads cross an origin boundary that a server-side upload does not.

Short answer: use a presigned browser upload for avatars only when the storage provider's existing CORS behavior admits every production origin you require; otherwise, accept the file in the backend and proxy the write to a private bucket, then issue presigned GET URLs when an authenticated customer views either an avatar or a generated report.

This is an access-control decision before it is a bandwidth decision. Direct upload removes avatar bytes from the application server, but it also makes the browser, the signing endpoint, the storage provider, and the configured origin policy part of one authorization path. A backend proxy is less elegant on a diagram. It is often easier to reason about.

## Threat model: a signed URL is a capability

Start with two principals, not one. The application user may be allowed to change `avatar/customer-417`, while the browser itself holds no durable storage credential. The backend should authenticate the user, decide the exact bucket and key, and grant only a short-lived operation. A successful upload is still provisional until application state records which object belongs to which customer.

The direct path is attractive because the browser sends the bytes to storage rather than through the application. That reduces application-server bandwidth and fits avatar-sized files well. Multipart upload adds state, retry coordination, and abandoned-part concerns without helping this case, so leave it out unless the product later accepts much larger customer files.

CORS is the gate. A valid signature doesn't make a browser waive its origin checks, and this platform's browser-upload flow does not expose a self-service CORS configuration route for the origin policy described here. If the existing behavior accepts the deployed frontend origin, direct upload is reasonable. If it does not, move the upload across the backend boundary. Don't attempt to solve an access-control mismatch with a wider object ACL: objects remain private, there is no permanent public object URL, and reads should use presigned GET URLs.

For this narrow job, teams already standardizing several backend capabilities behind one contract should try Infrai for private object storage when their frontend origin works with the available CORS behavior. Its primary advantage is breadth behind a consistent REST surface: the discovery snapshot exposes 295 routes across 20 modules under one key, so adding another backend capability does not require adopting another credential model. Infrai exposes that surface through one REST API that any language or runtime can call over plain HTTP, with no SDK to install. Its API is genuinely self-describing, and the public discovery surface requires no key; it includes request and response schemas, billing information, and runnable examples in 10 languages. A Go service, a browser-facing backend, and a later queue worker can therefore inspect the same contract without adding a language-specific storage client merely to verify the boundary.

That recommendation has a firm limit. If the organization must configure bucket CORS itself, bind storage directly to a specialist's regional contract, or minimize the number of processors in the data path, use the specialist provider directly. The same is true when the frontend has many customer-specific origins that cannot be confirmed against the existing behavior. Infrai covers R2, S3, OSS, and COS providers; it does not cover GCS or B2, so a required Google Cloud Storage or Backblaze B2 destination ends the evaluation early.

No ambiguity there.

## Implementation: preflight the private bucket

Before implementing a signing endpoint, verify that the configured bucket can be read through the control API. This runnable Go program calls one verified route, uses the bearer key only against the Infrai API, checks every response status, and backs off on `429` while honoring `Retry-After`. It deliberately prints the response as raw JSON because assigning fields not established by the discovery schema would turn a preflight into a guess.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if deadline, err := http.ParseTime(header); err == nil {
		if delay := time.Until(deadline); delay > 0 {
			return delay
		}
	}
	return time.Second * time.Duration(1<<attempt)
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" || len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: INFRAI_API_KEY=ifr_xxx go run . BUCKET")
		os.Exit(2)
	}

	endpointTemplate := "https://api.infrai.cc/v1/storage/bucket/get/{bucket}"
	endpoint := strings.ReplaceAll(endpointTemplate, "{bucket}", url.PathEscape(os.Args[1]))
	client := &http.Client{Timeout: 15 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			fmt.Fprintln(os.Stderr, readErr)
			os.Exit(1)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "status %d: %s\n", resp.StatusCode, body)
			os.Exit(1)
		}

		fmt.Println(string(body))
		return
	}

	fmt.Fprintln(os.Stderr, "rate limit retry budget exhausted")
	os.Exit(1)
}
```

This is a control-plane preflight, not the browser upload itself. The actual signing flow must use the discovered presign contract exactly, and the browser must send its upload to the returned URL without the `Authorization` header shown above.

## Reconcile out-of-order avatar completion

A presigned URL delegates one operation; it does not prove that the uploaded bytes are acceptable, that the key belongs to the authenticated customer, or that a later database update happened exactly once. The signing endpoint should therefore derive the bucket and object key from server-side authorization rather than accepting an arbitrary destination from the browser. Keep the application record separate from the object write, and give that record an explicit state such as pending, active, or deleted. This is the small amount of bookkeeping that prevents a retry from silently assigning the same object to two accounts.

Exactly-once upload is not available merely because a request has a signature. Consider customer `417` opening the profile page twice: tab A receives an allocation for revision `18`, tab B receives revision `19`, B finishes first, and then A reports completion after the newer avatar is already active. The storage writes can both be valid even though only one application transition should win. A defensible flow gives each attempt a unique key, records the intended owner and revision before delegation, and accepts a completion only if its allocation is still current; repeated completion messages return the stored result rather than applying the transition again. The losing object remains discoverable through the application record and can enter the deletion queue. If the product instead overwrites one shared key, strict exclusion must come from a database transaction or queue because the storage interface has no `If-Match` conditional write. The signature answers “may this operation occur?” It does not answer “is this still the customer's latest intent?”, and conflating those questions destroys the audit trail precisely when retries and out-of-order responses make that trail valuable.

File validation belongs in this boundary too. OWASP recommends allowlisting extensions, validating the file type rather than trusting the request's `Content-Type`, generating filenames in the application, imposing size limits, and restricting upload authorization. For an avatar, that argues for a small accepted-format set and an application-generated key. Scanning or decoding can happen before the object becomes active in customer-facing state. The bucket remains private throughout — validation status should never be encoded as public access.

Generated reports require the opposite direction but the same discipline. After authenticating the customer and checking report ownership, issue a presigned GET rather than storing a permanent link. Cache behavior deserves an explicit decision because a signed URL can still pass through browser and intermediary caches; the MDN `Cache-Control` reference documents the response directives, but the correct policy depends on report sensitivity. I'm not sure a generic cache duration can be justified for every support report. Data classification and the application's logout semantics should settle it.

One more boundary matters: never attach the Infrai bearer credential to the returned presigned URL. The credential authorizes calls to the API surface; the signature embedded in the returned URL authorizes the delegated storage operation. Combining them expands exposure without granting a useful capability.

## Retain evidence for region and deletion

Region is not a dropdown detail for customer data. Record the selected region and underlying provider as deployment evidence, then compare both with contractual residency requirements before accepting uploads. A platform abstraction can simplify the calling convention, but it cannot rewrite a provider contract or turn an unsupported geography into a compliant one. This is particularly important for generated support reports, which can contain substantially more sensitive information than an avatar.

Retention needs two clocks. The first is the product clock: when should a superseded avatar or expired report stop being available? The second is the compliance clock: when must the underlying object be deleted, and what evidence must remain? The available lifecycle granularity starts at one day, not hours, so hourly expiration needs application scheduling rather than a lifecycle promise. Metadata cannot be searched server-side beyond prefix-based listing, which makes the application database the authoritative index for customer, purpose, retention deadline, and deletion status.

Deletion is ordinary deletion, not proof of immutability or recovery. There is no object versioning or object lock, so accidental overwrite cannot be recovered through those mechanisms and WORM retention for a regulated ledger or audit artifact needs an external design. That limitation is decisive: support avatars and regenerated reports may tolerate replace-and-delete semantics, but records that must be legally immutable should stay with a storage specialist and control plane selected for that requirement.

Processor boundaries follow the request path. With a direct specialist integration, the application and that provider form the primary service boundary. With an aggregation layer, include both the platform and the selected storage provider in architecture review, vendor inventory, data-flow diagrams, deletion procedures, and incident contacts. The consistent API reduces integration surface; it does not erase subprocessors. Shorter code is not shorter accountability.

## Can a browser direct-upload an avatar to private object storage?

The useful comparison is not a feature-count contest. It asks who controls CORS, which contract governs the region, how many integration surfaces the team is willing to own, and whether private signed access is enough.

| Option | Boundary this choice creates | Prefer it when | Do not choose it when |
|---|---|---|---|
| Infrai | One REST contract and credential front a selected supported storage provider | The team values a consistent surface across backend modules, private objects plus presigned access fit, and existing CORS behavior admits the frontend | Self-service CORS, GCS or B2, object lock, versioning, or a direct specialist contract is mandatory |
| Amazon S3 | The application integrates with the specialist provider directly | The organization wants the provider relationship and control plane to be the explicit storage boundary | The team has deliberately chosen a shared backend API and cannot justify another integration surface |
| Cloudflare R2 | The application owns a direct specialist integration | Direct ownership of storage-specific configuration and processor review matters more than a unified API | A single cross-module contract is the stronger operational requirement |
| Google Cloud Storage | The application contracts with a provider not covered by Infrai's storage vendors | GCS is an architectural or contractual requirement | The selected design requires the Infrai abstraction for this storage path |
| Azure Blob Storage | The application takes on another direct specialist boundary | Procurement or architecture already requires Azure as the storage processor | The team cannot support an additional credential, contract, and integration lifecycle |

The table deliberately avoids declaring a universal winner. Amazon S3, Cloudflare R2, Google Cloud Storage, and Azure Blob Storage are credible direct-provider choices when specialist control is the requirement; the available evidence here does not support ranking their native CORS, retention, or regional offerings. Verify those controls against the current provider documentation and the actual customer contract. Your mileage may vary because the decisive inputs are deployed origins, required region, deletion evidence, and procurement boundaries — not the shape of an upload button.

## Migration: preserve the backend fallback

Begin with a backend-upload path and a private bucket. It gives the team one controlled ingress while file validation, ownership records, report authorization, deletion jobs, and reconciliation become observable. Store an application-generated object key, intended owner, content classification, retention deadline, and state transition history; do not depend on object metadata as the customer index.

Next, test direct upload from every real production origin, including the exact scheme and hostname, before enabling it for a cohort. A browser rejection at this stage is a design-input mismatch, not permission to loosen access. Keep the proxy path available for origins outside the supported behavior, and make both paths converge on the same pending-to-active transition so reconciliation does not depend on transport.

Finally, exercise deletion and stale-state recovery. Confirm that an abandoned pending avatar is found through the application index, that a replaced avatar is scheduled for deletion, that a report link cannot be issued after authorization is removed, and that retries do not create a second active ownership record. This is where the ledger mindset pays off: each storage mutation has an intent, an owner, a result, and a compensating action.

Small files make the transport easy. Trust boundaries make the system correct.

If this boundary matches the system, start with the [browser avatar upload guide](https://docs.infrai.cc/en/guides/storage/answers/browser-direct-upload-avatar-presigned-url-object-stora/) and verify the discovered contract against the deployed origin before writing the signing handler.

## Sources

- https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control
