# HTTP query attempts

`X-DATABEND-QUERY-ATTEMPT` is an optional, single unsigned 32-bit decimal
integer. `0` is the first send, `1` the first retry, and so on. For
`POST /v1/query`, this header requires a stable, non-empty `X-DATABEND-QUERY-ID`:

- No attempt header: legacy behavior. A local hit may be recovered; a miss may
  start a query.
- `0`: start the query, or recover the existing submission on a local hit.
- `> 0`: recover the existing submission only. Never start SQL on a miss.

The receiving Query process reserves the ID before restoring session state or
starting SQL. A marked recovery requires the original authenticated tenant,
user and client session. It returns page zero with the session state and
[query token](http-query-token.md) if still available. If the initial response
is pending or no longer recoverable, the server returns HTTP `409`,
`QuerySubmissionOutcomeUnknown` (`1136`). This does not mean SQL was not
executed. Do not reset the attempt to `0` or use a new ID to automatically execute
SQL again. Reuse attempt `0` only when every earlier send is known not to have
dispatched.

For query-state and result-page GETs the header is telemetry only. Missing or
invalid values do not change the response. A new poll or page request starts
its own count. Follow `next_uri` unchanged; callers may omit the header.

Recovery is local to the Query process that receives the request. It neither
searches other nodes nor fences old attempts across nodes. It does not cover
management statements handled by Gateway itself. Enable automatic submission
retries only when the path preserves the header and all eligible backends
support this contract: an older backend may ignore the header and start SQL.
