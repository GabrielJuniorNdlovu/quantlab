# Boom 1000 source assessment

Assessment date: 2026-10-26. Scope: documentation discovery and local sample inspection tools.
No market-data endpoint, trading account, or MT5 terminal was connected. No authorized genuine
sample was supplied. No source is qualified for QuantLab's intended durable research archive.

Implementation status: `IMPLEMENTED — SOURCE QUALIFICATION INCOMPLETE`.

## Sources investigated

Deriv's [official synthetic CFD listing](https://deriv.com/markets/derived-indices/synthetic-indices?tab=cfds)
names Boom 1000 Index. That establishes the display name and publisher. It does not establish
a machine symbol, quote precision, or availability on another Deriv product's API.

Two candidate routes are recorded in the packaged [source registry](../../src/quantlab/data/sources.json):

- `deriv-public-api`: Deriv's documented public Options market-data API. Its technical
  capabilities are documented, but Boom 1000 availability and durable-storage permission
  are unresolved. Registry state: `BLOCKED`.
- `deriv-mt5-recording`: an authorized recording or export from a Deriv MT5 terminal.
  MetaQuotes documents terminal data interfaces, but this project has no terminal metadata,
  history coverage, or authorized recording. Registry state: `DOCUMENTED`.

These routes are candidates, not interchangeable feeds. No independently authorized third-party
Boom 1000 archive was established during this assessment. An unofficial dump would not resolve
authenticity or storage rights.

## Instrument identity

QuantLab's internal ID remains `BOOM_1000`. Both candidates leave `external_id`, `pip_size`,
and `price_basis` null. Neither the internal name nor a guessed provider code is an executable
mapping. An observed external symbol must be linked to the official product and the relevant
server or API version before a reviewed import profile is created.

The [active-symbol migration documentation](https://developers.deriv.com/comparison/active-symbols/)
renames identity fields to `underlying_symbol` and `underlying_symbol_name`. Its precision
example uses a fractional `pip_size`. QuantLab's canonical `pip_size` is an integer number of
decimal places. Those representations require explicit unit verification, not an integer cast.
No live active-symbol response was requested to resolve Boom 1000 availability.

For the terminal route, [symbol_info](https://www.mql5.com/en/docs/python_metatrader5/mt5symbolinfo_py)
documents retrieval of symbol properties, including `digits` and `point`. Generic forex examples
are not evidence for Boom 1000. The exact terminal/server symbol, display name, digits, point,
and quote basis remain prerequisites for that route.

## Historical observations

The [published history request schema](https://raw.githubusercontent.com/deriv-com/deriv-api-schemas/master/schemas/ticks_history_request.schema.json)
documents tick and candle styles, explicit boundaries, a count, optional subscription, and
license-related interval adjustment. The new default count is 1,000. A request default is
neither a maximum retention period nor proof that every historical tick can be downloaded.

The [history response schema](https://raw.githubusercontent.com/deriv-com/deriv-api-schemas/master/schemas/ticks_history_response.schema.json)
describes parallel `history.prices` and `history.times` arrays, or OHLC candles with `epoch`.
The history object and its arrays are optional in the schema. A future adapter must check
errors, required content, array alignment, returned boundaries, ordering, and truncation.
It cannot infer a complete interval from a successful response alone. Overlapping requests,
same-time ticks, and boundary pagination remain integration questions. Candle history cannot
reconstruct individual ticks.

For MT5, [CopyTicksRange](https://www.mql5.com/en/docs/series/copyticksrange) documents
oldest-to-newest tick arrays with inclusive millisecond boundaries. Timeout and small-buffer
conditions can produce partial results. An inclusive restart boundary can overlap a previous
request, so timestamp-only deduplication would lose legitimate observations. No Boom-specific
retention start or continuous coverage was established.

The Python [copy_rates_range documentation](https://www.mql5.com/en/docs/python_metatrader5/mt5copyratesrange_py)
limits candle availability to terminal chart history and its configured maximum bars. This is
not a tick-retention guarantee. Initial and final valid timestamps in a local sample establish
only that sample's observed span. Silence within the span remains a suspected gap unless the
source supplies stronger continuity evidence.

## Streaming observations

Deriv's [public WebSocket documentation](https://developers.deriv.com/docs/options/ws-public/)
describes a market-data channel without authentication or OTP. Account operations are absent
from that channel. This documents a possible read-only mechanism; it does not establish that
Boom 1000 is exposed there or that QuantLab has a working collector.

The [tick response schema](https://raw.githubusercontent.com/deriv-com/deriv-api-schemas/master/schemas/ticks_response.schema.json)
describes an optional `tick` object. Only `pip_size` is required inside that object. `epoch`,
`quote`, `bid`, `ask`, `symbol`, and `id` are optional. It describes `tick.id` as a per-connection
identifier usable for unsubscribing. It must not become QuantLab's `provider_tick_id` or a
global event key. A subscription identifier does not distinguish repeated observations.

[Ticks comparison](https://developers.deriv.com/comparison/ticks/) documents subscribing with
`subscribe=1`. It provides no demonstrated exactly-once delivery, source sequence, durable
resume cursor, or uninterrupted history-to-stream handoff for this instrument. These remain
unverified, rather than assumed guarantees.

[Connection guidance](https://developers.deriv.com/docs/best-practices/) recommends backoff,
jitter, periodic ping, error handling, and subscription cleanup. Such operational guidance is
not a missing-tick recovery guarantee. [Rate limits](https://developers.deriv.com/docs/limits/)
use shared budgets and can change. No live request or subscription capacity was measured.

For MT5, [OnTick](https://www.mql5.com/en/docs/event_handlers/ontick) is an event callback for
an EA. Another NewTick event is not queued while one is already queued or executing. A future
recorder based on callbacks alone therefore cannot assert that it captured every tick.
Python snapshot retrieval is not an event subscription. No EA or terminal bridge is added in F00.

## Timestamp and price semantics

The API schemas expose integer epochs and JSON numeric prices. They do not supply a reception
timestamp or establish the required Boom-specific subsecond precision. Source-specific epoch
units and any precision guarantee must be confirmed before conversion. Existing F04
`historical.v1` profiles accept aware ISO 8601 timestamps, not numeric epochs or history arrays.

MetaQuotes' [tick structure](https://www.mql5.com/en/docs/constants/structures/mqltick) contains
`time`, `time_msc`, `bid`, `ask`, `last`, volumes, and flags. `time_msc` records update time in
milliseconds. Unchanged fields can retain prior values. These fields are not separate executions
and cannot be combined without an explicit price-basis policy. The Python
[tick-range reference](https://www.mql5.com/en/docs/python_metatrader5/mt5copyticksrange_py)
states that returned terminal market times are UTC.

The API's `quote` is described as market value. This assessment does not relabel it as bid,
ask, last, or an MT5-equivalent price. The terminal fields use binary doubles; a recorded text
export must document its decimal formatting. The F00 inspector uses the existing exact JSON
decoder and F01 price parser. Observed decimal places and timestamp fractions describe a file,
not a provider guarantee. No price rounding, timestamp guessing, interpolation, or deduplication
is introduced.

## Access and storage constraints

The [API Users terms](https://deriv.com/terms-and-conditions/api-users), version R26|03 dated
2026-10-16, prohibit storing feed content in section 2.1 and permit caching for up to 24 hours
in section 2.3. The intended long-term QuantLab archive requires applicable permission
clarification. An unauthenticated endpoint is not permission to retain its data indefinitely.
No provider contact or agreement was obtained during F00.

For terminal recordings, MetaQuotes API documentation establishes technical access, not
Deriv data-archival rights. The applicable terms, authorized recording, and permitted research
use remain unresolved. Switching transport does not establish permission. No credentials
are needed to list sources or inspect a local file already supplied with authorization.

## Verified facts and unresolved gates

The official pages and schemas above were retrieved on 2026-10-26. The registry distinguishes
`VERIFIED_METADATA` for the product listing from `DOCUMENTATION_CLAIM` for interface behavior.
Neither is a live observation. Links to mutable schema branches record this retrieval date,
not a guarantee about a future deployment.

Before a source can progress beyond documentation qualification, it needs:

1. Applicable permission for recorded samples, durable storage, and the intended research use.
2. Verified external instrument identity on the actual interface and deployment.
3. An authorized genuine sample with original bytes, acquisition context, and source reference.
4. Reviewed timestamp units, decimal precision, price basis, and identifier semantics.
5. Sample compatibility results through F01 or an explicitly scoped future F04 adaptation.
6. Documented coverage limits and reviewed continuity/reconnection behavior for the intended use.

No sample qualification was attempted. The shipped inspector and registry await the separately
requested testing phase. F00's implementation is complete within this development phase;
source qualification remains incomplete. It does not remove F04's outstanding integration
and scale-verification requirements.
