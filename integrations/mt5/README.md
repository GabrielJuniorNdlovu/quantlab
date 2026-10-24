# Verify the standalone MT5 fixture renderer

Native compilation and MT5 runtime behavior are **UNVERIFIED**. This source is a
feasibility prototype, independent of the Python replay application and its audit
log. It renders one predetermined arrow on a preloaded chart. It does not replay
or hide the chart's later price bars, evaluate outcomes, or demonstrate predictive
performance.

The development environment had no `metaeditor64`, `metaeditor`, `terminal64`, or
`wine` executable on `PATH`. Source inspection and current official documentation
were the available checks. Compilation, glyph placement, chart refresh behavior,
and platform packaging still require a terminal check.

## Run the manual check

1. In an existing local MT5 installation, open **File > Open Data Folder**.
2. Copy `QuantLabFixtureArrows.mq5` into `MQL5/Indicators/QuantLab/`.
3. Open the file in MetaEditor and compile it. Record the terminal/compiler build,
   compilation diagnostics, and any changes needed. Do not describe the prototype
   as verified before this succeeds.
4. Create or use a local custom engineering symbol named
   `QUANTLAB_BOOM_1000_FIXTURE`. Import prebuilt M1 fixture history containing an
   exact bar opening at `2026.01.01 00:00:00`, with prices around `1000.12`.
   Keep the terminal disconnected. This is a local test symbol, not an assumed
   provider machine symbol. Importing custom history is a separate manual terminal
   operation; this source creates no symbols or market data.
5. Open that symbol's M1 chart and attach the indicator. Match `FixtureSymbol` to
   the local engineering symbol's exact name. Another timeframe must fail with a
   parameter error.
6. Set `IssuedAtChartTime` to `2026.01.01 00:00:20`,
   `OriginalAnchorPrice` to `1000.12`, and `DemoCutoffChartTime` to
   `2026.01.01 00:00:19`. Expect no arrow.
7. Change the cutoff to `2026.01.01 00:00:20`. Expect one gold upward glyph at the
   `00:00` candle and the fixed price `1000.12`. Confirm the Data Window shows the
   same value. Adjust the chart's price scale if the fixed price is outside view.
8. Advance the cutoff to a later time, add later fixture bars, and trigger a chart
   recalculation. Confirm the arrow remains on the original opening minute and
   price. Repeat while changing the forming candle's low. Its low must never move
   the arrow.
9. Remove the exact `00:00` bar in a separate test history and reattach. Expect no
   arrow on a neighboring candle. Restore the original history and verify the
   original position returns.

Changing inputs reinitializes an indicator instance. These checks compare explicit
test configurations; they do not certify a monotonic replay clock or immutable
native research audit. The mandatory sequential replay and audit verification live
in the Python application.

## Map the Python alert contract

A future bridge must retain the Python alert ID, run and dataset identities,
`replay_alert.v1` schema version, issue timestamp and source position, data cutoff,
fixture source, horizon, original anchor, and separate outcome. A buffer supports
one value per bar, so multiple alerts within the same minute need an explicit
additional representation. This one-arrow prototype does not solve that policy.

The input issuance time and price represent the original Python arrow anchor.
The renderer floors the supplied chart time to its opening minute and searches
for an exact matching `time[]` value. `PLOT_SHIFT` and `PLOT_ARROW_SHIFT` remain zero.
A missing minute produces no adjacent-bar substitute. The price buffer uses
`double`, so a future exporter must document rounding from Python `Decimal` and
retain the exact decimal string in the audit. A native `datetime` stores seconds;
Python microseconds and same-second source order need separate fields. See the
[official datetime reference](https://www.mql5.com/en/docs/basis/types/integer/datetime).

The full `OnCalculate` signature supplies bar opening times. The prototype sets
series indexing explicitly and does not rely on default array direction. Series
index zero is the latest bar, which can still be forming; it is never treated as
completed. The exact timestamp determines the destination even when new bars
change array indices. See [OnCalculate](https://www.mql5.com/en/docs/event_handlers/oncalculate)
and [array and buffer indexing](https://www.mql5.com/en/docs/series/bufferdirection).

An epoch integer has no timezone label. Terminal server time is not established
as UTC by its representation. A future adapter must verify the supplied history's
clock convention and any server offsets before mapping Python UTC into chart
opening times. This prototype takes **chart times** explicitly; it performs no
unverified UTC conversion. The [TimeCurrent documentation](https://www.mql5.com/en/docs/dateandtime/timecurrent)
describes server time independently of the computer's local clock.

## Verified API constraints

The official [`DRAW_ARROW` reference](https://www.mql5.com/en/docs/customind/indicators_examples/draw_arrow)
describes a one-buffer symbol plot and requires explicitly initialized empty
values. [`SetIndexBuffer`](https://www.mql5.com/en/docs/customind/setindexbuffer)
binds the dynamic `double` array; series direction must be set after binding.
[`PlotIndexSetInteger`](https://www.mql5.com/en/docs/customind/plotindexsetinteger)
sets the arrow glyph and shifts. Glyph 241 follows the upward-arrow example in
the official [Wingdings reference](https://www.mql5.com/en/docs/constants/objectconstants/wingdings).
These sources establish the proposed API usage, not successful native execution.

An indicator cannot call `WebRequest()` to reach a Python HTTP service. The API is
synchronous and is unavailable to indicators and the Strategy Tester. No HTTP
bridge is included. See the official [`WebRequest` restrictions](https://www.mql5.com/en/docs/network/webrequest).

A later read-only file bridge is a possible design to evaluate. Ordinary MQL5
file access is confined to the terminal's `MQL5/Files` sandbox or the testing
agent's corresponding directory; `FILE_COMMON` selects the terminals' shared
folder. File encoding, atomic publication, schema validation, stale input,
monotonic replay cutoff, duplicate IDs, and independent audit verification need
an integration design before that bridge is implemented. This prototype opens
no files. See the official [`FileOpen` rules](https://www.mql5.com/en/docs/files/fileopen).
