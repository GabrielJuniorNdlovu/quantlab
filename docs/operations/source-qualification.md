# Inspect an authorized source sample

F00 commands run offline without database configuration or a trading account. Review
[the source assessment](../data-sources/boom1000-source-assessment.md) before selecting a source.
Neither initial candidate is qualified for durable research acquisition.

## Read the current evidence

```bash
quantlab source list
quantlab source describe deriv-mt5-recording
mkdir -p source-reports
quantlab source report deriv-public-api --output source-reports/deriv-api-qualification.json
```

The list shows `BLOCKED` for the public API and `DOCUMENTED` for the terminal-recording route.
Report generation exits successfully even when the source is blocked. Read its qualification
and blockers rather than treating a successful command as source acceptance.

To load an explicitly reviewed alternative registry, put the option before the operation:

```bash
quantlab source --registry /path/to/reviewed-sources.json list
```

## Inspect known files without guessing a profile

Supply only a recording you are authorized to inspect. The paths below are placeholders,
not shipped samples. Obtain the actual file, field names, and permission reference first.

For a native history JSON envelope, the documented paths can describe its arrays without
converting epochs or claiming the envelope is one market tick:

```bash
quantlab source inspect /path/to/authorized-history.json \
	--source-id deriv-public-api --format json \
	--event-field 'history.times[]' --price-field 'history.prices[]' \
	--timestamp uninterpreted --sample-kind genuine \
	--authorization-reference /path/to/applicable-permission \
	--original-reference /path/to/acquisition-notes \
	--output source-reports/history-inspection.json
```

This command does not resolve the public API's storage restriction. The permission reference
and `genuine` label are caller declarations. An independent reviewer must establish their
meaning and applicability. Do not put credentials or account payloads into samples or references.

Use `--format csv` for CSV, or `--format jsonl` for one object per line. `--event-field`,
`--price-field`, and `--instrument-field` select actual fields. Omit unknown bindings rather
than inventing them. The report records selected scalar types and bounded decoded examples.
`--timestamp iso8601` requests F01 parsing; the two Unix choices record a declared unit only.
No numeric epoch conversion is performed.

Use `--acquired-at` only when the acquisition time is known. It requires an explicitly zoned
ISO timestamp and remains separate from market event and inspection time. The default sample
kind is `unknown`; no file extension or successful parse authenticates a file.

## Check canonical compatibility after mapping review

When you have verified source identity, precision, fields, and ISO timestamp interpretation,
supply the existing F04 profile:

```bash
quantlab source inspect /path/to/authorized-export.csv \
	--source-id deriv-mt5-recording --config /path/to/reviewed-historical-profile.json \
	--sample-kind genuine --authorization-reference /path/to/applicable-permission \
	--output source-reports/canonical-inspection.json
```

`canonical_compatibility` then contains the existing F04/F01 outcome, including errors and
accepted timestamp bounds. A valid result establishes configured schema compatibility only.
If required source metadata is unresolved, keep the profile absent and retain structural
inspection as incomplete qualification evidence.

Inspect stdout when no output path is specified. Existing output paths are refused. Keep
the original recording and report together under your permitted retention policy. A report
contains hashes and summaries; it is not an original-source archive.

## Interpret results and qualify by review

Exit 0 means the requested operation completed without detected structural/selected-value
errors or blocked canonical compatibility. It does not mean source qualification. Exit 1
retains an inspection report with detected data or compatibility failures. Exit 2 returns
`source_error.v1` for configuration, I/O, or publication failure. Argparse usage errors also
return 2. Incomplete scans expose their parsed-prefix counts and `inspection_complete=false`.

Review genuine provenance, applicable permission, exact identity, sample compatibility,
historical coverage, and continuity requirements before editing registry state. Reference the
reviewed evidence and permission when progressing to `SAMPLE_VERIFIED` or `QUALIFIED`.
The inspector never makes that edit. The separate follow-up task will test these paths.
