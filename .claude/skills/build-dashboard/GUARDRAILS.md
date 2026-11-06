# Dashboard build guardrails

These are hard rules. Violating any of them produces empty or broken panels in front of a
live audience, which is worse than a shorter dashboard.

---

## Rule 1 — No panel without a passing `run_esql` call

Every query **must** be validated with `run_esql` before it is passed to `create_chart`,
`create_metric`, or `create_heatmap`. This is not optional and cannot be skipped for any
reason, including time pressure or user encouragement to "just try it".

A query **passes** only when:

- `run_esql` returns no error, AND
- The result set contains **at least one row with at least one non-null value column**.

An empty result set (`[]`) is a **failure**, not a pass.

---

## Rule 2 — Byte-identical strings only

The query string passed to a panel-creation tool must be **byte-identical** to the string
that was validated. If you change a single character — even a `LIMIT`, a field alias, a
whitespace change, or a color-unrelated parameter — re-run `run_esql` on the new string
before using it.

---

## Rule 3 — Diagnose before giving up; drop rather than guess

When a query fails, diagnose and retry in this order (budget 3 attempts per panel):

1. **Misspelled or wrong field name** — re-run `get_fields` and correct the field name.
2. **Filter excludes all documents** — remove or loosen the `WHERE` clause and re-test.
3. **Time filter outside the data span** — confirm the date range against the Step 3
   probe values; adjust the query's time filter or use `| WHERE @timestamp IS NOT NULL`.
4. **Aggregation over a mostly-null field** — try `COUNT(*)` instead, or pick a different
   field.
5. **Syntax not supported by this stack version** — consult `esql://reference` and
   rewrite.

After 3 failed attempts, **drop the panel**. Do not create it hoping it renders. Report
it in the Step 7 reply with the failure reason.

---

## Rule 4 — Sanity-check the shape, not just the count

Rows exist ≠ the panel will look right. Before accepting a query, check:

- **Time series:** the result must have more than one distinct time bucket. A single-row
  result means the interval is too coarse or the query is wrong.
- **Breakdown / by-series:** the result must have more than one distinct category value.
  A single category means the grouping field has only one value in this data set — choose
  a different field.
- **Metric:** all value columns must have at least one non-null, non-zero value. All-null
  or all-zero means the field doesn't exist or is always null for this index.

---

## Rule 5 — `timeField` must match the query

Set `timeField` to **the same date field you used in the query** (e.g. if the query
uses `BUCKET(event.created, 1 hour)`, set `timeField: "event.created"`). A `timeField`
mismatch causes Kibana's time picker to filter out all documents even though the query
validated fine. Omit `timeField` only when the field is literally `@timestamp` **and**
you confirmed in Step 3 that auto-detection works.

---

## Rule 6 — Dashboard time range must fit the data

Set the dashboard's default time range to the window returned by the Step 3 probe
(`MIN` / `MAX` of the date field). Never leave it at a default like "Last 15 minutes" if
the data doesn't extend into that range.

---

## Rule 7 — Export without asking

After the last valid panel is created, call `export_to_kibana` immediately. Do not:

- Ask the user "should I export this?"
- Stop at `view_dashboard` to request confirmation
- Wait for further instructions before exporting

The user can refine the dashboard after seeing it. Getting them to the URL is the goal.
