# Product Catalog Comparison Report

> Generated: 2026-04-16 06:19:13 UTC

## Summary

| Metric | Value |
|--------|-------|
| Total Rows (Source A) | 15 |
| Total Rows (Source B) | 17 |
| Keys Compared | 1 |
| Fully Matched Keys | 0 |
| Keys with Mismatches | 1 |
| Only in Source A | 1 |
| Only in Source B | 0 |
| Cell-Level Matches | 4 |
| Cell-Level Mismatches | 1 |
| **Match Rate** | **0.0%** |

## Mismatch Details

| Key | Column | Value A | Value B | Diff Type |
|-----|--------|---------|---------|-----------|
| p005 | status vs state | discontinued | archived | value_diff |

## Unmatched Records

### Only in Source A (1)

- `p013`

### Only in Source B (0)

*None*

## Per Column Breakdown

| Column | Matched | Mismatched | Total | Match Rate |
|--------|---------|------------|-------|------------|
| model vs model_code | 1 | 0 | 1 | 100.0% |
| name vs title | 1 | 0 | 1 | 100.0% |
| price vs unit_price | 1 | 0 | 1 | 100.0% |
| status vs state | 0 | 1 | 1 | 0.0% |
| stock vs inventory | 1 | 0 | 1 | 100.0% |

## Normalization Log

| Stage | Source | Action | Status |
| --- | --- | --- | --- |
| normalize | source_a | global_cleaning | applied |
| normalize | source_a | alias_groups | applied |
| normalize | source_b | global_cleaning | applied |
| normalize | source_b | alias_groups | applied |

## Filter Log

| Stage | Source | Action | Status |
| --- | --- | --- | --- |
| filter | source_a | skip | applied |
| filter | source_a | skip | applied |
| filter | source_b | skip | applied |
