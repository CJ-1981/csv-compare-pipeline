"""
Filter stage – applies row-level filtering rules to source DataFrames.

Each source (a / b) is filtered independently.  Rules within a single
source are evaluated **in config order** and combined with AND logic
(rows must survive **every** rule to remain).

Rule catalogue
~~~~~~~~~~~~~~
FLT-001  Filters apply per-source independently.
FLT-002  Rules evaluated in config order with AND logic.
FLT-003  Numeric operators (greater_than, less_than) coerce values to
         float.  Non-coercible values produce ``NaN`` masks that are
         treated as *no-match* (safe for AND chains).
FLT-004  Invalid regular expressions halt the pipeline with a clear
         ``FilterError`` that includes the pattern and the regex error.
FLT-005  Filter log records per-rule row counts (before / after /
         affected).

Configuration (top-level key ``filter``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: yaml

    filter:
      pre_filters:
        source_a:
          - column: "Stock"
            operator: "equals"
            value: 0
            action: "skip"
          - column: "Status"
            operator: "not_in"
            value: ["Deleted", "Archived"]
            action: "skip"
        source_b:
          - column: "Active"
            operator: "equals"
            value: false
            action: "skip"

Actions
-------
``skip``  – remove rows that *match* the rule condition.
``keep``  – retain **only** rows that match the rule condition.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .engine import (
    ConfigurationError,
    FilterError,
    PipelineContext,
    PipelineStage,
)

# ---------------------------------------------------------------------------
# Supported operators
# ---------------------------------------------------------------------------

VALID_OPERATORS: frozenset[str] = frozenset({
    "equals",
    "not_equals",
    "in",
    "not_in",
    "greater_than",
    "less_than",
    "contains",
    "matches_regex",
    "is_null",
    "is_not_null",
})

# Operators that require numeric coercion (FLT-003).
NUMERIC_OPERATORS: frozenset[str] = frozenset({
    "greater_than",
    "less_than",
})

# Operators that accept a scalar value and may benefit from smart
# type-aware comparison (bools, ints, floats vs. string data).
COMPARISON_OPERATORS: frozenset[str] = frozenset({
    "equals",
    "not_equals",
})

VALID_ACTIONS: frozenset[str] = frozenset({"skip", "keep"})

# Operators that do **not** require a ``value`` key.
NO_VALUE_OPERATORS: frozenset[str] = frozenset({"is_null", "is_not_null"})


# ---------------------------------------------------------------------------
# FilterStage
# ---------------------------------------------------------------------------

class FilterStage(PipelineStage):
    """Pipeline stage that applies row-level filter rules.

    See the module docstring for the full rule catalogue and configuration
    reference.
    """

    name: str = "filter"

    # ------------------------------------------------------------------
    # Configuration validation
    # ------------------------------------------------------------------

    def validate_config(self, config_section: Dict) -> None:
        """Validate the ``filter`` section of the pipeline configuration.

        Parameters
        ----------
        config_section:
            The value of ``config.get("filter", {})``.

        Raises
        ------
        ConfigurationError
            On any structural or semantic issue.
        """
        if config_section is None:
            return  # No filter config is a valid no-op.

        if not isinstance(config_section, dict):
            raise ConfigurationError(
                "'filter' configuration must be a mapping.",
                {"rule": "FLT-001", "got": type(config_section).__name__},
            )

        pre_filters = config_section.get("pre_filters")
        if pre_filters is None:
            return  # No pre_filters is a valid no-op.

        if not isinstance(pre_filters, dict):
            raise ConfigurationError(
                "'pre_filters' must be a mapping of source names to rule lists.",
                {
                    "rule": "FLT-001",
                    "got": type(pre_filters).__name__,
                },
            )

        for source_name, rules in pre_filters.items():
            if not isinstance(source_name, str) or not source_name.strip():
                raise ConfigurationError(
                    "Each source key in 'pre_filters' must be a non-empty string.",
                    {"rule": "FLT-001", "key": repr(source_name)},
                )
            if not isinstance(rules, list):
                raise ConfigurationError(
                    f"Rules for source '{source_name}' must be a list.",
                    {
                        "rule": "FLT-001",
                        "source": source_name,
                        "got": type(rules).__name__,
                    },
                )
            for idx, rule in enumerate(rules):
                self._validate_rule(rule, source_name, idx)

    def _validate_rule(
        self,
        rule: Any,
        source_name: str,
        index: int,
    ) -> None:
        """Validate a single filter rule dict."""
        if not isinstance(rule, dict):
            raise ConfigurationError(
                f"Rule #{index} for source '{source_name}' must be a mapping.",
                {
                    "rule": "FLT-002",
                    "source": source_name,
                    "index": index,
                    "got": type(rule).__name__,
                },
            )

        # Required keys
        required_keys = {"column", "operator", "action"}
        missing = required_keys - set(rule.keys())
        if missing:
            raise ConfigurationError(
                f"Rule #{index} for source '{source_name}' is missing "
                f"required key(s): {sorted(missing)}.",
                {
                    "rule": "FLT-002",
                    "source": source_name,
                    "index": index,
                    "missing": sorted(missing),
                },
            )

        # column
        column = rule["column"]
        if not isinstance(column, str) or not column.strip():
            raise ConfigurationError(
                f"Rule #{index} for source '{source_name}': "
                f"'column' must be a non-empty string.",
                {"rule": "FLT-002", "source": source_name, "index": index},
            )

        # operator
        operator = rule["operator"]
        if operator not in VALID_OPERATORS:
            raise ConfigurationError(
                f"Rule #{index} for source '{source_name}': "
                f"unknown operator '{operator}'. "
                f"Valid operators: {sorted(VALID_OPERATORS)}.",
                {
                    "rule": "FLT-002",
                    "source": source_name,
                    "index": index,
                    "operator": operator,
                },
            )

        # value – required for all operators except is_null / is_not_null
        if operator not in NO_VALUE_OPERATORS and "value" not in rule:
            raise ConfigurationError(
                f"Rule #{index} for source '{source_name}': "
                f"operator '{operator}' requires a 'value' key.",
                {
                    "rule": "FLT-002",
                    "source": source_name,
                    "index": index,
                    "operator": operator,
                },
            )

        # value type for in / not_in must be a list
        if operator in ("in", "not_in"):
            if not isinstance(rule.get("value"), list):
                raise ConfigurationError(
                    f"Rule #{index} for source '{source_name}': "
                    f"operator '{operator}' requires 'value' to be a list.",
                    {
                        "rule": "FLT-002",
                        "source": source_name,
                        "index": index,
                        "operator": operator,
                    },
                )
            value_list: List = rule["value"]
            if len(value_list) == 0:
                raise ConfigurationError(
                    f"Rule #{index} for source '{source_name}': "
                    f"operator '{operator}' requires a non-empty value list.",
                    {
                        "rule": "FLT-002",
                        "source": source_name,
                        "index": index,
                        "operator": operator,
                    },
                )

        # action
        action = rule["action"]
        if action not in VALID_ACTIONS:
            raise ConfigurationError(
                f"Rule #{index} for source '{source_name}': "
                f"unknown action '{action}'. "
                f"Valid actions: {sorted(VALID_ACTIONS)}.",
                {
                    "rule": "FLT-002",
                    "source": source_name,
                    "index": index,
                    "action": action,
                },
            )

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def execute(self, context: PipelineContext, config: Dict) -> None:
        """Apply filter rules to each source DataFrame independently.

        *FLT-001*: filters apply per-source independently.
        *FLT-002*: rules within each source are evaluated in order with
        AND logic (a row must survive every rule to remain).

        After execution the context is populated with:

        * ``df_filtered_a`` / ``df_filtered_b`` – filtered DataFrames
        * ``rows_filtered_a`` / ``rows_filtered_b`` – filtered row counts
        * ``filter_log`` – per-rule log entries (FLT-005)
        * ``stats["filter"]`` – summary statistics
        """
        from time import perf_counter

        t0 = perf_counter()

        # Resolve pre_filters from config (supports two locations)
        pre_filters = self._resolve_config(config)

        source_stats: Dict[str, Any] = {}

        # Process each source independently (FLT-001)
        for source_key, df_attr, filtered_attr, rows_attr in [
            ("source_a", "df_source_a", "df_filtered_a", "rows_filtered_a"),
            ("source_b", "df_source_b", "df_filtered_b", "rows_filtered_b"),
        ]:
            df = getattr(context, df_attr)
            if df is None:
                continue

            rules = pre_filters.get(source_key, [])
            initial_rows = len(df)

            if not rules:
                # No rules for this source – pass through unchanged
                setattr(context, filtered_attr, df.copy())
                setattr(context, rows_attr, initial_rows)
                context.filter_log.append({
                    "stage": "filter",
                    "source": source_key,
                    "action": "no_rules",
                    "message": (
                        f"No filter rules for {source_key}; "
                        f"all {initial_rows} row(s) retained."
                    ),
                    "rows_before": initial_rows,
                    "rows_after": initial_rows,
                    "rows_affected": 0,
                    "status": "passthrough",
                })
                source_stats[source_key] = {
                    "rules_applied": 0,
                    "rows_before": initial_rows,
                    "rows_after": initial_rows,
                    "rows_affected": 0,
                }
                continue

            # Apply rules sequentially (FLT-002 – AND logic)
            filtered_df = df.copy()
            total_affected = 0

            for rule_idx, rule in enumerate(rules):
                filtered_df, log_entry = self._evaluate_rule(
                    filtered_df,
                    rule,
                    source_key,
                    rule_idx,
                )
                context.filter_log.append(log_entry)  # FLT-005
                total_affected += log_entry.get("rows_affected", 0)

            # Reset index for a clean DataFrame
            filtered_df = filtered_df.reset_index(drop=True)

            setattr(context, filtered_attr, filtered_df)
            setattr(context, rows_attr, len(filtered_df))

            source_stats[source_key] = {
                "rules_applied": len(rules),
                "rows_before": initial_rows,
                "rows_after": len(filtered_df),
                "rows_affected": total_affected,
            }

        elapsed = perf_counter() - t0
        context.stats["filter"] = {
            **source_stats,
            "elapsed_seconds": round(elapsed, 4),
        }

    # ------------------------------------------------------------------
    # Config resolution
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_config(config: Dict) -> Dict[str, Any]:
        """Locate the ``pre_filters`` mapping in the pipeline config.

        Supports two canonical locations:

        1. ``config["filter"]["pre_filters"]``  (nested)
        2. ``config["pre_filters"]``             (top-level fallback)
        """
        # Nested under the "filter" section
        filter_section = config.get("filter")
        if isinstance(filter_section, dict):
            pre_filters = filter_section.get("pre_filters")
            if isinstance(pre_filters, dict) and pre_filters:
                return pre_filters

        # Top-level fallback
        pre_filters = config.get("pre_filters")
        if isinstance(pre_filters, dict) and pre_filters:
            return pre_filters

        return {}

    # ------------------------------------------------------------------
    # Single-rule evaluation
    # ------------------------------------------------------------------

    def _evaluate_rule(
        self,
        df: pd.DataFrame,
        rule: Dict[str, Any],
        source_name: str,
        rule_index: int,
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Evaluate a single filter rule against a DataFrame.

        Parameters
        ----------
        df:
            The **already-filtered** DataFrame (rules are chained with
            AND logic – FLT-002).
        rule:
            A validated rule dict with at least ``column``, ``operator``,
            ``action``, and (usually) ``value``.
        source_name:
            Human-readable label (``"source_a"`` or ``"source_b"``).
        rule_index:
            Zero-based rule index for logging.

        Returns
        -------
        tuple[pd.DataFrame, dict]
            ``(filtered_df, log_entry)`` where *log_entry* satisfies
            FLT-005.
        """
        column: str = rule["column"]
        operator: str = rule["operator"]
        action: str = rule.get("action", "skip")
        value: Any = rule.get("value")

        rows_before = len(df)

        # Guard: column existence
        if column not in df.columns:
            log_entry: Dict[str, Any] = {
                "stage": "filter",
                "source": source_name,
                "rule_index": rule_index,
                "column": column,
                "operator": operator,
                "action": action,
                "value": value,
                "rows_before": rows_before,
                "rows_after": rows_before,
                "rows_affected": 0,
                "status": "warning",
                "message": (
                    f"Column '{column}' not found in {source_name} "
                    f"(available: {list(df.columns)[:5]}...); rule skipped."
                ),
            }
            return df, log_entry

        series = df[column]

        # Compute boolean mask
        try:
            mask = self._apply_operator(series, operator, value)
        except FilterError:
            raise
        except Exception as exc:
            raise FilterError(
                f"Error evaluating filter rule #{rule_index} on "
                f"{source_name}.{column} with operator '{operator}': {exc}",
                {
                    "rule": "FLT-003",
                    "source": source_name,
                    "rule_index": rule_index,
                    "column": column,
                    "operator": operator,
                },
            ) from exc

        # Apply the mask based on action
        if action == "skip":
            # Remove rows where mask is True
            filtered_df = df.loc[~mask]
        else:
            # "keep" – retain only rows where mask is True
            filtered_df = df.loc[mask]

        rows_after = len(filtered_df)

        # FLT-005: per-rule row counts
        log_entry = {
            "stage": "filter",
            "source": source_name,
            "rule_index": rule_index,
            "column": column,
            "operator": operator,
            "action": action,
            "value": value if operator not in NO_VALUE_OPERATORS else None,
            "rows_before": rows_before,
            "rows_after": rows_after,
            "rows_affected": rows_before - rows_after,
            "status": "applied",
        }

        return filtered_df, log_entry

    # ------------------------------------------------------------------
    # Operator application
    # ------------------------------------------------------------------

    def _apply_operator(
        self,
        series: pd.Series,
        operator: str,
        value: Any,
    ) -> pd.Series:
        """Apply a filter operator and return a boolean mask.

        *FLT-003*: Numeric operators coerce values to float.
        *FLT-004*: Invalid regular expressions raise ``FilterError``.

        Parameters
        ----------
        series:
            The column to filter.
        operator:
            One of :data:`VALID_OPERATORS`.
        value:
            The comparison value (unused for ``is_null`` / ``is_not_null``).

        Returns
        -------
        pd.Series[bool]
            Boolean mask – ``True`` where the condition is satisfied.

        Raises
        ------
        FilterError
            On regex compilation failure (FLT-004) or numeric coercion
            failure (FLT-003).
        """
        # ---- Null-check operators ----
        if operator == "is_null":
            return series.isna() | (
                series.fillna("").astype(str).str.strip().eq("")
            )

        if operator == "is_not_null":
            return ~(
                series.isna()
                | series.fillna("").astype(str).str.strip().eq("")
            )

        # ---- Numeric operators (FLT-003) ----
        if operator in NUMERIC_OPERATORS:
            return self._apply_numeric_operator(series, operator, value)

        # ---- Equality / inequality ----
        if operator in COMPARISON_OPERATORS:
            str_series = series.fillna("").astype(str)
            if operator == "equals":
                return self._equals(str_series, value)
            return ~self._equals(str_series, value)

        # ---- Set membership ----
        if operator == "in":
            return self._in_operator(series, value)

        if operator == "not_in":
            return ~self._in_operator(series, value)

        # ---- Substring ----
        if operator == "contains":
            str_series = series.fillna("").astype(str)
            return str_series.str.contains(
                str(value), na=False, regex=False
            )

        # ---- Regex (FLT-004) ----
        if operator == "matches_regex":
            return self._matches_regex(series, value)

        # Unreachable due to validate_config guard
        raise FilterError(
            f"Unknown operator: '{operator}'",
            {"rule": "FLT-002", "operator": operator},
        )

    # -- numeric helpers -------------------------------------------------

    @staticmethod
    def _apply_numeric_operator(
        series: pd.Series,
        operator: str,
        value: Any,
    ) -> pd.Series:
        """Coerce both sides to float and compare (FLT-003).

        Non-coercible cell values become ``NaN``, which never satisfies
        any comparison → treated as *no-match* (safe for AND chains).
        """
        try:
            numeric_series = pd.to_numeric(series, errors="coerce")
            float_value = float(value)
        except (ValueError, TypeError) as exc:
            raise FilterError(
                f"Cannot coerce to float for operator '{operator}': "
                f"value={value!r}, error={exc}",
                {
                    "rule": "FLT-003",
                    "operator": operator,
                    "column": series.name,
                    "value": repr(value),
                },
            ) from exc

        if operator == "greater_than":
            return numeric_series > float_value
        return numeric_series < float_value

    # -- equality helpers ------------------------------------------------

    @staticmethod
    def _coerce_scalar_for_comparison(val: Any) -> str:
        """Canonical string form for comparison."""
        if isinstance(val, bool):
            return str(val).lower()  # "true" / "false"
        return str(val)

    @classmethod
    def _equals(cls, str_series: pd.Series, value: Any) -> pd.Series:
        """Smart equality comparison handling bools and numeric strings.

        * ``bool`` values → case-insensitive compare (``"true"`` / ``"false"``).
        * ``int`` / ``float`` values → try numeric compare first, then string.
        * Otherwise → plain string equality.
        """
        if isinstance(value, bool):
            return str_series.str.lower() == str(value).lower()

        # Numeric values: attempt numeric comparison first
        if isinstance(value, (int, float)):
            try:
                numeric_series = pd.to_numeric(str_series, errors="coerce")
                float_value = float(value)
                numeric_match: pd.Series = numeric_series == float_value
                # Use numeric match where it succeeds; fall back to string
                string_match = str_series == str(value)
                return numeric_match.fillna(string_match)
            except (ValueError, TypeError):
                pass

        return str_series == str(value)

    @classmethod
    def _in_operator(cls, series: pd.Series, value: Any) -> pd.Series:
        """Set-membership check (case-insensitive for bools)."""
        str_series = series.fillna("").astype(str)
        value_set = {
            cls._coerce_scalar_for_comparison(v) for v in value
        }
        return str_series.apply(
            lambda x: cls._coerce_scalar_for_comparison(x) in value_set
        )

    # -- regex helper (FLT-004) ------------------------------------------

    @staticmethod
    def _matches_regex(series: pd.Series, pattern: Any) -> pd.Series:
        """Apply a regex pattern using ``fullmatch`` semantics.

        *FLT-004*: regex compilation errors raise ``FilterError``.
        """
        pattern_str = str(pattern)
        try:
            # Compile first to get a clear error message
            re.compile(pattern_str)
        except re.error as exc:
            raise FilterError(
                f"Invalid regular expression in filter rule: "
                f"pattern='{pattern_str}', error='{exc}'",
                {
                    "rule": "FLT-004",
                    "column": series.name,
                    "pattern": pattern_str,
                    "regex_error": str(exc),
                },
            ) from exc

        str_series = series.fillna("").astype(str)
        return str_series.str.fullmatch(pattern_str, na=False)
