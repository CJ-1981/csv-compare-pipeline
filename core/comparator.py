"""
Comparator stage – joins and compares two source DataFrames.

Performs key-based joining via ``pd.merge()`` and cell-level value
comparison across mapped column pairs. Supports exact and set-based
comparison methods with null-aware matching.

Rule catalogue
~~~~~~~~~~~~~~
CMP-001  Default join strategy is inner join via ``pd.merge()``.
CMP-002  Non-unique key columns produce a warning (not an error).
CMP-003  Null vs null is treated as an exact match.
CMP-004  Null vs non-null is a mismatch of type ``"missing"``.
CMP-005  Set comparison ignores element order.
CMP-006  Comparison is case-sensitive after normalization.

Configuration (top-level key ``compare``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: yaml

    compare:
      key_column:
        source_a: "product_id"
        source_b: "sku"
      join_type: "inner"            # inner | left | right | outer
      column_mappings:
        - source_a: "name"
          source_b: "title"
          method: "exact"
        - source_a: "skills"
          source_b: "skill_list"
          method: "set"
        - source_a: "model"
          source_b: "model_name"
          method: "exact"
"""
from __future__ import annotations

import json
import math
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import pandas as pd

from .engine import (
    ComparisonError,
    ConfigurationError,
    PipelineContext,
    PipelineStage,
)

# ---------------------------------------------------------------------------
# Supported comparison methods
# ---------------------------------------------------------------------------

VALID_METHODS: frozenset[str] = frozenset({"exact", "set"})

VALID_JOIN_TYPES: frozenset[str] = frozenset({"inner", "left", "right", "outer"})


# ---------------------------------------------------------------------------
# ComparatorStage
# ---------------------------------------------------------------------------

class ComparatorStage(PipelineStage):
    """Pipeline stage that joins and compares two DataFrames.

    See the module docstring for the full rule catalogue and configuration
    reference.
    """

    name: str = "compare"

    # ------------------------------------------------------------------
    # Configuration validation
    # ------------------------------------------------------------------

    def validate_config(self, config_section: Dict) -> None:
        """Validate the ``compare`` section of the pipeline configuration.

        Parameters
        ----------
        config_section:
            The value of ``config.get("compare", {})``.

        Raises
        ------
        ConfigurationError
            On any structural or semantic issue.
        """
        if config_section is None:
            return

        if not isinstance(config_section, dict):
            raise ConfigurationError(
                "'compare' configuration must be a mapping.",
                {"rule": "CMP-001", "got": type(config_section).__name__},
            )

        # key_column (dict with source_a / source_b)
        key_col = config_section.get("key_column")
        if isinstance(key_col, dict):
            if "source_a" not in key_col or "source_b" not in key_col:
                raise ConfigurationError(
                    "'key_column' must contain both 'source_a' and 'source_b'.",
                    {"rule": "CMP-001"},
                )
            if not isinstance(key_col.get("source_a"), str) or not key_col["source_a"].strip():
                raise ConfigurationError(
                    "'key_column.source_a' must be a non-empty string.",
                    {"rule": "CMP-001"},
                )
            if not isinstance(key_col.get("source_b"), str) or not key_col["source_b"].strip():
                raise ConfigurationError(
                    "'key_column.source_b' must be a non-empty string.",
                    {"rule": "CMP-001"},
                )

        # join_type
        join_type = config_section.get("join_type", "inner")
        if join_type not in VALID_JOIN_TYPES:
            raise ConfigurationError(
                f"Invalid join_type '{join_type}'. "
                f"Must be one of: {sorted(VALID_JOIN_TYPES)}.",
                {"rule": "CMP-001", "got": join_type},
            )

        # column_mappings
        mappings = config_section.get("column_mappings", [])
        if mappings is not None and not isinstance(mappings, list):
            raise ConfigurationError(
                "'column_mappings' must be a list.",
                {"rule": "CMP-001", "got": type(mappings).__name__},
            )
        if isinstance(mappings, list):
            for idx, mapping in enumerate(mappings):
                self._validate_column_mapping(mapping, idx)

    @staticmethod
    def _validate_column_mapping(mapping: Any, index: int) -> None:
        """Validate a single column mapping entry."""
        if not isinstance(mapping, dict):
            raise ConfigurationError(
                f"Column mapping #{index} must be a mapping.",
                {"rule": "CMP-001", "index": index, "got": type(mapping).__name__},
            )

        for required in ("source_a", "source_b"):
            if required not in mapping:
                raise ConfigurationError(
                    f"Column mapping #{index} is missing '{required}'.",
                    {"rule": "CMP-001", "index": index, "missing": required},
                )
            if not isinstance(mapping[required], str) or not mapping[required].strip():
                raise ConfigurationError(
                    f"Column mapping #{index}: '{required}' must be a non-empty string.",
                    {"rule": "CMP-001", "index": index},
                )

        method = mapping.get("method", "exact")
        if method not in VALID_METHODS:
            raise ConfigurationError(
                f"Column mapping #{index}: invalid method '{method}'. "
                f"Must be one of: {sorted(VALID_METHODS)}.",
                {"rule": "CMP-001", "index": index, "method": method},
            )

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def execute(self, context: PipelineContext, config: Dict) -> None:
        """Join DataFrames on key columns and compare mapped column pairs.

        After execution the context is populated with:

        * ``comparison_result`` – dict with matched_keys, only_in_a,
          only_in_b, and per-cell details.
        * ``stats["compare"]`` – summary statistics and timing.
        """
        from time import perf_counter

        t0 = perf_counter()

        compare_cfg = config.get("compare", {})

        # ---- Resolve key columns ----
        key_a, key_b = self._resolve_key_columns(compare_cfg)

        join_type: str = compare_cfg.get("join_type", "inner")
        column_mappings: List[Dict[str, Any]] = compare_cfg.get("column_mappings", [])

        # ---- Resolve source DataFrames (normalized > filtered > raw) ----
        df_a = context.df_normalized_a if context.df_normalized_a is not None else (
            context.df_filtered_a if context.df_filtered_a is not None else context.df_source_a
        )
        df_b = context.df_normalized_b if context.df_normalized_b is not None else (
            context.df_filtered_b if context.df_filtered_b is not None else context.df_source_b
        )

        if df_a is None or df_b is None:
            raise ComparisonError(
                "Both source DataFrames must be available for comparison.",
                {"rule": "CMP-001"},
            )

        # ---- Handle empty DataFrames gracefully ----
        if df_a.empty or df_b.empty:
            only_a_keys: List = (
                df_a[key_a].dropna().astype(str).unique().tolist()
                if not df_a.empty and key_a in df_a.columns
                else []
            )
            only_b_keys: List = (
                df_b[key_b].dropna().astype(str).unique().tolist()
                if not df_b.empty and key_b in df_b.columns
                else []
            )
            context.comparison_result = {
                "matched_keys": [],
                "only_in_a": only_a_keys,
                "only_in_b": only_b_keys,
                "details": [],
            }
            elapsed = perf_counter() - t0
            context.stats["compare"] = {
                "elapsed_seconds": round(elapsed, 4),
                "matched_keys_count": 0,
                "only_in_a_count": len(only_a_keys),
                "only_in_b_count": len(only_b_keys),
                "details_count": 0,
                "join_type": join_type,
                "status": "skipped_empty_input",
            }
            return

        # ---- Verify key columns exist ----
        if key_a not in df_a.columns:
            raise ComparisonError(
                f"Key column '{key_a}' not found in source A. "
                f"Available columns: {list(df_a.columns)}",
                {"rule": "CMP-001", "source": "a", "key": key_a},
            )
        if key_b not in df_b.columns:
            raise ComparisonError(
                f"Key column '{key_b}' not found in source B. "
                f"Available columns: {list(df_b.columns)}",
                {"rule": "CMP-001", "source": "b", "key": key_b},
            )

        # ---- CMP-002: warn on non-unique keys ----
        self._check_key_uniqueness(df_a, key_a, "source_a", context)
        self._check_key_uniqueness(df_b, key_b, "source_b", context)

        # ---- Validate mapped columns (warn on missing, skip silently) ----
        valid_mappings = self._validate_mapped_columns(
            column_mappings, df_a, df_b, context
        )

        # ---- CMP-001: perform join via pd.merge() ----
        merged, matched_keys, only_in_a, only_in_b = self._perform_join(
            df_a, df_b, key_a, key_b, join_type
        )

        # ---- Compare column pairs for every row ----
        details = self._compare_all_rows(
            merged, key_a, key_b, valid_mappings, df_a, df_b
        )

        # ---- Store results in context ----
        context.comparison_result = {
            "matched_keys": matched_keys,
            "only_in_a": only_in_a,
            "only_in_b": only_in_b,
            "details": details,
        }

        elapsed = perf_counter() - t0
        context.stats["compare"] = {
            "elapsed_seconds": round(elapsed, 4),
            "matched_keys_count": len(matched_keys),
            "only_in_a_count": len(only_in_a),
            "only_in_b_count": len(only_in_b),
            "details_count": len(details),
            "join_type": join_type,
            "columns_compared": len(valid_mappings),
        }

    # ------------------------------------------------------------------
    # Key column resolution
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_key_columns(compare_cfg: Dict) -> Tuple[str, str]:
        """Extract key column names from the compare config.

        Supports both the dict-style ``key_column`` (preferred) and the
        list-style ``key_columns`` (backward compatibility).
        """
        key_config = compare_cfg.get("key_column")
        if isinstance(key_config, dict) and "source_a" in key_config and "source_b" in key_config:
            key_a = str(key_config["source_a"]).strip()
            key_b = str(key_config["source_b"]).strip()
            if key_a and key_b:
                return key_a, key_b

        # Fallback: list-style key_columns (same name on both sides)
        key_columns_list = compare_cfg.get("key_columns", [])
        if isinstance(key_columns_list, list) and len(key_columns_list) > 0:
            first_key = str(key_columns_list[0]).strip()
            if first_key:
                return first_key, first_key

        raise ComparisonError(
            "No key_column or key_columns defined in compare config. "
            "Expected key_column: {source_a: ..., source_b: ...}.",
            {"rule": "CMP-001"},
        )

    # ------------------------------------------------------------------
    # Key uniqueness check (CMP-002)
    # ------------------------------------------------------------------

    def _check_key_uniqueness(
        self,
        df: pd.DataFrame,
        key_col: str,
        source_label: str,
        context: PipelineContext,
    ) -> None:
        """Warn if key column has duplicate values (CMP-002).

        Duplicates do **not** halt the pipeline – they only produce a
        warning in the log.
        """
        if key_col not in df.columns:
            return

        value_counts = df[key_col].dropna().value_counts()
        duplicates = value_counts[value_counts > 1]

        if len(duplicates) > 0:
            dup_values = sorted(duplicates.index.astype(str).tolist())[:20]
            context.filter_log.append({
                "stage": "compare",
                "source": source_label,
                "action": "duplicate_key_warning",
                "rule": "CMP-002",
                "message": (
                    f"{source_label} has {len(duplicates)} duplicate key value(s) "
                    f"in column '{key_col}': {dup_values}"
                    f"{' ...' if len(duplicates) > 20 else ''}. "
                    f"Duplicate keys may produce unexpected join results."
                ),
                "duplicate_count": int(len(duplicates)),
                "duplicate_keys": dup_values,
                "status": "warning",
            })

    # ------------------------------------------------------------------
    # Mapped column validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_mapped_columns(
        column_mappings: List[Dict[str, Any]],
        df_a: pd.DataFrame,
        df_b: pd.DataFrame,
        context: PipelineContext,
    ) -> List[Dict[str, Any]]:
        """Return only mappings whose columns exist in both DataFrames.

        Missing columns are logged as warnings.
        """
        valid: List[Dict[str, Any]] = []
        for mapping in column_mappings:
            col_a = mapping.get("source_a", "")
            col_b = mapping.get("source_b", "")

            a_ok = col_a in df_a.columns
            b_ok = col_b in df_b.columns

            if not a_ok:
                context.error_log.append({
                    "stage": "compare",
                    "error_type": "column_missing",
                    "column": col_a,
                    "source": "a",
                    "message": (
                        f"Column '{col_a}' not found in source A "
                        f"(available: {list(df_a.columns)[:8]}); "
                        f"mapping '{col_a} vs {col_b}' skipped."
                    ),
                })
            if not b_ok:
                context.error_log.append({
                    "stage": "compare",
                    "error_type": "column_missing",
                    "column": col_b,
                    "source": "b",
                    "message": (
                        f"Column '{col_b}' not found in source B "
                        f"(available: {list(df_b.columns)[:8]}); "
                        f"mapping '{col_a} vs {col_b}' skipped."
                    ),
                })

            if a_ok and b_ok:
                valid.append(mapping)

        return valid

    # ------------------------------------------------------------------
    # Join (CMP-001)
    # ------------------------------------------------------------------

    def _perform_join(
        self,
        df_a: pd.DataFrame,
        df_b: pd.DataFrame,
        key_a: str,
        key_b: str,
        join_type: str,
    ) -> Tuple[pd.DataFrame, List, List, List]:
        """Join two DataFrames using ``pd.merge()`` (CMP-001).

        Parameters
        ----------
        df_a, df_b:
            Source DataFrames.
        key_a, key_b:
            Key column names in each source.
        join_type:
            One of ``"inner"``, ``"left"``, ``"right"``, ``"outer"``.

        Returns
        -------
        tuple[pd.DataFrame, list, list, list]
            ``(merged_df, matched_keys, only_in_a, only_in_b)``
        """
        keys_a: Set[str] = set(df_a[key_a].dropna().astype(str).unique())
        keys_b: Set[str] = set(df_b[key_b].dropna().astype(str).unique())

        merged = pd.merge(
            df_a,
            df_b,
            left_on=key_a,
            right_on=key_b,
            how=join_type,
            suffixes=("_a", "_b"),
            indicator=False,
        )

        # Key-set analysis is independent of join type
        matched: Set[str] = keys_a & keys_b
        only_in_a_set: Set[str] = keys_a - keys_b
        only_in_b_set: Set[str] = keys_b - keys_a

        return (
            merged,
            sorted(matched),
            sorted(only_in_a_set),
            sorted(only_in_b_set),
        )

    # ------------------------------------------------------------------
    # Row-level comparison
    # ------------------------------------------------------------------

    def _compare_all_rows(
        self,
        merged: pd.DataFrame,
        key_a: str,
        key_b: str,
        column_mappings: List[Dict[str, Any]],
        df_a: pd.DataFrame,
        df_b: pd.DataFrame,
    ) -> List[Dict[str, Any]]:
        """Compare every mapped column pair for every row in the merged df.

        Returns a flat list of comparison detail dicts.
        """
        details: List[Dict[str, Any]] = []

        if merged.empty or not column_mappings:
            return details

        # Pre-compute which merged column names correspond to each source column.
        # pandas suffixes overlapping names with _a / _b.
        merged_cols = set(merged.columns)
        col_name_map_a: Dict[str, str] = {}
        col_name_map_b: Dict[str, str] = {}

        for mapping in column_mappings:
            col_a = mapping["source_a"]
            col_b = mapping["source_b"]

            # Determine the actual column name in the merged DataFrame
            if col_a in merged_cols:
                col_name_map_a[col_a] = col_a
            elif f"{col_a}_a" in merged_cols:
                col_name_map_a[col_a] = f"{col_a}_a"
            # else: column won't be found (should not happen after validation)

            if col_b in merged_cols:
                col_name_map_b[col_b] = col_b
            elif f"{col_b}_b" in merged_cols:
                col_name_map_b[col_b] = f"{col_b}_b"

        for _, row in merged.iterrows():
            # Determine canonical key value
            raw_key = row.get(key_a)
            if self._is_null(raw_key):
                raw_key = row.get(key_b)
            if self._is_null(raw_key):
                continue
            key_str = str(raw_key)

            for mapping in column_mappings:
                col_a = mapping["source_a"]
                col_b = mapping["source_b"]
                method = mapping.get("method", "exact")

                merged_col_a = col_name_map_a.get(col_a)
                merged_col_b = col_name_map_b.get(col_b)

                if merged_col_a is None or merged_col_b is None:
                    continue

                val_a = row.get(merged_col_a)
                val_b = row.get(merged_col_b)

                result = self._compare_values(val_a, val_b, method, mapping)

                detail: Dict[str, Any] = {
                    "key": key_str,
                    "column": f"{col_a} vs {col_b}",
                    "value_a": result["value_a"],
                    "value_b": result["value_b"],
                    "normalized_a": result["normalized_a"],
                    "normalized_b": result["normalized_b"],
                    "status": result["status"],
                    "diff_type": result["diff_type"],
                }

                # Carry extra fields for set comparison
                if "only_in_a" in result:
                    detail["only_in_a"] = result["only_in_a"]
                if "only_in_b" in result:
                    detail["only_in_b"] = result["only_in_b"]
                if "common" in result:
                    detail["common"] = result["common"]

                details.append(detail)

        return details

    # ------------------------------------------------------------------
    # Value comparison
    # ------------------------------------------------------------------

    def _compare_values(
        self,
        val_a: Any,
        val_b: Any,
        method: str,
        column_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Compare two cell values using the specified method.

        Handles null-awareness (CMP-003, CMP-004) and dispatches to the
        appropriate comparison strategy.

        Parameters
        ----------
        val_a, val_b:
            Raw cell values from the merged DataFrame.
        method:
            ``"exact"`` or ``"set"``.
        column_config:
            The full column mapping dict (for future extensibility).

        Returns
        -------
        dict
            ``{value_a, value_b, normalized_a, normalized_b, status, diff_type}``
            plus optional keys for set comparison (``only_in_a``, ``only_in_b``,
            ``common``).
        """
        raw_a = self._serializable_value(val_a)
        raw_b = self._serializable_value(val_b)

        null_a = self._is_null(val_a)
        null_b = self._is_null(val_b)

        # CMP-003: null vs null = match
        if null_a and null_b:
            return {
                "value_a": raw_a,
                "value_b": raw_b,
                "normalized_a": None,
                "normalized_b": None,
                "status": "match",
                "diff_type": "exact_match",
            }

        # CMP-004: null vs non-null = mismatch type "missing"
        if null_a and not null_b:
            return {
                "value_a": raw_a,
                "value_b": raw_b,
                "normalized_a": None,
                "normalized_b": self._normalize_for_comparison(val_b, method),
                "status": "mismatch",
                "diff_type": "missing_in_a",
            }

        if not null_a and null_b:
            return {
                "value_a": raw_a,
                "value_b": raw_b,
                "normalized_a": self._normalize_for_comparison(val_a, method),
                "normalized_b": None,
                "status": "mismatch",
                "diff_type": "missing_in_b",
            }

        # Both non-null – dispatch by method
        if method == "set":
            return self._compare_sets(val_a, val_b)

        # Default: exact comparison (CMP-006: case-sensitive after normalization)
        norm_a = self._normalize_for_comparison(val_a, method)
        norm_b = self._normalize_for_comparison(val_b, method)
        diff_type = self._detect_difference_type(norm_a, norm_b)

        return {
            "value_a": raw_a,
            "value_b": raw_b,
            "normalized_a": norm_a,
            "normalized_b": norm_b,
            "status": "match" if diff_type == "exact_match" else "mismatch",
            "diff_type": diff_type,
        }

    # ------------------------------------------------------------------
    # Set comparison (CMP-005)
    # ------------------------------------------------------------------

    def _compare_sets(self, val_a: Any, val_b: Any) -> Dict[str, Any]:
        """Set-based comparison ignoring element order (CMP-005).

        Parses both values into sets and computes:
        * ``only_in_a`` – elements present in A but not B
        * ``only_in_b`` – elements present in B but not A
        * ``common``    – elements present in both

        Returns
        -------
        dict
            Match result with set-specific metadata.
        """
        set_a = self._parse_to_set(val_a)
        set_b = self._parse_to_set(val_b)

        only_in_a = sorted(set_a - set_b)
        only_in_b = sorted(set_b - set_a)
        common = sorted(set_a & set_b)

        if set_a == set_b:
            status = "match"
            diff_type = "exact_match"
        else:
            status = "mismatch"
            diff_type = "set_diff"

        return {
            "value_a": self._serializable_value(val_a),
            "value_b": self._serializable_value(val_b),
            "normalized_a": sorted(set_a),
            "normalized_b": sorted(set_b),
            "status": status,
            "diff_type": diff_type,
            "only_in_a": only_in_a,
            "only_in_b": only_in_b,
            "common": common,
        }

    # ------------------------------------------------------------------
    # Difference classification
    # ------------------------------------------------------------------

    def _detect_difference_type(self, val_a: Any, val_b: Any) -> str:
        """Classify the type of difference between two values.

        Returns one of:

        * ``"exact_match"``  – values are identical or both null
        * ``"value_diff"``   – both non-null but differ
        * ``"missing_in_a"`` – *val_a* is null, *val_b* is not
        * ``"missing_in_b"`` – *val_b* is null, *val_a* is not
        * ``"set_diff"``     – set comparison yields differences
        """
        null_a = self._is_null(val_a)
        null_b = self._is_null(val_b)

        if null_a and null_b:
            return "exact_match"
        if null_a:
            return "missing_in_a"
        if null_b:
            return "missing_in_b"

        # Both non-null – stringify and compare
        str_a = str(val_a)
        str_b = str(val_b)

        if str_a == str_b:
            return "exact_match"

        return "value_diff"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_null(value: Any) -> bool:
        """Check whether *value* should be treated as null.

        Recognises ``None``, ``float('nan')``, ``pd.NA``, and pandas
        ``NaN`` values.
        """
        if value is None:
            return True
        if isinstance(value, float) and math.isnan(value):
            return True
        try:
            return bool(pd.isna(value))
        except (ValueError, TypeError):
            return False

    @staticmethod
    def _serializable_value(value: Any) -> Any:
        """Convert a value to a JSON-serializable form.

        * ``None`` / ``NaN`` → ``None``
        * ``list`` / ``set`` / ``tuple`` → ``list``
        * Everything else → as-is
        """
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (ValueError, TypeError):
            pass
        if isinstance(value, float) and math.isnan(value):
            return None
        if isinstance(value, (list, set, tuple)):
            return [ComparatorStage._serializable_value(item) for item in value]
        return value

    @staticmethod
    def _normalize_for_comparison(value: Any, method: str) -> Any:
        """Normalize a value for comparison.

        CMP-006: comparison is case-sensitive **after** normalization.
        Since the NormalizerStage has already applied any case transforms,
        this method simply ensures a consistent string representation.

        For ``"set"`` method the value is returned as-is (parsing happens
        inside ``_compare_sets``).
        """
        if method == "set":
            return value

        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (ValueError, TypeError):
            pass

        return str(value)

    @staticmethod
    def _parse_to_set(value: Any) -> Set[str]:
        """Parse a cell value into a set of strings (CMP-005).

        Parsing strategy (tried in order):

        1. If already a ``list`` / ``set`` / ``tuple`` – convert elements.
        2. If a string starting with ``[`` – try JSON, then bracket-split.
        3. Otherwise – comma-split fallback.
        """
        if value is None:
            return set()
        try:
            if pd.isna(value):
                return set()
        except (ValueError, TypeError):
            pass

        # Already iterable
        if isinstance(value, (list, set, tuple)):
            return {str(item).strip() for item in value if str(item).strip()}

        s = str(value).strip()
        if not s:
            return set()

        # Try JSON array
        if s.startswith("["):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return {str(item).strip() for item in parsed if str(item).strip()}
            except (json.JSONDecodeError, ValueError):
                pass

            # Bracket-quoted without JSON quotes: [a, b, c]
            if s.endswith("]"):
                inner = s[1:-1].strip()
                if inner:
                    return {item.strip() for item in inner.split(",") if item.strip()}
                return set()

        # Comma-split fallback
        return {item.strip() for item in s.split(",") if item.strip()}
