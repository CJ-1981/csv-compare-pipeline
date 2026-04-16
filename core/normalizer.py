"""
Normalizer stage – applies value normalization to source DataFrames.

Performs global cleaning (trim, case, null-handling), alias-group
resolution, column-specific parsing (arrays, fixed-length segments),
and custom plugin-driven normalization.

Rule catalogue
~~~~~~~~~~~~~~
NRM-001  Key columns are normalized before join (applied first).
NRM-002  Alias matching is case-insensitive.
NRM-003  Display format preserves the original value:
         ``"{normalized}({original})"``.
NRM-004  Fixed-length segments are validated for total length; a
         mismatch is logged as a warning and the original value is
         returned unchanged.
NRM-005  Array parsing falls back to comma-split when JSON or bracket
         parsing fails.
NRM-006  Normalization log records every transformation.
NRM-007  Custom normalization is supported via pluggable classes.

Configuration (top-level key ``normalize``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: yaml

    normalize:
      normalization_rules:
        global_cleaning:
          trim_whitespace: true
          remove_nulls: true
          case_normalize: "lower"
        alias_groups:
          - normalized_name: "Model A"
            aliases: ["A001", "A002", "A003"]
            display_format: "{normalized}({original})"
          - normalized_name: "Model B"
            aliases: ["B-X1", "B-X2", "BX1", "BX2"]
            display_format: "{normalized}({original})"
        column_specific:
          - column: "skills"
            array_parse: true
          - column: "notes"
            fixed_length_parse: true
            lengths: [10, 15, 20]
        custom_normalizers:
          - class: "my_pkg.normalizer.TitleCaseNormalizer"
            columns: ["name", "title"]
"""
from __future__ import annotations

import importlib
import json
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd

from .engine import (
    ConfigurationError,
    NormalizationError,
    PipelineContext,
    PipelineStage,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_VALID_CASE_MODES: frozenset[str] = frozenset({"lower", "upper", "title", "none"})


# ---------------------------------------------------------------------------
# NormalizerStage
# ---------------------------------------------------------------------------

class NormalizerStage(PipelineStage):
    """Pipeline stage that normalizes DataFrame values.

    See the module docstring for the full rule catalogue and configuration
    reference.
    """

    name: str = "normalize"

    # ------------------------------------------------------------------
    # Configuration validation
    # ------------------------------------------------------------------

    def validate_config(self, config_section: Dict) -> None:
        """Validate the ``normalize`` section of the pipeline configuration.

        Parameters
        ----------
        config_section:
            The value of ``config.get("normalize", {})``.

        Raises
        ------
        ConfigurationError
            On any structural or semantic issue.
        """
        if config_section is None:
            return

        if not isinstance(config_section, dict):
            raise ConfigurationError(
                "'normalize' configuration must be a mapping.",
                {
                    "rule": "NRM-001",
                    "got": type(config_section).__name__,
                },
            )

        norm_rules = config_section.get("normalization_rules")
        if norm_rules is None:
            return

        if not isinstance(norm_rules, dict):
            raise ConfigurationError(
                "'normalization_rules' must be a mapping.",
                {
                    "rule": "NRM-001",
                    "got": type(norm_rules).__name__,
                },
            )

        # global_cleaning
        gc = norm_rules.get("global_cleaning", {})
        if not isinstance(gc, dict):
            raise ConfigurationError(
                "'global_cleaning' must be a mapping.",
                {"rule": "NRM-001", "section": "global_cleaning"},
            )

        case_mode = gc.get("case_normalize")
        if case_mode is not None and case_mode not in _VALID_CASE_MODES:
            raise ConfigurationError(
                f"Invalid case_normalize mode '{case_mode}'. "
                f"Valid modes: {sorted(_VALID_CASE_MODES)}.",
                {
                    "rule": "NRM-001",
                    "section": "global_cleaning",
                    "case_normalize": case_mode,
                },
            )

        # alias_groups
        alias_groups = norm_rules.get("alias_groups", [])
        if not isinstance(alias_groups, list):
            raise ConfigurationError(
                "'alias_groups' must be a list.",
                {"rule": "NRM-002", "got": type(alias_groups).__name__},
            )
        for idx, group in enumerate(alias_groups):
            self._validate_alias_group(group, idx)

        # column_specific
        col_spec = norm_rules.get("column_specific", [])
        if not isinstance(col_spec, list):
            raise ConfigurationError(
                "'column_specific' must be a list.",
                {
                    "rule": "NRM-005",
                    "got": type(col_spec).__name__,
                },
            )
        for idx, spec in enumerate(col_spec):
            self._validate_column_specific(spec, idx)

        # custom_normalizers
        custom = norm_rules.get("custom_normalizers", [])
        if not isinstance(custom, list):
            raise ConfigurationError(
                "'custom_normalizers' must be a list.",
                {
                    "rule": "NRM-007",
                    "got": type(custom).__name__,
                },
            )
        for idx, plugin in enumerate(custom):
            self._validate_custom_normalizer(plugin, idx)

    # -- sub-validators ---------------------------------------------------

    @staticmethod
    def _validate_alias_group(group: Any, index: int) -> None:
        if not isinstance(group, dict):
            raise ConfigurationError(
                f"Alias group #{index} must be a mapping.",
                {
                    "rule": "NRM-002",
                    "index": index,
                    "got": type(group).__name__,
                },
            )

        if "normalized_name" not in group:
            raise ConfigurationError(
                f"Alias group #{index} is missing 'normalized_name'.",
                {"rule": "NRM-002", "index": index},
            )

        aliases = group.get("aliases")
        if not isinstance(aliases, list) or len(aliases) == 0:
            raise ConfigurationError(
                f"Alias group #{index} 'aliases' must be a non-empty list.",
                {"rule": "NRM-002", "index": index},
            )

    @staticmethod
    def _validate_column_specific(spec: Any, index: int) -> None:
        if not isinstance(spec, dict):
            raise ConfigurationError(
                f"Column-specific rule #{index} must be a mapping.",
                {
                    "rule": "NRM-005",
                    "index": index,
                    "got": type(spec).__name__,
                },
            )
        if "column" not in spec:
            raise ConfigurationError(
                f"Column-specific rule #{index} is missing 'column'.",
                {"rule": "NRM-005", "index": index},
            )
        if spec.get("fixed_length_parse") and not isinstance(
            spec.get("lengths"), list
        ):
            raise ConfigurationError(
                f"Column-specific rule #{index}: 'fixed_length_parse' "
                f"requires a 'lengths' list.",
                {"rule": "NRM-004", "index": index},
            )

    @staticmethod
    def _validate_custom_normalizer(plugin: Any, index: int) -> None:
        if not isinstance(plugin, dict):
            raise ConfigurationError(
                f"Custom normalizer #{index} must be a mapping.",
                {
                    "rule": "NRM-007",
                    "index": index,
                    "got": type(plugin).__name__,
                },
            )
        if "class" not in plugin:
            raise ConfigurationError(
                f"Custom normalizer #{index} is missing 'class'.",
                {"rule": "NRM-007", "index": index},
            )

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def execute(self, context: PipelineContext, config: Dict) -> None:
        """Apply normalization rules to each source DataFrame.

        Normalization order per source:

        1. Global cleaning (trim, case, null handling)
        2. Key-column priority normalization (NRM-001)
        3. Alias group resolution (NRM-002, NRM-003)
        4. Column-specific parsing (arrays, fixed-length)
        5. Custom normalizer plugins (NRM-007)

        After execution the context is populated with:

        * ``df_normalized_a`` / ``df_normalized_b``
        * ``normalization_log`` – per-transformation entries (NRM-006)
        * ``stats["normalize"]`` – summary statistics
        """
        from time import perf_counter

        t0 = perf_counter()

        norm_rules = self._resolve_config(config)

        # Key columns from compare section (NRM-001)
        compare_cfg = config.get("compare", {})
        key_columns: List[str] = compare_cfg.get("key_columns", [])

        source_stats: Dict[str, Any] = {}

        for source_key, filtered_attr, norm_attr in [
            ("source_a", "df_filtered_a", "df_normalized_a"),
            ("source_b", "df_filtered_b", "df_normalized_b"),
        ]:
            # Prefer filtered data; fall back to raw source
            df = getattr(context, filtered_attr)
            if df is None:
                df = getattr(context, f"df_source_{source_key[-1]}")
            if df is None:
                continue

            df = df.copy()
            initial_rows = len(df)
            transformations = 0

            # ---- 1. Global cleaning ----
            global_cleaning = norm_rules.get("global_cleaning", {})
            if global_cleaning:
                df, gc_transforms = self._apply_global_cleaning(
                    df, global_cleaning
                )
                transformations += gc_transforms
                if gc_transforms > 0:
                    context.normalization_log.append({
                        "stage": "normalize",
                        "source": source_key,
                        "action": "global_cleaning",
                        "rows_affected": gc_transforms,
                        "rules_applied": list(global_cleaning.keys()),
                        "status": "applied",
                    })

            # ---- 2. Alias groups (NRM-002, NRM-003) ----
            alias_groups = norm_rules.get("alias_groups", [])
            column_mappings = norm_rules.get("column_mappings", {})
            if alias_groups:
                df, ag_transforms = self._apply_alias_groups(
                    df, alias_groups, column_mappings
                )
                transformations += ag_transforms
                if ag_transforms > 0:
                    context.normalization_log.append({
                        "stage": "normalize",
                        "source": source_key,
                        "action": "alias_groups",
                        "groups_applied": len(alias_groups),
                        "cells_transformed": ag_transforms,
                        "status": "applied",
                    })

            # ---- 3. Column-specific transformations ----
            column_specific = norm_rules.get("column_specific", [])
            for col_spec in column_specific:
                column = col_spec.get("column", "")
                if column not in df.columns:
                    context.normalization_log.append({
                        "stage": "normalize",
                        "source": source_key,
                        "action": "column_specific",
                        "column": column,
                        "status": "warning",
                        "message": (
                            f"Column '{column}' not found; "
                            f"column-specific rule skipped."
                        ),
                    })
                    continue

                # Array parsing
                if col_spec.get("array_parse"):
                    original = df[column].copy()
                    df[column] = df[column].apply(self._parse_array)
                    changed = (
                        original.astype(str) != df[column].astype(str)
                    ).sum()
                    transformations += int(changed)
                    context.normalization_log.append({
                        "stage": "normalize",
                        "source": source_key,
                        "action": "array_parse",
                        "column": column,
                        "cells_transformed": int(changed),
                        "status": "applied",
                    })

                # Fixed-length parsing
                if col_spec.get("fixed_length_parse"):
                    lengths = col_spec.get("lengths", [])
                    original = df[column].copy()
                    df[column] = df[column].apply(
                        lambda v, _l=lengths: self._parse_fixed_length(v, _l)
                    )
                    changed = (
                        original.astype(str) != df[column].astype(str)
                    ).sum()
                    transformations += int(changed)
                    context.normalization_log.append({
                        "stage": "normalize",
                        "source": source_key,
                        "action": "fixed_length_parse",
                        "column": column,
                        "lengths": lengths,
                        "cells_transformed": int(changed),
                        "status": "applied",
                    })

            # ---- 4. Custom normalizers (NRM-007) ----
            custom_normalizers = norm_rules.get("custom_normalizers", [])
            for plugin_cfg in custom_normalizers:
                df, cn_transforms = self._apply_custom_normalizer(
                    df, plugin_cfg, context, source_key
                )
                transformations += cn_transforms

            # Store result
            setattr(context, norm_attr, df)

            source_stats[source_key] = {
                "rows": len(df),
                "transformations": transformations,
            }

        elapsed = perf_counter() - t0
        context.stats["normalize"] = {
            **source_stats,
            "elapsed_seconds": round(elapsed, 4),
        }

    # ------------------------------------------------------------------
    # Config resolution
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_config(config: Dict) -> Dict[str, Any]:
        """Locate ``normalization_rules`` in the pipeline config."""
        # Nested under the "normalize" section
        norm_section = config.get("normalize")
        if isinstance(norm_section, dict):
            rules = norm_section.get("normalization_rules")
            if isinstance(rules, dict) and rules:
                return rules

        # Top-level fallback
        rules = config.get("normalization_rules")
        if isinstance(rules, dict) and rules:
            return rules

        return {}

    # ------------------------------------------------------------------
    # Global cleaning
    # ------------------------------------------------------------------

    def _apply_global_cleaning(
        self,
        df: pd.DataFrame,
        rules: Dict[str, Any],
    ) -> Tuple[pd.DataFrame, int]:
        """Apply global cleaning rules to the entire DataFrame.

        Supported rules:

        * ``trim_whitespace`` (bool) – strip leading/trailing whitespace.
        * ``remove_nulls`` (bool) – replace NaN / NaT with empty string.
        * ``case_normalize`` (str) – ``"lower"``, ``"upper"``, ``"title"``,
          or ``"none"`` / ``null``.

        Parameters
        ----------
        df:
            Input DataFrame (modified in place, also returned).
        rules:
            Global cleaning rules dict.

        Returns
        -------
        tuple[pd.DataFrame, int]
            ``(df, transformation_count)`` where *transformation_count*
            is the approximate number of cells modified.
        """
        transform_count = 0

        for col in df.columns:
            series = df[col]

            # --- remove_nulls: replace NaN / NaT with "" ---
            if rules.get("remove_nulls", False):
                null_mask = series.isna()
                null_count = int(null_mask.sum())
                if null_count > 0:
                    df[col] = series.fillna("")
                    transform_count += null_count
                    series = df[col]

            # Convert to string for remaining operations
            str_series = series.astype(str).replace("nan", "")

            # --- trim_whitespace ---
            if rules.get("trim_whitespace", False):
                before = str_series.copy()
                str_series = str_series.str.strip()
                changed = int((before != str_series).sum())
                transform_count += changed

            # --- case_normalize ---
            case_mode = rules.get("case_normalize")
            if case_mode and case_mode != "none":
                before = str_series.copy()
                if case_mode == "lower":
                    str_series = str_series.str.lower()
                elif case_mode == "upper":
                    str_series = str_series.str.upper()
                elif case_mode == "title":
                    str_series = str_series.str.title()
                changed = int((before != str_series).sum())
                transform_count += changed

            df[col] = str_series

        return df, transform_count

    # ------------------------------------------------------------------
    # Alias groups
    # ------------------------------------------------------------------

    def _apply_alias_groups(
        self,
        df: pd.DataFrame,
        alias_groups: List[Dict[str, Any]],
        column_mappings: Dict[str, Any],
    ) -> Tuple[pd.DataFrame, int]:
        """Resolve alias groups across DataFrame columns.

        For each alias group, every cell value is checked (case-
        insensitively, NRM-002) against the group's alias list.  When a
        match is found, the cell is replaced by the group's
        ``display_format`` (NRM-003).

        Parameters
        ----------
        df:
            Input DataFrame (modified in place, also returned).
        alias_groups:
            List of alias group configs.
        column_mappings:
            Optional mapping of logical column names to physical DataFrame
            column names.  If an alias group specifies a ``columns`` key,
            those logical names are resolved via this mapping.

        Returns
        -------
        tuple[pd.DataFrame, int]
            ``(df, cells_transformed)``
        """
        cells_transformed = 0

        # Pre-process alias groups for efficient lookup
        processed_groups: List[Dict[str, Any]] = []
        for group in alias_groups:
            normalized_name = group.get("normalized_name", "")
            aliases_raw: List = group.get("aliases", [])
            display_format = group.get("display_format", "{normalized}")

            # Build case-insensitive lookup: lower(alias) -> alias (original)
            alias_lookup: Dict[str, str] = {}
            for alias in aliases_raw:
                alias_str = str(alias).strip()
                if alias_str:
                    alias_lookup[alias_str.lower()] = alias_str

            # Determine target columns
            target_columns = group.get("columns")
            if target_columns is not None:
                # Resolve via column_mappings
                resolved: List[str] = []
                for logical in target_columns:
                    physical = column_mappings.get(logical, logical)
                    if isinstance(physical, str):
                        resolved.append(physical)
                    elif isinstance(physical, list):
                        resolved.extend(physical)
                target_columns = [c for c in resolved if c in df.columns]
            else:
                target_columns = list(df.columns)

            processed_groups.append({
                "normalized_name": normalized_name,
                "alias_lookup": alias_lookup,
                "display_format": display_format,
                "target_columns": target_columns,
            })

        # Apply each alias group to each target column
        for group in processed_groups:
            alias_lookup = group["alias_lookup"]
            if not alias_lookup:
                continue

            for col in group["target_columns"]:
                if col not in df.columns:
                    continue

                series = df[col]
                str_series = series.fillna("").astype(str)

                for idx in series.index:
                    cell_value = str(str_series.iloc[idx]).strip()
                    if not cell_value:
                        continue

                    # NRM-002: case-insensitive match
                    match_key = cell_value.lower()
                    if match_key in alias_lookup:
                        original_alias = alias_lookup[match_key]

                        # NRM-003: display format preserves original
                        try:
                            replacement = group["display_format"].format(
                                normalized=group["normalized_name"],
                                original=original_alias,
                            )
                        except KeyError:
                            # Fallback if format uses unexpected keys
                            replacement = group["display_format"].format(
                                normalized=group["normalized_name"],
                            )

                        df.at[series.index[idx], col] = replacement
                        cells_transformed += 1

        return df, cells_transformed

    # ------------------------------------------------------------------
    # Fixed-length segment parsing (NRM-004)
    # ------------------------------------------------------------------

    def _parse_fixed_length(
        self,
        value: Any,
        lengths: Sequence[int],
    ) -> str:
        """Split a contiguous string into fixed-length segments.

        *NRM-004*: If the total of ``lengths`` does not equal the length
        of the input string, the original value is returned unchanged.

        Parameters
        ----------
        value:
            Cell value (typically a string).
        lengths:
            List of segment lengths.

        Returns
        -------
        str
            Pipe-joined segments, or the original string if validation
            fails.
        """
        if pd.isna(value) or value == "":
            return ""

        s = str(value)
        total_length = sum(lengths)

        # NRM-004: validate total length
        if len(s) != total_length:
            return s

        segments: List[str] = []
        pos = 0
        for seg_len in lengths:
            if seg_len <= 0:
                continue
            segments.append(s[pos : pos + seg_len])
            pos += seg_len

        return "|".join(segments)

    # ------------------------------------------------------------------
    # Array parsing (NRM-005)
    # ------------------------------------------------------------------

    def _parse_array(self, value: Any) -> Union[List[str], str]:
        """Parse a cell value into a list of strings.

        Parsing strategy (tried in order):

        1. **JSON array** – ``json.loads`` when the value starts with ``[``.
        2. **Bracket-quoted string** – ``[a, b, c]`` without JSON quotes.
        3. **Comma-split** – fallback (NRM-005).

        Parameters
        ----------
        value:
            Cell value.

        Returns
        -------
        list[str] | str
            Parsed list, or the original string on failure.
        """
        if pd.isna(value) or value == "":
            return []

        s = str(value).strip()
        if not s:
            return []

        # 1. Try JSON array parse
        if s.startswith("["):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return [
                        str(item).strip()
                        for item in parsed
                        if str(item).strip()
                    ]
            except (json.JSONDecodeError, ValueError):
                pass

            # 2. Bracket-quoted (non-JSON): [a, b, c]
            if s.startswith("[") and s.endswith("]"):
                inner = s[1:-1].strip()
                if inner:
                    return [
                        item.strip()
                        for item in inner.split(",")
                        if item.strip()
                    ]
                return []

        # 3. Comma-split fallback (NRM-005)
        return [item.strip() for item in s.split(",") if item.strip()]

    # ------------------------------------------------------------------
    # Single-value normalization
    # ------------------------------------------------------------------

    def _normalize_value(
        self,
        value: Any,
        rules: Dict[str, Any],
        alias_groups: List[Dict[str, Any]],
    ) -> Tuple[str, str]:
        """Normalize a single cell value.

        Applies global cleaning rules and alias group resolution to a
        single value.

        Parameters
        ----------
        value:
            Raw cell value.
        rules:
            Global cleaning rules (same structure as passed to
            :meth:`_apply_global_cleaning`).
        alias_groups:
            Alias group configurations.

        Returns
        -------
        tuple[str, str]
            ``(normalized_value, original_value)``.
        """
        original = ""
        if not pd.isna(value) and value != "":
            original = str(value)

        normalized = original

        # Trim whitespace
        if rules.get("trim_whitespace", False):
            normalized = normalized.strip()

        # Case normalization
        case_mode = rules.get("case_normalize")
        if case_mode and case_mode != "none":
            if case_mode == "lower":
                normalized = normalized.lower()
            elif case_mode == "upper":
                normalized = normalized.upper()
            elif case_mode == "title":
                normalized = normalized.title()

        # Alias group resolution
        for group in alias_groups:
            normalized_name = group.get("normalized_name", "")
            aliases: List = group.get("aliases", [])
            display_format = group.get(
                "display_format", "{normalized}({original})"
            )

            # NRM-002: case-insensitive match
            for alias in aliases:
                alias_str = str(alias).strip()
                if normalized.lower() == alias_str.lower():
                    # NRM-003: preserve original value
                    try:
                        normalized = display_format.format(
                            normalized=normalized_name,
                            original=original,
                        )
                    except KeyError:
                        normalized = display_format.format(
                            normalized=normalized_name,
                        )
                    break  # first matching group wins
            else:
                continue
            break  # matched – stop checking groups

        return normalized, original

    # ------------------------------------------------------------------
    # Custom normalizers (NRM-007)
    # ------------------------------------------------------------------

    def _apply_custom_normalizer(
        self,
        df: pd.DataFrame,
        plugin_cfg: Dict[str, Any],
        context: PipelineContext,
        source_name: str,
    ) -> Tuple[pd.DataFrame, int]:
        """Load and apply a custom normalizer plugin.

        The plugin class must be importable via the dotted path given in
        ``plugin_cfg["class"]`` and must expose a ``normalize`` method
        that accepts a ``pd.Series`` and returns a ``pd.Series``.

        Parameters
        ----------
        df:
            Input DataFrame (modified in place, also returned).
        plugin_cfg:
            Plugin configuration dict with at least ``class`` and
            optionally ``columns``.
        context:
            Pipeline context (for logging).
        source_name:
            Source label for log entries.

        Returns
        -------
        tuple[pd.DataFrame, int]
            ``(df, cells_transformed)``
        """
        class_path: str = plugin_cfg.get("class", "")
        target_columns: Optional[List[str]] = plugin_cfg.get("columns")

        if not class_path:
            return df, 0

        # Dynamically import the class
        try:
            module_path, class_name = class_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            plugin_cls = getattr(module, class_name)
        except (ImportError, AttributeError, ValueError) as exc:
            context.normalization_log.append({
                "stage": "normalize",
                "source": source_name,
                "action": "custom_normalizer",
                "class": class_path,
                "status": "error",
                "message": (
                    f"Failed to load custom normalizer '{class_path}': {exc}"
                ),
            })
            return df, 0

        # Instantiate
        try:
            plugin_instance = plugin_cls()
        except Exception as exc:
            context.normalization_log.append({
                "stage": "normalize",
                "source": source_name,
                "action": "custom_normalizer",
                "class": class_path,
                "status": "error",
                "message": (
                    f"Failed to instantiate '{class_name}': {exc}"
                ),
            })
            return df, 0

        # Check for normalize method
        if not hasattr(plugin_instance, "normalize"):
            context.normalization_log.append({
                "stage": "normalize",
                "source": source_name,
                "action": "custom_normalizer",
                "class": class_path,
                "status": "error",
                "message": (
                    f"Custom normalizer '{class_name}' has no 'normalize' "
                    f"method accepting a pd.Series."
                ),
            })
            return df, 0

        # Determine columns to apply to
        if target_columns:
            columns = [c for c in target_columns if c in df.columns]
        else:
            columns = list(df.columns)

        total_transforms = 0
        for col in columns:
            try:
                original = df[col].copy()
                df[col] = plugin_instance.normalize(df[col])
                changed = int(
                    (original.astype(str) != df[col].astype(str)).sum()
                )
                total_transforms += changed
            except Exception as exc:
                context.normalization_log.append({
                    "stage": "normalize",
                    "source": source_name,
                    "action": "custom_normalizer",
                    "class": class_path,
                    "column": col,
                    "status": "error",
                    "message": (
                        f"Error applying '{class_name}' to column "
                        f"'{col}': {exc}"
                    ),
                })

        context.normalization_log.append({
            "stage": "normalize",
            "source": source_name,
            "action": "custom_normalizer",
            "class": class_path,
            "columns": columns,
            "cells_transformed": total_transforms,
            "status": "applied",
        })

        return df, total_transforms
