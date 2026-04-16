"""
Wizard module – interactive configuration creation using prompt_toolkit.

Provides the :class:`ConfigWizard` class that walks the user through
configuring a CSV comparison pipeline step-by-step, validates inputs,
reads CSV headers for autocomplete, and produces a ready-to-use YAML
configuration file.
"""
from __future__ import annotations

import csv
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from prompt_toolkit import prompt
    from prompt_toolkit.completion import WordCompleter
    from prompt_toolkit.validation import ValidationError, Validator
    _HAS_PROMPT_TOOLKIT = True
except ImportError:
    _HAS_PROMPT_TOOLKIT = False
    prompt = None  # type: ignore[assignment, misc]
    WordCompleter = None  # type: ignore[assignment, misc]
    Validator = None  # type: ignore[assignment, misc]
    ValidationError = None  # type: ignore[assignment, misc]


# ---------------------------------------------------------------------------
# Prompt helpers (graceful fallback when prompt_toolkit is missing)
# ---------------------------------------------------------------------------

def _prompt(message: str, default: str = "", completer=None, validator=None) -> str:
    """Prompt for user input with optional autocomplete and validation.

    Falls back to plain ``input()`` when prompt_toolkit is not available.
    """
    suffix = f" [{default}]" if default else ""
    if _HAS_PROMPT_TOOLKIT and prompt is not None:
        try:
            return prompt(
                f"{message}{suffix}: ",
                default=default,
                completer=completer,
                validator=validator,
            )
        except (EOFError, KeyboardInterrupt):
            print()
            return default
    else:
        try:
            val = input(f"{message}{suffix}: ").strip()
            return val if val else default
        except (EOFError, KeyboardInterrupt):
            print()
            return default


def _prompt_yes_no(message: str, default: bool = True) -> bool:
    """Prompt for a yes/no answer."""
    hint = "Y/n" if default else "y/N"
    raw = _prompt(f"{message} ({hint})", default="y" if default else "n").strip().lower()
    return raw in ("y", "yes", "true", "1")


# ---------------------------------------------------------------------------
# File validation helpers
# ---------------------------------------------------------------------------

def _read_csv_columns(file_path: str, delimiter: str = ",", encoding: str = "utf-8") -> List[str]:
    """Read the header row from a CSV file and return column names.

    Parameters
    ----------
    file_path:
        Path to the CSV file.
    delimiter:
        Field delimiter (default ``","``).
    encoding:
        File encoding (default ``"utf-8"``).

    Returns
    -------
    list[str]
        List of column names from the first row.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    ValueError
        If the file cannot be parsed.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    try:
        with open(path, "r", encoding=encoding, newline="") as fh:
            reader = csv.reader(fh, delimiter=delimiter)
            headers = next(reader, None)
            if not headers:
                raise ValueError(f"CSV file is empty or has no header: {file_path}")
            return [h.strip() for h in headers if h.strip()]
    except UnicodeDecodeError:
        # Try latin-1 as a fallback
        with open(path, "r", encoding="latin-1", newline="") as fh:
            reader = csv.reader(fh, delimiter=delimiter)
            headers = next(reader, None)
            if not headers:
                raise ValueError(f"CSV file is empty or has no header: {file_path}")
            return [h.strip() for h in headers if h.strip()]


# ---------------------------------------------------------------------------
# ConfigWizard
# ---------------------------------------------------------------------------

class ConfigWizard:
    """Interactive wizard that guides the user through creating a pipeline
    configuration file.

    Usage::

        wizard = ConfigWizard()
        yaml_string = wizard.run()
        # yaml_string is ready to be written to config.yaml

    The wizard collects:

    1. Two source CSV files (path, delimiter, encoding).
    2. Key columns for joining.
    3. Column mappings (which columns to compare and how).
    4. Optional filter rules.
    5. Optional normalization (alias groups).
    6. Output format preferences.
    """

    VERSION: str = "1.0"

    def run(self) -> str:
        """Run the full interactive wizard.

        Returns
        -------
        str
            A complete YAML configuration string ready to be saved.
        """
        print()
        print("=" * 64)
        print("  CSV Compare Pipeline — Configuration Wizard")
        print("=" * 64)
        print()

        # Step 1: Source A
        source_a = self._prompt_source("Source A")

        # Step 2: Source B
        source_b = self._prompt_source("Source B")

        # Step 3: Key columns
        key_config = self._prompt_key_column(
            source_a["columns"],
            source_b["columns"],
        )

        # Step 4: Column mappings
        column_mappings = self._prompt_column_mappings(
            source_a["columns"],
            source_b["columns"],
        )

        # Step 5: Filters
        filters = self._prompt_filters(
            source_a["columns"],
            source_b["columns"],
        )

        # Step 6: Normalization
        normalization = self._prompt_normalization(
            source_a["columns"] + source_b["columns"],
        )

        # Step 7: Output formats
        output_formats = self._prompt_output_formats()

        # Step 8: Generate YAML
        yaml_str = self._generate_yaml(
            sources={"a": source_a, "b": source_b},
            key=key_config,
            mappings=column_mappings,
            filters=filters,
            normalization=normalization,
            output=output_formats,
        )

        # Save to config.yaml
        try:
            config_path = "config.yaml"
            with open(config_path, "w", encoding="utf-8") as fh:
                fh.write(yaml_str)
            print()
            print(f"  Configuration saved to: {config_path}")
            print()
        except OSError as exc:
            print(f"  Warning: Could not write config.yaml: {exc}")
            print("  Showing YAML output instead:")
            print()
            print(yaml_str)

        return yaml_str

    # ------------------------------------------------------------------
    # Source prompt
    # ------------------------------------------------------------------

    def _prompt_source(self, label: str) -> dict:
        """Prompt for a source file path, delimiter, and encoding.

        Parameters
        ----------
        label:
            Human-readable label (e.g. ``"Source A"``).

        Returns
        -------
        dict
            ``{"path": str, "delimiter": str, "encoding": str, "columns": list[str]}``
        """
        print(f"\n--- {label} ---")
        print()

        while True:
            file_path = _prompt(f"  CSV file path for {label}").strip()
            if not file_path:
                print("    Error: File path is required.")
                continue

            if not os.path.isfile(file_path):
                print(f"    Error: File not found: {file_path}")
                continue

            # Success
            break

        delimiter = _prompt(
            f"  Delimiter for {label}",
            default=",",
            completer=WordCompleter([",", ";", "\t", "|"], ignore_case=True) if _HAS_PROMPT_TOOLKIT else None,
        )

        encoding = _prompt(
            f"  File encoding for {label}",
            default="utf-8",
            completer=WordCompleter(["utf-8", "latin-1", "cp1252", "ascii", "utf-16"], ignore_case=True) if _HAS_PROMPT_TOOLKIT else None,
        )

        # Read columns
        try:
            columns = _read_csv_columns(file_path, delimiter=delimiter, encoding=encoding)
            print(f"    Detected {len(columns)} column(s): {', '.join(columns)}")
        except (FileNotFoundError, ValueError) as exc:
            print(f"    Warning: Could not read columns: {exc}")
            columns = []

        return {
            "path": file_path,
            "delimiter": delimiter,
            "encoding": encoding,
            "columns": columns,
        }

    # ------------------------------------------------------------------
    # Key column prompt
    # ------------------------------------------------------------------

    def _prompt_key_column(
        self,
        columns_a: List[str],
        columns_b: List[str],
    ) -> dict:
        """Prompt for the key column used to join the two sources.

        Parameters
        ----------
        columns_a:
            Column names in source A.
        columns_b:
            Column names in source B.

        Returns
        -------
        dict
            ``{"source_a": str, "source_b": str}``
        """
        print("\n--- Key Column ---")
        print()

        completer_a = WordCompleter(columns_a, ignore_case=True) if columns_a and _HAS_PROMPT_TOOLKIT else None
        completer_b = WordCompleter(columns_b, ignore_case=True) if columns_b and _HAS_PROMPT_TOOLKIT else None

        key_a = _prompt(
            "  Key column in Source A",
            default=columns_a[0] if columns_a else "",
            completer=completer_a,
        )

        # Intelligent default: try to find key_a in source B
        default_b = ""
        if key_a in columns_b:
            default_b = key_a
        elif columns_b:
            default_b = columns_b[0]

        key_b = _prompt(
            "  Key column in Source B",
            default=default_b,
            completer=completer_b,
        )

        return {
            "source_a": key_a,
            "source_b": key_b,
        }

    # ------------------------------------------------------------------
    # Column mappings prompt
    # ------------------------------------------------------------------

    def _prompt_column_mappings(
        self,
        columns_a: List[str],
        columns_b: List[str],
    ) -> list:
        """Prompt for which columns to compare and the comparison method.

        Parameters
        ----------
        columns_a:
            Column names in source A.
        columns_b:
            Column names in source B.

        Returns
        -------
        list[dict]
            List of column mapping dicts with ``source_a``, ``source_b``,
            and ``method`` keys.
        """
        print("\n--- Column Mappings ---")
        print("  Define which columns to compare between the two sources.")
        print("  Leave blank to finish adding mappings.")
        print()

        mappings: List[Dict[str, str]] = []

        completer_a = WordCompleter(columns_a, ignore_case=True) if columns_a and _HAS_PROMPT_TOOLKIT else None
        completer_b = WordCompleter(columns_b, ignore_case=True) if columns_b and _HAS_PROMPT_TOOLKIT else None
        method_completer = WordCompleter(["exact", "set"], ignore_case=True) if _HAS_PROMPT_TOOLKIT else None

        # Auto-suggest matching column names
        common = sorted(set(columns_a) & set(columns_b))
        if common:
            print(f"  Common column(s) found: {', '.join(common)}")

        index = 1
        while True:
            col_a = _prompt(
                f"  Mapping #{index} — Source A column",
                completer=completer_a,
            ).strip()
            if not col_a:
                break

            col_b = _prompt(
                f"  Mapping #{index} — Source B column",
                default=col_a if col_a in columns_b else "",
                completer=completer_b,
            ).strip()
            if not col_b:
                print("    Skipping — Source B column is required.")
                continue

            method = _prompt(
                f"  Mapping #{index} — Comparison method",
                default="exact",
                completer=method_completer,
            ).strip().lower()

            if method not in ("exact", "set"):
                print(f"    Warning: Unknown method '{method}', using 'exact'.")
                method = "exact"

            mappings.append({
                "source_a": col_a,
                "source_b": col_b,
                "method": method,
            })

            print(f"    Added: {col_a} <-> {col_b} ({method})")
            index += 1

        print(f"\n  Total: {len(mappings)} column mapping(s) configured.")
        return mappings

    # ------------------------------------------------------------------
    # Filters prompt
    # ------------------------------------------------------------------

    def _prompt_filters(
        self,
        columns_a: List[str],
        columns_b: List[str],
    ) -> dict:
        """Prompt for optional filter rules.

        Parameters
        ----------
        columns_a:
            Column names in source A.
        columns_b:
            Column names in source B.

        Returns
        -------
        dict
            Filter configuration dict (empty if no filters configured).
        """
        print("\n--- Filters (Optional) ---")

        if not _prompt_yes_no("  Add filter rules?", default=False):
            return {}

        print()

        all_columns = sorted(set(columns_a + columns_b))
        col_completer = WordCompleter(all_columns, ignore_case=True) if all_columns and _HAS_PROMPT_TOOLKIT else None
        op_completer = WordCompleter([
            "equals", "not_equals", "in", "not_in",
            "greater_than", "less_than", "contains",
            "matches_regex", "is_null", "is_not_null",
        ], ignore_case=True) if _HAS_PROMPT_TOOLKIT else None
        action_completer = WordCompleter(["skip", "keep"], ignore_case=True) if _HAS_PROMPT_TOOLKIT else None

        pre_filters: Dict[str, List[Dict[str, Any]]] = {"source_a": [], "source_b": []}

        for source_key, label in [("source_a", "Source A"), ("source_b", "Source B")]:
            print(f"  --- Filters for {label} ---")
            rule_idx = 1

            while True:
                if not _prompt_yes_no(f"    Add rule #{rule_idx} for {label}?", default=False):
                    break

                column = _prompt(
                    f"    Rule #{rule_idx} — Column",
                    completer=col_completer,
                ).strip()

                operator = _prompt(
                    f"    Rule #{rule_idx} — Operator",
                    default="equals",
                    completer=op_completer,
                ).strip().lower()

                action = _prompt(
                    f"    Rule #{rule_idx} — Action (skip or keep)",
                    default="skip",
                    completer=action_completer,
                ).strip().lower()

                rule: Dict[str, Any] = {
                    "column": column,
                    "operator": operator,
                    "action": action,
                }

                # Value (not required for is_null / is_not_null)
                if operator not in ("is_null", "is_not_null"):
                    if operator in ("in", "not_in"):
                        value_str = _prompt(f"    Rule #{rule_idx} — Values (comma-separated)").strip()
                        rule["value"] = [v.strip() for v in value_str.split(",") if v.strip()]
                    else:
                        rule["value"] = _prompt(f"    Rule #{rule_idx} — Value").strip()

                pre_filters[source_key].append(rule)
                print(f"    Added: {column} {operator} {rule.get('value', '')} [{action}]")
                rule_idx += 1

        # Build the filter config dict
        filter_config: Dict[str, Any] = {}
        has_rules = any(len(rules) > 0 for rules in pre_filters.values())
        if has_rules:
            filter_config["pre_filters"] = pre_filters

        total_rules = sum(len(r) for r in pre_filters.values())
        print(f"\n  Total: {total_rules} filter rule(s) configured.")
        return filter_config

    # ------------------------------------------------------------------
    # Normalization prompt
    # ------------------------------------------------------------------

    def _prompt_normalization(self, columns: List[str]) -> dict:
        """Prompt for optional normalization configuration.

        Parameters
        ----------
        columns:
            Combined column names from both sources.

        Returns
        -------
        dict
            Normalization configuration dict (empty if none configured).
        """
        print("\n--- Normalization (Optional) ---")

        if not _prompt_yes_no("  Configure normalization rules?", default=False):
            return {}

        print()

        norm_config: Dict[str, Any] = {"normalization_rules": {}}
        rules: Dict[str, Any] = {}

        # Global cleaning
        print("  --- Global Cleaning ---")
        trim = _prompt_yes_no("    Trim whitespace?", default=True)
        remove_nulls = _prompt_yes_no("    Remove null values?", default=True)
        case_mode = _prompt(
            "    Case normalization",
            default="none",
            completer=WordCompleter(["none", "lower", "upper", "title"], ignore_case=True) if _HAS_PROMPT_TOOLKIT else None,
        ).strip().lower()

        if trim or remove_nulls or (case_mode and case_mode != "none"):
            rules["global_cleaning"] = {
                "trim_whitespace": trim,
                "remove_nulls": remove_nulls,
                "case_normalize": case_mode if case_mode != "none" else None,
            }

        # Alias groups
        print()
        print("  --- Alias Groups ---")
        alias_groups: List[Dict[str, Any]] = []
        group_idx = 1

        while True:
            if not _prompt_yes_no(f"    Add alias group #{group_idx}?", default=False):
                break

            normalized_name = _prompt(f"    Group #{group_idx} — Normalized name").strip()
            if not normalized_name:
                print("      Skipping — normalized name is required.")
                continue

            aliases_str = _prompt(f"    Group #{group_idx} — Aliases (comma-separated)").strip()
            aliases = [a.strip() for a in aliases_str.split(",") if a.strip()]

            if not aliases:
                print("      Skipping — at least one alias is required.")
                continue

            display_format = _prompt(
                f"    Group #{group_idx} — Display format",
                default="{normalized}({original})",
            ).strip()

            group: Dict[str, Any] = {
                "normalized_name": normalized_name,
                "aliases": aliases,
                "display_format": display_format,
            }

            col_completer = WordCompleter(columns, ignore_case=True) if columns and _HAS_PROMPT_TOOLKIT else None
            apply_to_all = _prompt_yes_no(f"    Apply to all columns?", default=True)
            if not apply_to_all:
                cols_str = _prompt(f"    Group #{group_idx} — Columns (comma-separated)", completer=col_completer).strip()
                group["columns"] = [c.strip() for c in cols_str.split(",") if c.strip()]

            alias_groups.append(group)
            print(f"    Added: '{normalized_name}' <- {', '.join(aliases)}")
            group_idx += 1

        if alias_groups:
            rules["alias_groups"] = alias_groups

        if rules:
            norm_config["normalization_rules"] = rules

        items: List[str] = []
        if alias_groups:
            items.append(f"{len(alias_groups)} alias group(s)")
        if "global_cleaning" in rules:
            items.append("global cleaning")
        print(f"\n  Total: {', '.join(items) if items else 'none'} configured.")
        return norm_config

    # ------------------------------------------------------------------
    # Output format prompt
    # ------------------------------------------------------------------

    def _prompt_output_formats(self) -> list:
        """Prompt for output format preferences.

        Returns
        -------
        list[str]
            List of selected format names.
        """
        print("\n--- Output Formats ---")

        all_formats = ["html", "excel", "csv", "markdown"]
        format_descriptions = {
            "html": "Self-contained HTML report (default)",
            "excel": "Multi-sheet Excel workbook (.xlsx)",
            "csv": "Plain CSV files (one per section)",
            "markdown": "Markdown report (.md)",
        }

        selected: List[str] = []

        for fmt in all_formats:
            desc = format_descriptions.get(fmt, "")
            if _prompt_yes_no(f"  Generate {fmt} output? ({desc})", default=(fmt == "html")):
                selected.append(fmt)

        if not selected:
            selected = ["html"]

        print(f"\n  Selected: {', '.join(selected)}")
        return selected

    # ------------------------------------------------------------------
    # YAML generation
    # ------------------------------------------------------------------

    def _generate_yaml(
        self,
        sources: Dict[str, dict],
        key: Dict[str, str],
        mappings: List[Dict[str, str]],
        filters: Dict[str, Any],
        normalization: Dict[str, Any],
        output: List[str],
    ) -> str:
        """Generate a complete YAML configuration string.

        Parameters
        ----------
        sources:
            ``{"a": {path, delimiter, encoding, columns}, "b": ...}``
        key:
            ``{"source_a": str, "source_b": str}``
        mappings:
            List of ``{source_a, source_b, method}`` dicts.
        filters:
            Filter config dict (may be empty).
        normalization:
            Normalization config dict (may be empty).
        output:
            List of format names (e.g. ``["html", "excel"]``).

        Returns
        -------
        str
            A complete, valid YAML configuration string.
        """
        lines: List[str] = []

        lines.append(f"schema_version: \"{self.VERSION}\"")
        lines.append("")

        # Sources
        lines.append("sources:")
        for label in ("a", "b"):
            src = sources[label]
            lines.append(f"  {label}:")
            lines.append(f"    path: \"{src['path']}\"")
            if src.get("delimiter") and src["delimiter"] != ",":
                escaped_delim = src["delimiter"].replace("\t", "\\t")
                lines.append(f"    delimiter: \"{escaped_delim}\"")
            if src.get("encoding") and src["encoding"] != "utf-8":
                lines.append(f"    encoding: \"{src['encoding']}\"")
        lines.append("")

        # Compare
        lines.append("compare:")
        lines.append(f"  key_column:")
        lines.append(f"    source_a: \"{key['source_a']}\"")
        lines.append(f"    source_b: \"{key['source_b']}\"")
        # Include list-style key_columns for backward-compatible validation (CFG-010)
        lines.append(f"  key_columns:")
        lines.append(f"    - \"{key['source_a']}\"")
        lines.append(f"  join_type: inner")
        if mappings:
            lines.append(f"  column_mappings:")
            for m in mappings:
                lines.append(f"    - source_a: \"{m['source_a']}\"")
                lines.append(f"      source_b: \"{m['source_b']}\"")
                lines.append(f"      method: \"{m.get('method', 'exact')}\"")
        lines.append("")

        # Filter
        if filters:
            lines.append("filter:")
            if "pre_filters" in filters:
                lines.append("  pre_filters:")
                for source_key, rules in filters["pre_filters"].items():
                    if not rules:
                        continue
                    lines.append(f"    {source_key}:")
                    for rule in rules:
                        lines.append(f"      - column: \"{rule.get('column', '')}\"")
                        lines.append(f"        operator: \"{rule.get('operator', 'equals')}\"")
                        lines.append(f"        action: \"{rule.get('action', 'skip')}\"")
                        if "value" in rule:
                            val = rule["value"]
                            if isinstance(val, list):
                                lines.append(f"        value:")
                                for v in val:
                                    lines.append(f"          - \"{v}\"")
                            else:
                                lines.append(f"        value: \"{val}\"")
            lines.append("")

        # Normalize
        if normalization and normalization.get("normalization_rules"):
            lines.append("normalize:")
            rules = normalization["normalization_rules"]

            # Global cleaning
            gc = rules.get("global_cleaning", {})
            if gc:
                lines.append("  normalization_rules:")
                lines.append("    global_cleaning:")
                lines.append(f"      trim_whitespace: {str(gc.get('trim_whitespace', False)).lower()}")
                lines.append(f"      remove_nulls: {str(gc.get('remove_nulls', False)).lower()}")
                case = gc.get("case_normalize")
                if case and case != "none":
                    lines.append(f'      case_normalize: "{case}"')
            else:
                lines.append("  normalization_rules:")

            # Alias groups
            ag = rules.get("alias_groups", [])
            if ag:
                if not gc:
                    lines.append("  normalization_rules:")
                lines.append("    alias_groups:")
                for group in ag:
                    lines.append(f"      - normalized_name: \"{group.get('normalized_name', '')}\"")
                    lines.append(f"        aliases:")
                    for alias in group.get("aliases", []):
                        lines.append(f'          - "{alias}"')
                    fmt = group.get("display_format", "{normalized}")
                    # Escape braces for YAML
                    lines.append(f"        display_format: \"{fmt}\"")
                    if "columns" in group:
                        lines.append(f"        columns:")
                        for col in group["columns"]:
                            lines.append(f'          - "{col}"')
            lines.append("")

        # Output
        lines.append("output:")
        lines.append("  dir: \"output\"")
        lines.append("")

        lines.append("report:")
        lines.append("  title: \"Data Comparison Report\"")
        lines.append("  output:")
        lines.append("    formats:")

        format_file_map = {
            "html": "comparison_report.html",
            "excel": "comparison_report.xlsx",
            "csv": "comparison_report",
            "markdown": "comparison_report.md",
        }
        for fmt in output:
            file_name = format_file_map.get(fmt, f"comparison_report.{fmt}")
            lines.append(f"      - type: \"{fmt}\"")
            lines.append(f"        file_name: \"{file_name}\"")

        lines.append("  sections:")
        for section in [
            "summary", "mismatch_details", "unmatched_records",
            "per_column_breakdown", "normalization_log", "filter_log",
        ]:
            lines.append(f"    - name: \"{section}\"")
            lines.append("      enabled: true")

        lines.append("")

        return "\n".join(lines)
