"""
Display module – rich-based terminal output for the CSV Compare Pipeline.

Provides the :class:`Display` facade that wraps the rich library to produce
styled console output including banners, tables, progress indicators,
and colour-coded messages.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TaskProgressColumn, TimeRemainingColumn
from rich.table import Table
from rich.text import Text

# ---------------------------------------------------------------------------
# Colour constants
# ---------------------------------------------------------------------------

_COLORS = {
    "error": "red",
    "warning": "yellow",
    "success": "green",
    "info": "blue",
    "header": "cyan",
    "muted": "dim",
    "bold": "bold",
}

_VERSION = "1.0.0"


# ---------------------------------------------------------------------------
# Display class
# ---------------------------------------------------------------------------

class Display:
    """Facade over the rich console for all pipeline terminal output.

    Every public method writes to a shared :class:`rich.console.Console`
    instance.  The class is intentionally stateless between calls so it
    can be freely instantiated or reused.
    """

    def __init__(self, console: Optional[Console] = None) -> None:
        self._console: Console = console or Console()

    # ------------------------------------------------------------------
    # Header / Banner
    # ------------------------------------------------------------------

    def print_header(self, title: str, version: str = _VERSION) -> None:
        """Print a styled pipeline banner using a rich Panel.

        Parameters
        ----------
        title:
            The main title to display (e.g. ``"CSV Compare Pipeline"``).
        version:
            Version string appended to the banner.
        """
        banner_text = Text()
        banner_text.append(title, style="bold white")
        banner_text.append(f"\n", style="")
        banner_text.append(f"Version {version}", style=_COLORS["muted"])

        panel = Panel(
            banner_text,
            border_style=_COLORS["header"],
            padding=(1, 4),
            expand=False,
        )
        self._console.print()
        self._console.print(panel)
        self._console.print()

    # ------------------------------------------------------------------
    # Configuration summary
    # ------------------------------------------------------------------

    def print_config_summary(self, config: dict) -> None:
        """Render a formatted configuration summary.

        Displays sources, key columns, column mappings, filters, and
        output settings in a compact table.

        Parameters
        ----------
        config:
            The fully loaded pipeline configuration dictionary.
        """
        table = Table(
            title="Configuration Summary",
            title_style=_COLORS["header"],
            show_header=False,
            border_style=_COLORS["header"],
            expand=False,
            padding=(0, 2),
        )
        table.add_column("Key", style="bold cyan", min_width=22)
        table.add_column("Value", min_width=40)

        # Sources
        sources = config.get("sources", {})
        table.add_row("Sources", "")
        for label in ("a", "b"):
            src = sources.get(label, {})
            path = src.get("path", "<not set>")
            delimiter = src.get("delimiter", ",")
            encoding = src.get("encoding", "auto-detect")
            table.add_row(f"  Source {label.upper()}", f"{path}  (delim={delimiter}, enc={encoding})")

        # Compare / key columns
        compare = config.get("compare", {})
        key_column = compare.get("key_column", {})
        key_a = key_column.get("source_a", compare.get("key_columns", ["?"])[0] if compare.get("key_columns") else "?")
        key_b = key_column.get("source_b", key_a)
        table.add_row("Key Column", f"Source A: {key_a}  |  Source B: {key_b}")

        join_type = compare.get("join_type", "inner")
        table.add_row("Join Type", str(join_type))

        # Column mappings
        mappings = compare.get("column_mappings", [])
        table.add_row("Column Mappings", f"{len(mappings)} mapping(s)")
        for m in mappings:
            col_a = m.get("source_a", "?")
            col_b = m.get("source_b", "?")
            method = m.get("method", "exact")
            table.add_row("", f"  {col_a} <-> {col_b}  ({method})")

        # Filter
        filter_cfg = config.get("filter", {})
        pre_filters = filter_cfg.get("pre_filters", {})
        total_rules = sum(len(rules) for rules in pre_filters.values()) if isinstance(pre_filters, dict) else 0
        if total_rules > 0:
            table.add_row("Filters", f"{total_rules} rule(s)")
            for source_key, rules in pre_filters.items():
                for r in rules:
                    col = r.get("column", "?")
                    op = r.get("operator", "?")
                    action = r.get("action", "?")
                    val = r.get("value", "")
                    val_str = f" = {val}" if val else ""
                    table.add_row("", f"  {source_key}.{col} {op}{val_str} [{action}]")
        else:
            table.add_row("Filters", "None")

        # Normalize
        norm_cfg = config.get("normalize", {})
        norm_rules = norm_cfg.get("normalization_rules", {})
        alias_groups = norm_rules.get("alias_groups", [])
        global_cleaning = norm_rules.get("global_cleaning", {})
        if alias_groups or global_cleaning:
            items: List[str] = []
            if alias_groups:
                items.append(f"{len(alias_groups)} alias group(s)")
            if global_cleaning:
                items.append("global cleaning enabled")
            table.add_row("Normalization", ", ".join(items))
        else:
            table.add_row("Normalization", "None")

        # Output
        report_cfg = config.get("report", {})
        output_cfg = report_cfg.get("output", {})
        formats = output_cfg.get("formats", [])
        format_types = [f.get("type", "?") for f in formats]
        table.add_row("Output Formats", ", ".join(format_types) if format_types else "html (default)")

        # Schema version
        table.add_row("Schema Version", str(config.get("schema_version", "unknown")))

        self._console.print(table)
        self._console.print()

    # ------------------------------------------------------------------
    # Stage progress
    # ------------------------------------------------------------------

    def print_stage_progress(
        self,
        stage_name: str,
        status: str,
        detail: str = "",
    ) -> None:
        """Print a styled single-line stage progress message.

        Parameters
        ----------
        stage_name:
            Human-readable stage name (e.g. ``"Import"``).
        status:
            One of ``"running"``, ``"done"``, ``"skipped"``, ``"error"``.
        detail:
            Optional supplementary text.
        """
        status_styles: Dict[str, str] = {
            "running": _COLORS["info"],
            "done": _COLORS["success"],
            "skipped": _COLORS["muted"],
            "error": _COLORS["error"],
            "warning": _COLORS["warning"],
        }

        status_symbols: Dict[str, str] = {
            "running": ">>",
            "done": "OK",
            "skipped": "--",
            "error": "!!",
            "warning": "??",
        }

        color = status_styles.get(status, "white")
        symbol = status_symbols.get(status, "??")

        text = Text()
        text.append(f"  [{symbol}] ", style=color)
        text.append(f"{stage_name}: ", style="bold white")
        text.append(status.capitalize(), style=color)
        if detail:
            text.append(f"  — {detail}", style=_COLORS["muted"])

        self._console.print(text)

    # ------------------------------------------------------------------
    # Progress bar
    # ------------------------------------------------------------------

    def print_progress_bar(
        self,
        current: int,
        total: int,
        description: str = "",
    ) -> None:
        """Print an animated progress bar for the given completion state.

        Parameters
        ----------
        current:
            Current item count.
        total:
            Total item count.
        description:
            Optional label shown to the left of the bar.
        """
        completed = min(current, total)
        with Progress(
            TextColumn("[bold]{task.description}"),
            BarColumn(bar_width=40),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=self._console,
            transient=False,
        ) as progress:
            task_id = progress.add_task(description or "Progress", total=total)
            progress.update(task_id, completed=completed)

    # ------------------------------------------------------------------
    # Results table
    # ------------------------------------------------------------------

    def print_results_table(self, stats: dict) -> None:
        """Print a formatted summary table of pipeline results.

        Parameters
        ----------
        stats:
            Dictionary containing pipeline statistics.  Expected keys
            (all optional with sensible defaults):

            * ``total_rows_a``, ``total_rows_b``
            * ``matched_keys``, ``only_in_a``, ``only_in_b``
            * ``full_match_count``, ``mismatch_count``
            * ``match_rate_percent``
            * ``total_mismatches``
            * ``per_column_stats`` – dict of ``{column: {matched, mismatched, rate_percent}}``
            * ``elapsed_seconds``
        """
        # --- Overview table ---
        overview = Table(
            title="Results Overview",
            title_style=_COLORS["header"],
            border_style=_COLORS["header"],
            show_lines=True,
        )
        overview.add_column("Metric", style="bold white", min_width=30)
        overview.add_column("Value", justify="right", min_width=16)

        rows_a = stats.get("total_rows_a", stats.get("import_source_a", {}).get("rows", "N/A"))
        rows_b = stats.get("total_rows_b", stats.get("import_source_b", {}).get("rows", "N/A"))
        matched = stats.get("matched_keys", "N/A")
        only_a = stats.get("only_in_a", "N/A")
        only_b = stats.get("only_in_b", "N/A")
        full_match = stats.get("full_match_count", "N/A")
        mismatch_count = stats.get("mismatch_count", "N/A")
        rate = stats.get("match_rate_percent", "N/A")
        total_mismatches = stats.get("total_mismatches", "N/A")
        elapsed = stats.get("elapsed_seconds", stats.get("pipeline_elapsed", "N/A"))

        def _style_rate(val: Any) -> str:
            try:
                v = float(val)
                if v >= 90:
                    return f"[{_COLORS['success']}]{v}%[/]"
                elif v >= 70:
                    return f"[{_COLORS['warning']}]{v}%[/]"
                else:
                    return f"[{_COLORS['error']}]{v}%[/]"
            except (ValueError, TypeError):
                return str(val)

        overview.add_row("Total Rows (Source A)", str(rows_a))
        overview.add_row("Total Rows (Source B)", str(rows_b))
        overview.add_row("Matched Keys", str(matched))
        overview.add_row("Only in Source A", str(only_a))
        overview.add_row("Only in Source B", str(only_b))
        overview.add_row("Full Match Keys", str(full_match))
        overview.add_row("Keys with Mismatches", str(mismatch_count))
        overview.add_row("Match Rate", _style_rate(rate))
        overview.add_row("Total Cell Mismatches", str(total_mismatches))
        overview.add_row("Elapsed Time", f"{elapsed}s" if isinstance(elapsed, (int, float)) else str(elapsed))

        self._console.print(overview)
        self._console.print()

        # --- Per-column breakdown ---
        per_column = stats.get("per_column_stats", {})
        if per_column and isinstance(per_column, dict):
            col_table = Table(
                title="Per-Column Breakdown",
                title_style=_COLORS["header"],
                border_style=_COLORS["header"],
                show_lines=True,
            )
            col_table.add_column("Column", style="bold white", min_width=28)
            col_table.add_column("Matched", justify="right")
            col_table.add_column("Mismatched", justify="right", style=_COLORS["error"])
            col_table.add_column("Total", justify="right")
            col_table.add_column("Match Rate", justify="right")

            for col_name, col_data in per_column.items():
                if not isinstance(col_data, dict):
                    continue
                m = col_data.get("matched", 0)
                mm = col_data.get("mismatched", 0)
                total = m + mm
                rate = col_data.get("rate_percent", 0.0)
                col_table.add_row(
                    col_name,
                    str(m),
                    str(mm),
                    str(total),
                    _style_rate(rate),
                )

            self._console.print(col_table)
            self._console.print()

    # ------------------------------------------------------------------
    # Mismatch summary
    # ------------------------------------------------------------------

    def print_mismatch_summary(self, comparison_result: dict) -> None:
        """Print a summary of mismatches from the comparison result.

        Parameters
        ----------
        comparison_result:
            The ``comparison_result`` dict from :class:`PipelineContext`.
        """
        if not comparison_result:
            self.print_warning("No comparison result available.")
            return

        details = comparison_result.get("details", [])
        mismatches = [d for d in details if d.get("status") == "mismatch"]

        if not mismatches:
            self.print_success("No mismatches found — all compared keys match perfectly.")
            return

        # Group mismatches by column
        by_column: Dict[str, int] = {}
        for m in mismatches:
            col = m.get("column", "<unknown>")
            by_column[col] = by_column.get(col, 0) + 1

        table = Table(
            title=f"Mismatch Summary ({len(mismatches)} cell-level mismatch(es))",
            title_style=_COLORS["error"],
            border_style=_COLORS["error"],
            show_lines=True,
        )
        table.add_column("Column", style="bold white", min_width=28)
        table.add_column("Mismatch Count", justify="right", style=_COLORS["error"])

        for col_name, count in sorted(by_column.items(), key=lambda x: -x[1]):
            table.add_row(col_name, str(count))

        self._console.print(table)
        self._console.print()

    # ------------------------------------------------------------------
    # Filter summary
    # ------------------------------------------------------------------

    def print_filter_summary(self, filter_log: list) -> None:
        """Print a summary of filter impacts.

        Parameters
        ----------
        filter_log:
            List of log entry dicts from :attr:`PipelineContext.filter_log`.
        """
        if not filter_log:
            self.print_success("No filter rules applied.")
            return

        table = Table(
            title="Filter Impact Summary",
            title_style=_COLORS["header"],
            border_style=_COLORS["header"],
            show_lines=True,
        )
        table.add_column("Source", style="bold white", min_width=14)
        table.add_column("Action", min_width=20)
        table.add_column("Column", min_width=14)
        table.add_column("Before", justify="right")
        table.add_column("After", justify="right")
        table.add_column("Affected", justify="right", style=_COLORS["warning"])
        table.add_column("Status", min_width=10)

        for entry in filter_log:
            source = str(entry.get("source", ""))
            action = str(entry.get("action", ""))
            column = str(entry.get("column", ""))
            before = str(entry.get("rows_before", ""))
            after = str(entry.get("rows_after", ""))
            affected = str(entry.get("rows_affected", ""))
            status = str(entry.get("status", ""))

            status_style = _COLORS["muted"]
            if status == "error":
                status_style = _COLORS["error"]
            elif status == "warning":
                status_style = _COLORS["warning"]
            elif status == "applied":
                status_style = _COLORS["success"]

            table.add_row(source, action, column, before, after, affected, Text(status, style=status_style))

        self._console.print(table)
        self._console.print()

    # ------------------------------------------------------------------
    # Normalization summary
    # ------------------------------------------------------------------

    def print_normalization_summary(self, norm_log: list) -> None:
        """Print a summary of normalization impacts.

        Parameters
        ----------
        norm_log:
            List of log entry dicts from :attr:`PipelineContext.normalization_log`.
        """
        if not norm_log:
            self.print_success("No normalization rules applied.")
            return

        table = Table(
            title="Normalization Impact Summary",
            title_style=_COLORS["header"],
            border_style=_COLORS["header"],
            show_lines=True,
        )
        table.add_column("Source", style="bold white", min_width=14)
        table.add_column("Action", min_width=24)
        table.add_column("Detail", min_width=30)
        table.add_column("Status", min_width=10)

        for entry in norm_log:
            source = str(entry.get("source", ""))
            action = str(entry.get("action", ""))
            status = str(entry.get("status", ""))

            # Build a detail string
            detail_parts: List[str] = []
            if "rows_affected" in entry:
                detail_parts.append(f"rows={entry['rows_affected']}")
            if "groups_applied" in entry:
                detail_parts.append(f"groups={entry['groups_applied']}")
            if "cells_transformed" in entry:
                detail_parts.append(f"cells={entry['cells_transformed']}")
            if "column" in entry:
                detail_parts.append(f"col={entry['column']}")
            if "rules_applied" in entry:
                detail_parts.append(f"rules={entry['rules_applied']}")
            if "message" in entry:
                detail_parts.append(str(entry["message"]))
            detail = " | ".join(detail_parts) if detail_parts else ""

            status_style = _COLORS["muted"]
            if status == "error":
                status_style = _COLORS["error"]
            elif status == "warning":
                status_style = _COLORS["warning"]
            elif status == "applied":
                status_style = _COLORS["success"]

            table.add_row(source, action, detail, Text(status, style=status_style))

        self._console.print(table)
        self._console.print()

    # ------------------------------------------------------------------
    # Colour-coded messages
    # ------------------------------------------------------------------

    def print_error(self, message: str, details: Optional[dict] = None) -> None:
        """Print a styled error message in red.

        Parameters
        ----------
        message:
            Primary error text.
        details:
            Optional dict of key-value pairs appended as detail lines.
        """
        self._console.print()
        text = Text()
        text.append("  [ERROR] ", style=f"bold {_COLORS['error']}")
        text.append(message, style=_COLORS["error"])
        self._console.print(text)

        if details:
            for key, value in details.items():
                detail_text = Text()
                detail_text.append(f"         {key}: ", style="bold")
                detail_text.append(str(value), style=_COLORS["error"])
                self._console.print(detail_text)
        self._console.print()

    def print_warning(self, message: str) -> None:
        """Print a styled warning message in yellow.

        Parameters
        ----------
        message:
            Warning text.
        """
        text = Text()
        text.append("  [WARN]  ", style=f"bold {_COLORS['warning']}")
        text.append(message, style=_COLORS["warning"])
        self._console.print(text)

    def print_success(self, message: str) -> None:
        """Print a styled success message in green.

        Parameters
        ----------
        message:
            Success text.
        """
        text = Text()
        text.append("  [OK]    ", style=f"bold {_COLORS['success']}")
        text.append(message, style=_COLORS["success"])
        self._console.print(text)

    def print_info(self, message: str) -> None:
        """Print a styled informational message in blue.

        Parameters
        ----------
        message:
            Info text.
        """
        text = Text()
        text.append("  [INFO]  ", style=f"bold {_COLORS['info']}")
        text.append(message, style=_COLORS["info"])
        self._console.print(text)

    # ------------------------------------------------------------------
    # Output files list
    # ------------------------------------------------------------------

    def print_output_files(self, file_paths: list) -> None:
        """Print a list of generated output file paths.

        Parameters
        ----------
        file_paths:
            List of file path strings.
        """
        if not file_paths:
            self.print_warning("No output files were generated.")
            return

        self._console.print()
        header_text = Text()
        header_text.append("  Generated Output Files", style=f"bold {_COLORS['header']}")
        self._console.print(header_text)

        for path in file_paths:
            if os.path.isdir(path):
                self._console.print(f"    [dir]  {path}", style=_COLORS["info"])
            elif os.path.exists(path):
                size = os.path.getsize(path)
                size_str = _human_readable_size(size)
                self._console.print(f"    [file] {path}  ({size_str})", style=_COLORS["success"])
            else:
                self._console.print(f"    [???]  {path}", style=_COLORS["warning"])

        self._console.print()

    # ------------------------------------------------------------------
    # Separator
    # ------------------------------------------------------------------

    def print_separator(self, char: str = "─", width: int = 72) -> None:
        """Print a visual separator line.

        Parameters
        ----------
        char:
            Character used for the line.
        width:
            Total width of the separator.
        """
        self._console.print(char * width, style=_COLORS["muted"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _human_readable_size(size_bytes: int) -> str:
    """Convert a byte count to a human-readable string (e.g. ``"2.4 MB"``)."""
    if size_bytes < 0:
        return "0 B"
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"
