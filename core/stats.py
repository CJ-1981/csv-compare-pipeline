"""
Statistics engine – 3-tier stat computation from pipeline context.

Computes summary and per-column statistics that feed into report
generation. Supports three tiers:

Tier 1 – Built-in stat library (pre-defined computations).
Tier 2 – Inline expression evaluation (basic arithmetic referencing
         other stats).
Tier 3 – Plugin stats loaded from an external registry.

Rule catalogue
~~~~~~~~~~~~~~
STAT-001  Stats are computed only for enabled report sections.
STAT-002  Name conflicts across tiers are fatal errors.
STAT-003  Missing dependencies raise clear errors before computation.
STAT-004  Dependencies are resolved topologically (Kahn's algorithm).
STAT-005  Expression evaluation is sandboxed (no function calls,
          attribute access, or subscripts).
STAT-006  Stats are computed once and cached in context.stats.

Configuration (top-level key ``report``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: yaml

    report:
      sections:
        - name: "summary"
          enabled: true
          stats:
            - "total_rows_a"
            - "total_rows_b"
            - "matched_keys"
            - "match_rate_percent"
            - "mismatch_count"
        - name: "column_breakdown"
          enabled: true
          stats:
            - "per_column_stats"
            - "high_risk_mismatches"
        - name: "derived"
          enabled: true
          stats:
            - name: "alert_threshold"
              expression: "match_rate_percent < 80"
            - name: "efficiency_score"
              expression: "full_match_count * 100 / total_rows_a"
            - name: "custom_plugin_stat"
              plugin: "my_package.stats.CustomStat"
              depends_on: ["matched_keys"]
"""
from __future__ import annotations

import ast
import importlib
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .engine import ComparisonError, PipelineContext


# ---------------------------------------------------------------------------
# Helper: safe division
# ---------------------------------------------------------------------------

def _safe_divide(numerator: Any, denominator: Any) -> float:
    """Return *numerator / denominator*, or ``0.0`` on division by zero."""
    try:
        n = float(numerator) if numerator is not None else 0.0
        d = float(denominator) if denominator is not None else 0.0
        if d == 0:
            return 0.0
        return n / d
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Tier 1 compute functions
# ---------------------------------------------------------------------------

def _compute_total_rows_a(context: PipelineContext, _stats: Dict) -> int:
    """Total rows in source A (raw import count)."""
    return context.rows_source_a


def _compute_total_rows_b(context: PipelineContext, _stats: Dict) -> int:
    """Total rows in source B (raw import count)."""
    return context.rows_source_b


def _compute_rows_filtered_a(context: PipelineContext, _stats: Dict) -> int:
    """Rows remaining after filtering in source A."""
    return context.rows_filtered_a


def _compute_rows_filtered_b(context: PipelineContext, _stats: Dict) -> int:
    """Rows remaining after filtering in source B."""
    return context.rows_filtered_b


def _compute_matched_keys(context: PipelineContext, _stats: Dict) -> int:
    """Number of keys present in both sources."""
    result = context.comparison_result
    if result is None:
        return 0
    return len(result.get("matched_keys", []))


def _compute_only_in_a(context: PipelineContext, _stats: Dict) -> int:
    """Number of keys found only in source A."""
    result = context.comparison_result
    if result is None:
        return 0
    return len(result.get("only_in_a", []))


def _compute_only_in_b(context: PipelineContext, _stats: Dict) -> int:
    """Number of keys found only in source B."""
    result = context.comparison_result
    if result is None:
        return 0
    return len(result.get("only_in_b", []))


def _compute_total_compared_keys(context: PipelineContext, stats: Dict) -> int:
    """Total number of keys compared (delegates to matched_keys)."""
    return stats.get("matched_keys", 0)


def _compute_full_match_count(context: PipelineContext, _stats: Dict) -> int:
    """Number of keys where **all** mapped columns match."""
    result = context.comparison_result
    if result is None:
        return 0
    details: List[Dict] = result.get("details", [])
    if not details:
        return 0

    # Group comparison status by key
    key_statuses: Dict[str, bool] = {}
    for detail in details:
        key = detail.get("key", "")
        if key not in key_statuses:
            key_statuses[key] = True  # assume full match until a mismatch
        if detail.get("status") == "mismatch":
            key_statuses[key] = False

    return sum(1 for is_match in key_statuses.values() if is_match)


def _compute_mismatch_count(context: PipelineContext, stats: Dict) -> int:
    """Number of keys where **any** mapped column mismatches."""
    total_compared = stats.get("total_compared_keys", 0)
    full_match = stats.get("full_match_count", 0)
    return total_compared - full_match


def _compute_match_rate_percent(context: PipelineContext, stats: Dict) -> float:
    """Percentage of compared keys with full column match.

    Returns ``0.0`` when there are no compared keys (division by zero
    guard).
    """
    full_match = stats.get("full_match_count", 0)
    total_compared = stats.get("total_compared_keys", 0)
    rate = _safe_divide(full_match, total_compared) * 100
    return round(rate, 2)


def _compute_total_mismatches(context: PipelineContext, _stats: Dict) -> int:
    """Total number of individual cell-level mismatches across all keys."""
    result = context.comparison_result
    if result is None:
        return 0
    details: List[Dict] = result.get("details", [])
    return sum(1 for d in details if d.get("status") == "mismatch")


def _compute_per_column_stats(context: PipelineContext, _stats: Dict) -> Dict[str, Dict]:
    """Per-column matched / mismatched counts and match rate percent.

    Returns
    -------
    dict[str, dict]
        ``{column_label: {matched, mismatched, rate_percent}}``
    """
    result = context.comparison_result
    if result is None:
        return {}
    details: List[Dict] = result.get("details", [])
    if not details:
        return {}

    column_data: Dict[str, Dict[str, int]] = defaultdict(
        lambda: {"matched": 0, "mismatched": 0}
    )

    for detail in details:
        col = detail.get("column", "")
        if detail.get("status") == "match":
            column_data[col]["matched"] += 1
        else:
            column_data[col]["mismatched"] += 1

    per_column: Dict[str, Dict[str, Any]] = {}
    for col, counts in column_data.items():
        total = counts["matched"] + counts["mismatched"]
        rate = _safe_divide(counts["matched"], total) * 100
        per_column[col] = {
            "matched": counts["matched"],
            "mismatched": counts["mismatched"],
            "rate_percent": round(rate, 2),
        }

    return per_column


def _compute_high_risk_mismatches(context: PipelineContext, _stats: Dict) -> List[str]:
    """Keys with 3 or more column mismatches.

    Returns a sorted list of key values.
    """
    result = context.comparison_result
    if result is None:
        return []
    details: List[Dict] = result.get("details", [])
    if not details:
        return []

    # Count mismatches per key
    key_mismatch_counts: Dict[str, int] = defaultdict(int)
    for detail in details:
        if detail.get("status") == "mismatch":
            key_mismatch_counts[detail.get("key", "")] += 1

    # Keys with 3+ column mismatches
    high_risk = [
        key for key, count in key_mismatch_counts.items() if count >= 3
    ]
    return sorted(high_risk)


# ---------------------------------------------------------------------------
# Tier 1: Built-in stat library
# ---------------------------------------------------------------------------

BUILT_IN_STATS: Dict[str, Dict[str, Any]] = {
    "total_rows_a": {
        "description": "Total rows in source A (raw import count).",
        "compute_fn": _compute_total_rows_a,
        "depends_on": [],
    },
    "total_rows_b": {
        "description": "Total rows in source B (raw import count).",
        "compute_fn": _compute_total_rows_b,
        "depends_on": [],
    },
    "rows_filtered_a": {
        "description": "Rows remaining after filtering in source A.",
        "compute_fn": _compute_rows_filtered_a,
        "depends_on": [],
    },
    "rows_filtered_b": {
        "description": "Rows remaining after filtering in source B.",
        "compute_fn": _compute_rows_filtered_b,
        "depends_on": [],
    },
    "matched_keys": {
        "description": "Number of keys present in both sources.",
        "compute_fn": _compute_matched_keys,
        "depends_on": [],
    },
    "only_in_a": {
        "description": "Number of keys found only in source A.",
        "compute_fn": _compute_only_in_a,
        "depends_on": [],
    },
    "only_in_b": {
        "description": "Number of keys found only in source B.",
        "compute_fn": _compute_only_in_b,
        "depends_on": [],
    },
    "total_compared_keys": {
        "description": "Total number of keys compared (matched_keys count).",
        "compute_fn": _compute_total_compared_keys,
        "depends_on": ["matched_keys"],
    },
    "full_match_count": {
        "description": "Number of keys where ALL mapped columns match.",
        "compute_fn": _compute_full_match_count,
        "depends_on": [],
    },
    "mismatch_count": {
        "description": "Number of keys where ANY mapped column mismatches.",
        "compute_fn": _compute_mismatch_count,
        "depends_on": ["full_match_count", "total_compared_keys"],
    },
    "match_rate_percent": {
        "description": "Percentage of compared keys with full column match.",
        "compute_fn": _compute_match_rate_percent,
        "depends_on": ["full_match_count", "total_compared_keys"],
    },
    "total_mismatches": {
        "description": "Total number of individual cell-level mismatches.",
        "compute_fn": _compute_total_mismatches,
        "depends_on": [],
    },
    "per_column_stats": {
        "description": "Per-column match/mismatch counts and match rate.",
        "compute_fn": _compute_per_column_stats,
        "depends_on": [],
    },
    "high_risk_mismatches": {
        "description": "Keys with 3 or more column mismatches.",
        "compute_fn": _compute_high_risk_mismatches,
        "depends_on": [],
    },
}


# ---------------------------------------------------------------------------
# StatsEngine
# ---------------------------------------------------------------------------

class StatsEngine:
    """3-tier statistics computation engine.

    Computes requested statistics from the pipeline context, supporting:

    * **Tier 1** – Built-in stat library (:data:`BUILT_IN_STATS`).
    * **Tier 2** – Inline expression evaluation with safe arithmetic.
    * **Tier 3** – Plugin stats loaded from external classes.

    Usage::

        engine = StatsEngine()
        engine.register_plugin("my_stat", {
            "compute_fn": my_plugin.compute,
            "depends_on": ["matched_keys"],
            "description": "My custom stat",
        })
        stats = engine.compute_all(context, config)
    """

    def __init__(self) -> None:
        self._plugin_registry: Dict[str, Dict[str, Any]] = {}
        self._computed_cache: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Plugin registry
    # ------------------------------------------------------------------

    def register_plugin(self, name: str, plugin_def: Dict[str, Any]) -> None:
        """Register a plugin stat definition.

        Parameters
        ----------
        name:
            Unique stat name (must not clash with built-in or other
            plugins).
        plugin_def:
            Dict with required keys ``compute_fn`` (callable accepting
            ``context`` and ``stats`` dicts) and optional keys
            ``depends_on`` (list of stat names) and ``description``.
        """
        self._plugin_registry[name] = plugin_def

    def unregister_plugin(self, name: str) -> None:
        """Remove a plugin stat from the registry."""
        self._plugin_registry.pop(name, None)

    def reset_cache(self) -> None:
        """Clear the computed-stats cache (STAT-006)."""
        self._computed_cache.clear()

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def compute_all(
        self,
        context: PipelineContext,
        config: Dict,
    ) -> Dict[str, Any]:
        """Compute all requested statistics and cache in context.

        Parameters
        ----------
        context:
            Pipeline context (must contain comparison results and metadata).
        config:
            Full pipeline configuration dict. Stats are extracted from
            the ``report.sections[].stats`` entries.

        Returns
        -------
        dict[str, Any]
            All computed statistics keyed by stat name.

        Raises
        ------
        ComparisonError
            * STAT-002 – name conflict across tiers.
            * STAT-003 – missing or unsatisfied dependency.
            * STAT-004 – circular dependency.
            * STAT-005 – unsafe or invalid expression.
        """
        # STAT-001: collect from enabled sections only
        stat_definitions = self._collect_stat_definitions(config)

        if not stat_definitions:
            context.stats["stats_engine_status"] = "no_stats_requested"
            return {}

        # STAT-002: resolve names, detect conflicts
        resolved = self._resolve_stat_definitions(stat_definitions)

        # STAT-004: topological sort
        compute_order = self._topological_sort(resolved)

        # Compute each stat in dependency order
        computed: Dict[str, Any] = {}

        for stat_name in compute_order:
            # STAT-006: skip if already cached from a previous compute_all call
            if stat_name in self._computed_cache:
                computed[stat_name] = self._computed_cache[stat_name]
                continue

            stat_def = resolved[stat_name]
            tier = stat_def.get("tier", 1)

            # Verify dependencies are available
            for dep in stat_def.get("depends_on", []):
                if dep not in computed and dep not in self._computed_cache:
                    raise ComparisonError(
                        f"Unsatisfied dependency for stat '{stat_name}': "
                        f"'{dep}' has not been computed.",
                        {"rule": "STAT-003", "stat": stat_name, "dependency": dep},
                    )

            try:
                if tier == 2:
                    # Tier 2: expression evaluation (STAT-005)
                    value = self._evaluate_expression(
                        stat_def["expression"], computed
                    )
                elif tier == 3:
                    # Tier 3: plugin
                    compute_fn: Callable = stat_def["compute_fn"]
                    value = compute_fn(context, computed)
                else:
                    # Tier 1: built-in
                    compute_fn = stat_def["compute_fn"]
                    value = compute_fn(context, computed)

                computed[stat_name] = value
                self._computed_cache[stat_name] = value  # STAT-006: cache

            except ComparisonError:
                raise
            except Exception as exc:
                raise ComparisonError(
                    f"Error computing stat '{stat_name}': {exc}",
                    {"rule": "STAT-001", "stat": stat_name},
                ) from exc

        # STAT-006: persist in context.stats
        context.stats.update(computed)
        context.stats["stats_engine_status"] = "computed"

        return computed

    # ------------------------------------------------------------------
    # Stat definition collection (STAT-001)
    # ------------------------------------------------------------------

    def _collect_stat_definitions(
        self, config: Dict,
    ) -> List[Dict[str, Any]]:
        """Walk all enabled report sections and collect stat references.

        Each reference is either:
        * A string – references a built-in or registered plugin stat.
        * A dict – defines an inline expression (Tier 2) or plugin (Tier 3).
        """
        definitions: List[Dict[str, Any]] = []

        report_cfg = config.get("report", {})
        if not isinstance(report_cfg, dict):
            return definitions

        sections: List[Dict] = report_cfg.get("sections", [])
        if not isinstance(sections, list):
            return definitions

        for section in sections:
            if not isinstance(section, dict):
                continue
            if not section.get("enabled", True):
                continue  # STAT-001: skip disabled sections

            stats_list = section.get("stats", [])
            if not isinstance(stats_list, list):
                continue

            for stat_ref in stats_list:
                parsed = self._parse_stat_reference(stat_ref)
                if parsed is not None:
                    definitions.append(parsed)

        return definitions

    def _parse_stat_reference(self, stat_ref: Any) -> Optional[Dict[str, Any]]:
        """Parse a single stat reference into a normalised definition dict."""
        if isinstance(stat_ref, str):
            # Simple name reference → Tier 1 (or plugin if registered)
            name = stat_ref.strip()
            if not name:
                return None
            return {"name": name, "tier": 1}

        if not isinstance(stat_ref, dict):
            return None

        name = stat_ref.get("name", "").strip()
        if not name:
            return None

        if "expression" in stat_ref:
            expression = stat_ref["expression"]
            if not isinstance(expression, str) or not expression.strip():
                return None
            return {
                "name": name,
                "tier": 2,
                "expression": expression.strip(),
                "depends_on": self._extract_expression_deps(expression),
                "description": stat_ref.get(
                    "description",
                    f"Expression: {expression.strip()}",
                ),
            }

        if "plugin" in stat_ref:
            return {
                "name": name,
                "tier": 3,
                "plugin_path": stat_ref["plugin"],
                "depends_on": stat_ref.get("depends_on", []),
                "description": stat_ref.get(
                    "description",
                    f"Plugin: {stat_ref['plugin']}",
                ),
            }

        # Generic dict – might have compute_fn injected later
        return {
            "name": name,
            "tier": 1,
            "depends_on": stat_ref.get("depends_on", []),
            "description": stat_ref.get("description", ""),
        }

    # ------------------------------------------------------------------
    # Name resolution & conflict detection (STAT-002)
    # ------------------------------------------------------------------

    def _resolve_stat_definitions(
        self,
        definitions: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """Map stat names to their full definitions.

        STAT-002: duplicate names across tiers are a fatal error.

        Transitive dependencies are automatically pulled in: if a
        requested stat depends on ``total_compared_keys`` (for example),
        that stat is also included even if it was not explicitly listed
        in the report sections.

        Resolution order for Tier 1 (string references):

        1. :data:`BUILT_IN_STATS`
        2. ``_plugin_registry``
        """
        resolved: Dict[str, Dict[str, Any]] = {}
        seen: Dict[str, str] = {}  # name → "tier N / source" for error messages

        # Queue of definitions still to process (may grow as transitive
        # deps are discovered).
        queue: List[Dict[str, Any]] = list(definitions)

        # Track names that have ever been enqueued so that transitive
        # dependency resolution does not produce duplicates when the
        # user explicitly requested a dependency.
        enqueued: Set[str] = {d["name"] for d in definitions}

        while queue:
            defn = queue.pop(0)
            name: str = defn["name"]
            tier: int = defn.get("tier", 1)

            # STAT-002: check for name conflicts BEFORE the skip guard.
            # A name that was already resolved explicitly (by user request)
            # but appears again in the queue must be flagged.  Transitive
            # dependencies pulled in automatically are added *only* when
            # not already present, so they never trigger this.
            if name in seen:
                raise ComparisonError(
                    f"Stat name conflict (STAT-002): '{name}' is defined "
                    f"multiple times. First definition: {seen[name]}. "
                    f"Conflicting definition: tier {tier}.",
                    {"rule": "STAT-002", "stat_name": name},
                )

            # Already resolved (e.g. pulled in as a transitive dep) –
            # skip.  This prevents infinite loops on well-formed DAGs;
            # circular deps are caught later by topological sort.
            if name in resolved:
                continue

            source_desc = f"tier {tier}"
            seen[name] = source_desc

            if tier == 1:
                if name in BUILT_IN_STATS:
                    resolved[name] = dict(BUILT_IN_STATS[name])
                    resolved[name]["tier"] = 1
                elif name in self._plugin_registry:
                    resolved[name] = dict(self._plugin_registry[name])
                    resolved[name]["tier"] = 1
                else:
                    raise ComparisonError(
                        f"Unknown stat '{name}'. It is not a built-in stat "
                        f"and no plugin with that name has been registered. "
                        f"Available built-in stats: "
                        f"{sorted(BUILT_IN_STATS.keys())}",
                        {"rule": "STAT-001", "stat_name": name},
                    )
            elif tier == 3 and "plugin_path" in defn:
                plugin_def = self._load_plugin(defn["plugin_path"])
                if plugin_def is None:
                    raise ComparisonError(
                        f"Could not load plugin stat '{name}' from "
                        f"'{defn['plugin_path']}'. Ensure the class is "
                        f"importable and exposes a 'compute' static method.",
                        {"rule": "STAT-001", "stat_name": name},
                    )
                resolved[name] = {
                    "name": name,
                    "tier": 3,
                    "compute_fn": plugin_def["compute_fn"],
                    "depends_on": defn.get(
                        "depends_on", plugin_def.get("depends_on", [])
                    ),
                    "description": defn.get(
                        "description", plugin_def.get("description", "")
                    ),
                }
            else:
                resolved[name] = dict(defn)

            # ---- Transitive dependency resolution ----
            for dep in resolved[name].get("depends_on", []):
                if dep in enqueued or dep in resolved:
                    continue  # already queued or resolved
                # Dependency not yet seen – pull it in from built-in
                # or plugin registry.
                if dep in BUILT_IN_STATS:
                    queue.append({
                        "name": dep,
                        "tier": 1,
                    })
                    enqueued.add(dep)
                elif dep in self._plugin_registry:
                    queue.append({
                        "name": dep,
                        "tier": 1,
                    })
                    enqueued.add(dep)
                # If dep is neither built-in nor registered, it may be
                # another Tier 2/3 stat that will be resolved later,
                # or it's an error that will be caught in the compute
                # loop.

        return resolved

    # ------------------------------------------------------------------
    # Topological sort (STAT-004)
    # ------------------------------------------------------------------

    @staticmethod
    def _topological_sort(
        resolved: Dict[str, Dict[str, Any]],
    ) -> List[str]:
        """Topological sort using Kahn's algorithm.

        Parameters
        ----------
        resolved:
            Dict mapping stat name → definition (with ``depends_on``).

        Returns
        -------
        list[str]
            Stat names in computation order (dependencies first).

        Raises
        ------
        ComparisonError
            On circular dependencies (STAT-004).
        """
        # Build graph and in-degree map
        all_names: Set[str] = set(resolved.keys())
        graph: Dict[str, Set[str]] = {n: set() for n in all_names}
        in_degree: Dict[str, int] = {n: 0 for n in all_names}

        for name, defn in resolved.items():
            for dep in defn.get("depends_on", []):
                # Only count intra-graph edges (skip external deps not in
                # the resolved set)
                if dep in all_names:
                    graph[dep].add(name)
                    in_degree[name] += 1

        # Kahn's algorithm – deterministic ordering via sorted queue
        queue: List[str] = sorted(n for n, d in in_degree.items() if d == 0)
        order: List[str] = []

        while queue:
            node = queue.pop(0)
            order.append(node)

            for neighbour in sorted(graph[node]):
                in_degree[neighbour] -= 1
                if in_degree[neighbour] == 0:
                    queue.append(neighbour)
                    queue.sort()  # maintain deterministic order

        if len(order) != len(resolved):
            remaining = sorted(all_names - set(order))
            raise ComparisonError(
                f"Circular dependency detected among stats (STAT-004): "
                f"{remaining}",
                {"rule": "STAT-004", "circular_stats": remaining},
            )

        return order

    # ------------------------------------------------------------------
    # Tier 2: Expression evaluation (STAT-005)
    # ------------------------------------------------------------------

    @staticmethod
    def _evaluate_expression(
        expr: str,
        computed_stats: Dict[str, Any],
    ) -> Any:
        """Safely evaluate an arithmetic expression (STAT-005).

        Supports basic arithmetic operators (``+``, ``-``, ``*``, ``/``,
        ``//``, ``%``, ``**``) and comparison operators (``<``, ``>``,
        ``<=``, ``>=``, ``==``, ``!=``).  Stat names are resolved from
        *computed_stats*.

        **Disallowed**: function calls, attribute access, subscripts,
        comprehensions, lambda, imports, and any other complex constructs.

        Parameters
        ----------
        expr:
            The expression string to evaluate.
        computed_stats:
            Previously computed stat values available by name.

        Returns
        -------
        Any
            The result of the expression (typically ``int`` or ``float``).

        Raises
        ------
        ComparisonError
            On syntax errors, unknown references, or unsafe constructs.
        """
        stripped = expr.strip()
        try:
            tree = ast.parse(stripped, mode="eval")
        except SyntaxError as exc:
            raise ComparisonError(
                f"Invalid expression syntax: '{stripped}'",
                {"rule": "STAT-005", "expression": stripped},
            ) from exc

        # Validate every AST node – check disallowed constructs FIRST
        # (before Name resolution) so that e.g. ``__import__('os')``
        # is flagged as an illegal Call rather than an unknown Name.
        call_func_names: Set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                raise ComparisonError(
                    f"Function calls are not allowed in expressions (STAT-005): "
                    f"'{stripped}' contains {ast.dump(node)}.",
                    {"rule": "STAT-005", "expression": stripped},
                )
            elif isinstance(node, ast.Attribute):
                raise ComparisonError(
                    f"Attribute access is not allowed in expressions (STAT-005): "
                    f"'{stripped}' contains {ast.dump(node)}.",
                    {"rule": "STAT-005", "expression": stripped},
                )
            elif isinstance(node, ast.Subscript):
                raise ComparisonError(
                    f"Subscript access is not allowed in expressions (STAT-005): "
                    f"'{stripped}' contains {ast.dump(node)}.",
                    {"rule": "STAT-005", "expression": stripped},
                )
            elif isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
                raise ComparisonError(
                    f"Comprehensions are not allowed in expressions (STAT-005): "
                    f"'{stripped}'.",
                    {"rule": "STAT-005", "expression": stripped},
                )
            elif isinstance(node, ast.Lambda):
                raise ComparisonError(
                    f"Lambda expressions are not allowed (STAT-005): "
                    f"'{stripped}'.",
                    {"rule": "STAT-005", "expression": stripped},
                )
            elif isinstance(node, ast.Import):
                raise ComparisonError(
                    f"Import statements are not allowed (STAT-005): "
                    f"'{stripped}'.",
                    {"rule": "STAT-005", "expression": stripped},
                )
            elif isinstance(node, ast.Starred):
                raise ComparisonError(
                    f"Starred expressions are not allowed (STAT-005): "
                    f"'{stripped}'.",
                    {"rule": "STAT-005", "expression": stripped},
                )

        # Second pass: resolve Name references to computed stats
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                if node.id not in computed_stats:
                    raise ComparisonError(
                        f"Unknown stat reference in expression '{stripped}': "
                        f"'{node.id}'. Available: {sorted(computed_stats.keys())}",
                        {
                            "rule": "STAT-003",
                            "expression": stripped,
                            "reference": node.id,
                        },
                    )

        # Evaluate
        try:
            code = compile(tree, "<stat_expression>", "eval")
            result = eval(code, {"__builtins__": {}}, dict(computed_stats))
        except ZeroDivisionError:
            # Graceful handling consistent with _safe_divide
            return 0.0
        except Exception as exc:
            raise ComparisonError(
                f"Error evaluating expression '{stripped}': {exc}",
                {"rule": "STAT-005", "expression": stripped},
            ) from exc

        return result

    @staticmethod
    def _extract_expression_deps(expr: str) -> List[str]:
        """Extract stat-name references from an expression string.

        Names that appear as the ``func`` of a :class:`ast.Call` node
        (e.g. ``__import__`` in ``__import__('os')``) are **excluded**
        because they represent function calls, not stat references.
        Validation of such constructs happens later in
        :meth:`_evaluate_expression`.

        Returns a sorted, deduplicated list of ``ast.Name`` identifiers.
        """
        try:
            tree = ast.parse(expr.strip(), mode="eval")
        except SyntaxError:
            return []

        # Collect names used as call targets – these are NOT stat refs.
        call_targets: Set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                call_targets.add(node.func.id)

        deps: Set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and node.id not in call_targets:
                deps.add(node.id)
        return sorted(deps)

    # ------------------------------------------------------------------
    # Tier 3: Plugin loading
    # ------------------------------------------------------------------

    @staticmethod
    def _load_plugin(plugin_path: str) -> Optional[Dict[str, Any]]:
        """Dynamically import a plugin stat class.

        The class at *plugin_path* must expose:

        * ``compute(context: PipelineContext, stats: dict) -> Any``
          – static or class method.
        * ``depends_on: list[str]`` (optional) – stat dependencies.
        * ``description: str`` (optional) – human-readable description.

        Parameters
        ----------
        plugin_path:
            Dotted import path, e.g. ``"my_package.stats.CustomStat"``.

        Returns
        -------
        dict | None
            Plugin definition dict, or ``None`` if loading fails.
        """
        try:
            module_path, class_name = plugin_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            plugin_cls = getattr(module, class_name)
        except (ImportError, AttributeError, ValueError):
            return None

        if not hasattr(plugin_cls, "compute"):
            return None

        compute_fn = getattr(plugin_cls, "compute")
        if not callable(compute_fn):
            return None

        return {
            "compute_fn": compute_fn,
            "depends_on": getattr(plugin_cls, "depends_on", []),
            "description": getattr(plugin_cls, "description", ""),
        }

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @staticmethod
    def list_builtin_stats() -> Dict[str, str]:
        """Return ``{name: description}`` for all built-in stats."""
        return {
            name: defn.get("description", "")
            for name, defn in BUILT_IN_STATS.items()
        }

    @staticmethod
    def list_plugin_stats(engine: "StatsEngine") -> Dict[str, str]:
        """Return ``{name: description}`` for all registered plugin stats."""
        return {
            name: defn.get("description", "")
            for name, defn in engine._plugin_registry.items()
        }
