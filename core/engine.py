"""
Pipeline engine - orchestrates all stages of the CSV comparison pipeline.
"""
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from pathlib import Path
from datetime import datetime, timezone


class PipelineError(Exception):
    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message)
        self.details = details or {}

    def __str__(self):
        base = super().__str__()
        if self.details:
            pairs = "; ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{base} [{pairs}]"
        return base


class ConfigurationError(PipelineError):
    pass


class ImportError(PipelineError):
    pass


class FilterError(PipelineError):
    pass


class NormalizationError(PipelineError):
    pass


class ComparisonError(PipelineError):
    pass


class PluginError(PipelineError):
    pass


class OutputError(PipelineError):
    pass


@dataclass
class PipelineContext:
    config: Dict[str, Any] = field(default_factory=dict)
    df_source_a: Optional[Any] = None
    df_source_b: Optional[Any] = None
    df_filtered_a: Optional[Any] = None
    df_filtered_b: Optional[Any] = None
    df_normalized_a: Optional[Any] = None
    df_normalized_b: Optional[Any] = None
    comparison_result: Optional[Dict] = None
    stats: Dict[str, Any] = field(default_factory=dict)
    filter_log: List[Dict] = field(default_factory=list)
    normalization_log: List[Dict] = field(default_factory=list)
    error_log: List[Dict] = field(default_factory=list)
    rows_source_a: int = 0
    rows_source_b: int = 0
    rows_filtered_a: int = 0
    rows_filtered_b: int = 0
    columns_source_a: List[str] = field(default_factory=list)
    columns_source_b: List[str] = field(default_factory=list)
    output_dir: str = "output"
    timestamp: str = ""


class PipelineStage:
    name: str = "base"

    def validate_config(self, config_section: Dict) -> None:
        pass

    def execute(self, context: PipelineContext, config: Dict) -> None:
        raise NotImplementedError

    def describe(self) -> str:
        return f"Stage: {self.name}"


class PipelineEngine:
    DEFAULT_STAGES = ["import", "filter", "normalize", "compare", "report"]

    def __init__(self):
        self.stages: List[PipelineStage] = []
        self._register_default_stages()

    def _register_default_stages(self):
        _registry = {}
        try:
            from .importer import CSVImporter
            _registry["import"] = CSVImporter
        except Exception as e:
            print(f"[DEBUG] Failed to import CSVImporter: {e}")
        try:
            from . import filter as filter_mod
            _registry["filter"] = filter_mod.FilterStage
        except Exception as e:
            print(f"[DEBUG] Failed to import FilterStage: {e}")
        try:
            from .normalizer import NormalizerStage
            _registry["normalize"] = NormalizerStage
        except Exception as e:
            print(f"[DEBUG] Failed to import NormalizerStage: {e}")
        try:
            from .comparator import ComparatorStage
            _registry["compare"] = ComparatorStage
        except Exception as e:
            print(f"[DEBUG] Failed to import ComparatorStage: {e}")
        try:
            from .reporter import ReporterStage
            _registry["report"] = ReporterStage
        except Exception as e:
            print(f"[DEBUG] Failed to import ReporterStage: {e}")

        for stage_name in self.DEFAULT_STAGES:
            cls = _registry.get(stage_name)
            if cls is not None:
                self.stages.append(cls())

    def register_stage(self, stage: PipelineStage):
        self.stages.append(stage)

    @classmethod
    def load_config(cls, config_path: str) -> Dict:
        path = Path(config_path)
        if not path.exists():
            raise ConfigurationError(f"Config file not found: {config_path}", {"path": config_path})
        try:
            import yaml
        except Exception as exc:
            raise ConfigurationError("PyYAML required. Install: pip install pyyaml") from exc
        try:
            with open(path, "r", encoding="utf-8") as fh:
                raw = yaml.safe_load(fh) or {}
        except yaml.YAMLError as exc:
            raise ConfigurationError(f"Invalid YAML: {exc}") from exc
        if not isinstance(raw, dict):
            raise ConfigurationError("Config must be a YAML mapping.")
        if raw.get("schema_version") is None:
            raise ConfigurationError("Missing 'schema_version'.", {"rule": "CFG-001"})
        return raw

    def validate_config(self, config: Dict) -> None:
        # CFG-001
        sv = config.get("schema_version")
        if sv is None:
            raise ConfigurationError("Missing 'schema_version'.", {"rule": "CFG-001"})
        if not isinstance(sv, str):
            raise ConfigurationError("'schema_version' must be a string.", {"rule": "CFG-001"})
        # CFG-002
        supported = {"1.0"}
        if sv not in supported:
            raise ConfigurationError(f"Unsupported schema_version '{sv}'. Supported: {sorted(supported)}", {"rule": "CFG-002"})

        # CFG-003 - sources section with source_a and source_b
        sources = config.get("sources")
        if not isinstance(sources, dict):
            raise ConfigurationError("Missing 'sources' section.", {"rule": "CFG-003"})
        if "source_a" not in sources or "source_b" not in sources:
            raise ConfigurationError("'sources' must contain 'source_a' and 'source_b'.", {"rule": "CFG-003"})

        # CFG-004 - source files must exist
        for label, key in [("source_a", "source_a"), ("source_b", "source_b")]:
            src = sources[label]
            if not isinstance(src, dict) or "file" not in src:
                raise ConfigurationError(f"Source '{label}' must have 'file' key.", {"rule": "CFG-004"})
            if not Path(src["file"]).exists():
                raise ConfigurationError(f"File not found: {src['file']}", {"rule": "CFG-004", "source": label})

        # CFG-005 - compare section with key_column
        compare = config.get("compare")
        if not isinstance(compare, dict):
            raise ConfigurationError("Missing 'compare' section.", {"rule": "CFG-005"})
        key_col = compare.get("key_column")
        if not isinstance(key_col, dict) or "source_a" not in key_col or "source_b" not in key_col:
            raise ConfigurationError("'compare.key_column' must have 'source_a' and 'source_b'.", {"rule": "CFG-005"})
        col_maps = compare.get("column_mappings")
        if not isinstance(col_maps, list) or len(col_maps) == 0:
            raise ConfigurationError("'compare.column_mappings' must be a non-empty list.", {"rule": "CFG-006"})

        # CFG-007 - mapped columns exist in sources
        df_a_cols = None
        df_b_cols = None
        try:
            import pandas as pd
            src_a = pd.read_csv(sources["source_a"]["file"], nrows=1, dtype=str)
            src_b = pd.read_csv(sources["source_b"]["file"], nrows=1, dtype=str)
            df_a_cols = set(src_a.columns)
            df_b_cols = set(src_b.columns)
        except Exception:
            pass  # skip column check if files can't be previewed

        if df_a_cols and df_b_cols:
            key_a = key_col["source_a"]
            key_b = key_col["source_b"]
            if key_a not in df_a_cols:
                raise ConfigurationError(f"Key column '{key_a}' not in source_a.", {"rule": "CFG-007"})
            if key_b not in df_b_cols:
                raise ConfigurationError(f"Key column '{key_b}' not in source_b.", {"rule": "CFG-007"})
            for m in col_maps:
                if m.get("source_a") and m["source_a"] not in df_a_cols:
                    raise ConfigurationError(f"Mapped column '{m['source_a']}' not in source_a.", {"rule": "CFG-007"})
                if m.get("source_b") and m["source_b"] not in df_b_cols:
                    raise ConfigurationError(f"Mapped column '{m['source_b']}' not in source_b.", {"rule": "CFG-007"})

        # CFG-008 - pre_filters operators valid
        VALID_OPS = {"equals", "not_equals", "in", "not_in", "greater_than", "less_than",
                      "contains", "matches_regex", "is_null", "is_not_null"}
        pf = config.get("pre_filters", {})
        for src_label in ["source_a", "source_b"]:
            rules = pf.get(src_label, [])
            for i, rule in enumerate(rules):
                if not isinstance(rule, dict):
                    raise ConfigurationError(f"pre_filters.{src_label}[{i}] must be a mapping.", {"rule": "CFG-008"})
                op = rule.get("operator")
                if op not in VALID_OPS:
                    raise ConfigurationError(f"Invalid operator '{op}' in pre_filters.{src_label}[{i}].", {"rule": "CFG-008"})

        # CFG-009 - normalization_rules structure
        nr = config.get("normalization_rules", {})
        if isinstance(nr, dict):
            groups = nr.get("alias_groups", [])
            seen_names = set()
            for g in groups:
                if isinstance(g, dict):
                    n = g.get("normalized_name")
                    if n in seen_names:
                        raise ConfigurationError(f"Duplicate alias group name '{n}'.", {"rule": "CFG-009"})
                    seen_names.add(n)

        # CFG-010 - output directory writable
        report = config.get("report", {})
        output_dir = report.get("output", {}).get("dir", "output")
        try:
            os.makedirs(output_dir, exist_ok=True)
        except Exception:
            raise ConfigurationError(f"Cannot create output directory: {output_dir}", {"rule": "CFG-010"})

    def run(self, config: Dict, output_dir: str = "output", verbose: bool = False) -> PipelineContext:
        self.validate_config(config)
        now = datetime.now(timezone.utc)
        context = PipelineContext(
            config=config,
            output_dir=output_dir,
            timestamp=now.strftime("%Y%m%dT%H%M%SZ"),
        )
        context.stats["pipeline_start"] = now.isoformat()
        os.makedirs(output_dir, exist_ok=True)

        for idx, stage in enumerate(self.stages, start=1):
            stage_label = f"{idx}/{len(self.stages)}"
            try:
                stage_config = config.get(stage.name, {})
                stage.validate_config(stage_config)
                stage.execute(context, config)
            except (ConfigurationError, PipelineError):
                context.error_log.append({
                    "stage": stage.name,
                    "error_type": "pipeline_error",
                    "message": str(__import__("sys").exc_info()[1]),
                })
                raise
            except Exception as exc:
                wrapped = PipelineError(
                    f"Unexpected error in stage '{stage.name}': {exc}",
                    {"stage": stage.name, "error_type": type(exc).__name__},
                )
                raise wrapped from exc

        end = datetime.now(timezone.utc)
        context.stats["elapsed_seconds"] = (end - now).total_seconds()
        return context
