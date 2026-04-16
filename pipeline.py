#!/usr/bin/env python
"""
Pipeline CLI – command-line entry point for the CSV Compare Pipeline.

Provides four sub-commands via ``click``:

* **run**      — Execute the full comparison pipeline.
* **init**     — Create a new configuration file using the interactive wizard.
* **validate** — Validate a configuration file without executing.
* **version**  — Display version information.

Usage examples::

    python pipeline.py run -c config.yaml -o output -v
    python pipeline.py init -o .
    python pipeline.py validate -c config.yaml
    python pipeline.py version
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Optional

import click

# ---------------------------------------------------------------------------
# Ensure the pipeline package root is on sys.path so that imports work
# regardless of the working directory.
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Rich logging handler
# ---------------------------------------------------------------------------

try:
    from rich.logging import RichHandler
    _HAS_RICH_LOGGING = True
except ImportError:
    _HAS_RICH_LOGGING = False

# ---------------------------------------------------------------------------
# Pipeline imports
# ---------------------------------------------------------------------------

try:
    from core.engine import (
        ConfigurationError,
        PipelineContext,
        PipelineEngine,
        PipelineError,
    )
    from core.plugin_loader import PluginLoader
    from core.stats import StatsEngine
    from tui.display import Display
    from tui.wizard import ConfigWizard
    _PIPELINE_AVAILABLE = True
except ImportError as exc:
    _PIPELINE_AVAILABLE = False
    _IMPORT_ERROR = exc

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PIPELINE_VERSION: str = "1.0.0"
PIPELINE_NAME: str = "CSV Compare Pipeline"

# Exit codes
EXIT_SUCCESS: int = 0
EXIT_FATAL: int = 1
EXIT_RECOVERABLE: int = 2


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure the root logger with a rich handler (when available).

    Parameters
    ----------
    verbose:
        When ``True``, set log level to DEBUG; otherwise INFO.

    Returns
    -------
    logging.Logger
        The configured root logger.
    """
    log_level = logging.DEBUG if verbose else logging.INFO

    # Remove any existing handlers to avoid duplicates
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    root_logger.setLevel(log_level)

    if _HAS_RICH_LOGGING:
        handler = RichHandler(
            rich_tracebacks=True,
            show_time=True,
            show_path=verbose,
        )
    else:
        handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(log_level)
    formatter = logging.Formatter(
        fmt="%(message)s",
        datefmt="[%X]",
    )
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    return root_logger


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------

@click.group()
@click.version_option(PIPELINE_VERSION, prog_name=PIPELINE_NAME)
def cli() -> None:
    """CSV Compare Pipeline – Configurable data comparison and reporting engine."""


# ---------------------------------------------------------------------------
# run command
# ---------------------------------------------------------------------------

@cli.command()
@click.option(
    "--config", "-c",
    default="config.yaml",
    help="Path to the YAML configuration file.",
    show_default=True,
)
@click.option(
    "--output", "-o",
    default="output",
    help="Output directory for reports.",
    show_default=True,
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    default=False,
    help="Enable verbose logging.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show what would happen without executing.",
)
def run(config: str, output: str, verbose: bool, dry_run: bool) -> None:
    """Execute the comparison pipeline."""
    # Guard: ensure pipeline modules are available
    if not _PIPELINE_AVAILABLE:
        click.echo(
            f"[FATAL] Cannot import pipeline modules: {_IMPORT_ERROR}",
            err=True,
        )
        sys.exit(EXIT_FATAL)

    # Setup logging
    logger = _setup_logging(verbose)
    display = Display()

    # 1. Show header
    display.print_header(PIPELINE_NAME, PIPELINE_VERSION)

    # 2. Load config with validation
    display.print_stage_progress("Configuration", "running", f"loading {config}")

    try:
        loaded_config = PipelineEngine.load_config(config)
    except ConfigurationError as exc:
        display.print_error(f"Failed to load configuration: {exc}", details=exc.details)
        sys.exit(EXIT_FATAL)
    except Exception as exc:
        display.print_error(f"Unexpected error loading configuration: {exc}")
        sys.exit(EXIT_FATAL)

    display.print_stage_progress("Configuration", "done", f"schema v{loaded_config.get('schema_version', '?')}")

    # Merge CLI output override into report section
    if output != "output":
        loaded_config.setdefault("report", {}).setdefault("output", {})["dir"] = output

    # 3. Show config summary
    display.print_config_summary(loaded_config)

    # Dry-run: validate only
    if dry_run:
        display.print_info("Dry-run mode — validating configuration only.")
        display.print_separator()

        try:
            engine = PipelineEngine()
            engine.validate_config(loaded_config)
            display.print_success("Configuration is valid.")
            display.print_info(f"  Stages: {', '.join(s.name for s in engine.stages)}")
            sys.exit(EXIT_SUCCESS)
        except ConfigurationError as exc:
            display.print_error(f"Configuration validation failed: {exc}", details=exc.details)
            sys.exit(EXIT_FATAL)

    # 4. Create engine
    display.print_stage_progress("Engine", "running", "initializing pipeline engine")

    try:
        engine = PipelineEngine()
    except Exception as exc:
        display.print_error(f"Failed to initialize engine: {exc}")
        sys.exit(EXIT_FATAL)

    stage_names = [s.name for s in engine.stages]
    display.print_stage_progress("Engine", "done", f"{len(stage_names)} stage(s): {', '.join(stage_names)}")

    # 5. Load plugins (if plugins dir exists)
    plugin_dir = os.path.join(str(_PROJECT_ROOT), "plugins")
    plugin_loader = None
    if os.path.isdir(plugin_dir):
        display.print_stage_progress("Plugins", "running", f"scanning {plugin_dir}")
        try:
            plugin_loader = PluginLoader(plugins_dir=plugin_dir)
            plugin_summary = plugin_loader.discover_and_load_all()
            loaded = plugin_summary.get("loaded", [])
            skipped = plugin_summary.get("skipped", [])
            if loaded:
                display.print_stage_progress("Plugins", "done", f"{len(loaded)} plugin(s) loaded")
                for name in loaded:
                    display.print_info(f"  Plugin: {name}")
            if skipped:
                display.print_warning(f"{len(skipped)} plugin(s) skipped")
        except Exception as exc:
            display.print_warning(f"Plugin loading failed (non-fatal): {exc}")
    else:
        display.print_stage_progress("Plugins", "skipped", "no plugins directory found")

    # 6. Run pipeline
    display.print_separator()
    display.print_stage_progress("Pipeline", "running", "executing all stages")
    display.print_separator()

    context: Optional[PipelineContext] = None
    has_recoverable_errors = False

    try:
        context = engine.run(loaded_config, output_dir=output, verbose=verbose)

        # Compute statistics
        display.print_stage_progress("Statistics", "running", "computing metrics")
        try:
            stats_engine = StatsEngine()
            if plugin_loader:
                plugin_loader.integrate_with_stats_engine(stats_engine)
            stats = stats_engine.compute_all(context, loaded_config)
            context.stats.update(stats)
        except Exception as exc:
            display.print_warning(f"Statistics computation failed (non-fatal): {exc}")
            stats = context.stats

        display.print_stage_progress("Statistics", "done", f"{len(stats)} metric(s) computed")

    except ConfigurationError as exc:
        display.print_error(f"Configuration error: {exc}", details=exc.details)
        sys.exit(EXIT_FATAL)
    except PipelineError as exc:
        display.print_error(f"Pipeline error: {exc}", details=exc.details)
        sys.exit(EXIT_FATAL)
    except Exception as exc:
        display.print_error(f"Unexpected pipeline error: {exc}")
        sys.exit(EXIT_FATAL)

    if context is None:
        display.print_error("Pipeline produced no context (internal error).")
        sys.exit(EXIT_FATAL)

    # Check for recoverable errors (warnings in logs)
    if context.error_log:
        has_recoverable_errors = True
        for error_entry in context.error_log:
            display.print_warning(
                f"[{error_entry.get('stage', '?')}] {error_entry.get('message', 'Unknown error')}"
            )

    # 7. Show results table
    display.print_separator()
    display.print_results_table(context.stats)

    # 8. Show mismatch summary
    if context.comparison_result:
        display.print_mismatch_summary(context.comparison_result)

    # 9. Show filter summary
    if context.filter_log:
        display.print_filter_summary(context.filter_log)

    # 10. Show normalization summary
    if context.normalization_log:
        display.print_normalization_summary(context.normalization_log)

    # 11. Show output files
    display.print_separator()
    output_files: List[str] = []

    # Collect files from report stats
    report_stats = context.stats.get("report", {})
    for fmt in ("html", "excel", "csv", "markdown"):
        path_key = f"{fmt}_path"
        if path_key in report_stats:
            output_files.append(report_stats[path_key])

    # Also scan the output directory for any files produced
    if os.path.isdir(output):
        for entry in sorted(os.listdir(output)):
            full_path = os.path.join(output, entry)
            if os.path.isfile(full_path) and full_path not in output_files:
                output_files.append(full_path)

    display.print_output_files(output_files)

    # 12. Final status
    display.print_separator()
    elapsed = context.stats.get("elapsed_seconds", 0)
    display.print_success(f"Pipeline completed in {elapsed:.3f}s")

    if has_recoverable_errors:
        display.print_warning("Pipeline completed with recoverable errors. Review warnings above.")
        sys.exit(EXIT_RECOVERABLE)
    else:
        sys.exit(EXIT_SUCCESS)


# ---------------------------------------------------------------------------
# init command
# ---------------------------------------------------------------------------

@cli.command()
@click.option(
    "--output-dir", "-o",
    default=".",
    help="Directory to save the configuration file.",
    show_default=True,
)
def init(output_dir: str) -> None:
    """Create a new configuration file using the interactive wizard."""
    if not _PIPELINE_AVAILABLE:
        click.echo(
            f"[FATAL] Cannot import pipeline modules: {_IMPORT_ERROR}",
            err=True,
        )
        sys.exit(EXIT_FATAL)

    display = Display()
    display.print_header(PIPELINE_NAME, PIPELINE_VERSION)

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Change to the output directory so config.yaml is written there
    original_cwd = os.getcwd()
    try:
        os.chdir(output_dir)
        wizard = ConfigWizard()
        yaml_str = wizard.run()
        display.print_success("Configuration wizard completed successfully.")
    except Exception as exc:
        display.print_error(f"Wizard failed: {exc}")
        sys.exit(EXIT_FATAL)
    finally:
        os.chdir(original_cwd)


# ---------------------------------------------------------------------------
# validate command
# ---------------------------------------------------------------------------

@cli.command()
@click.option(
    "--config", "-c",
    default="config.yaml",
    help="Path to the YAML configuration file.",
    show_default=True,
)
def validate(config: str) -> None:
    """Validate configuration without executing pipeline."""
    if not _PIPELINE_AVAILABLE:
        click.echo(
            f"[FATAL] Cannot import pipeline modules: {_IMPORT_ERROR}",
            err=True,
        )
        sys.exit(EXIT_FATAL)

    display = Display()
    display.print_header(PIPELINE_NAME, PIPELINE_VERSION)

    # Load config
    display.print_stage_progress("Load", "running", f"reading {config}")

    try:
        loaded_config = PipelineEngine.load_config(config)
    except ConfigurationError as exc:
        display.print_error(f"Failed to load configuration: {exc}", details=exc.details)
        sys.exit(EXIT_FATAL)
    except Exception as exc:
        display.print_error(f"Unexpected error loading configuration: {exc}")
        sys.exit(EXIT_FATAL)

    display.print_stage_progress("Load", "done", f"schema v{loaded_config.get('schema_version', '?')}")

    # Show config summary
    display.print_config_summary(loaded_config)

    # Run validation
    display.print_stage_progress("Validate", "running", "checking configuration schema")

    try:
        engine = PipelineEngine()
        engine.validate_config(loaded_config)
    except ConfigurationError as exc:
        display.print_error(f"Validation failed: {exc}", details=exc.details)
        sys.exit(EXIT_FATAL)
    except Exception as exc:
        display.print_error(f"Unexpected validation error: {exc}")
        sys.exit(EXIT_FATAL)

    display.print_stage_progress("Validate", "done", "all checks passed")

    # Validate individual stages
    stage_names = [s.name for s in engine.stages]
    display.print_info(f"Validating {len(stage_names)} stage configuration(s)...")

    all_valid = True
    for stage in engine.stages:
        stage_config = loaded_config.get(stage.name, {})
        try:
            stage.validate_config(stage_config)
            display.print_stage_progress(f"  {stage.name.capitalize()}", "done")
        except ConfigurationError as exc:
            display.print_stage_progress(f"  {stage.name.capitalize()}", "error", str(exc))
            all_valid = False

    # File existence checks for sources
    sources = loaded_config.get("sources", {})
    for label, display_label in [("source_a", "A"), ("source_b", "B")]:
        src = sources.get(label, {})
        path = src.get("file", "")
        if path:
            if os.path.isfile(path):
                display.print_success(f"  Source {display_label} file exists: {path}")
                try:
                    import csv as csv_mod
                    delim = src.get("delimiter", ",")
                    enc = src.get("encoding", "utf-8")
                    with open(path, "r", encoding=enc, newline="") as fh:
                        reader = csv_mod.reader(fh, delimiter=delim)
                        headers = next(reader, None)
                        if headers:
                            display.print_info(f"  Source {display_label} columns ({len(headers)}): {', '.join(h.strip() for h in headers if h.strip())}")
                        else:
                            display.print_warning(f"  Source {display_label} CSV has no header row.")
                except UnicodeDecodeError:
                    display.print_warning(f"  Source {display_label} encoding issue with '{enc}'.")
                except Exception as exc:
                    display.print_warning(f"  Source {display_label} read issue: {exc}")
            else:
                display.print_error(f"  Source {display_label} file NOT found: {path}")
                all_valid = False

    display.print_separator()

    if all_valid:
        display.print_success("Configuration is valid and ready to use.")
        sys.exit(EXIT_SUCCESS)
    else:
        display.print_warning("Configuration has issues that should be fixed before running.")
        sys.exit(EXIT_RECOVERABLE)


# ---------------------------------------------------------------------------
# version command
# ---------------------------------------------------------------------------

@cli.command(name="version")
def version() -> None:
    """Display pipeline version information."""
    display = Display()
    display.print_header(PIPELINE_NAME, PIPELINE_VERSION)

    table_kwargs: dict = {}
    if _PIPELINE_AVAILABLE:
        from rich.table import Table as RichTable
        table = RichTable(
            title="Version Information",
            title_style="cyan",
            border_style="cyan",
            show_header=False,
            expand=False,
        )
        table.add_column("Key", style="bold cyan", min_width=22)
        table.add_column("Value", min_width=40)

        table.add_row("Pipeline Version", PIPELINE_VERSION)
        table.add_row("Python Version", f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")

        # Check dependency versions
        deps = ["click", "rich", "prompt_toolkit", "yaml", "pandas", "openpyxl"]
        for dep in deps:
            try:
                mod = __import__(dep)
                ver = getattr(mod, "__version__", "installed")
                table.add_row(f"  {dep}", str(ver))
            except ImportError:
                table.add_row(f"  {dep}", "not installed")

        # Pipeline components
        table.add_row("Pipeline Engine", "available")
        table.add_row("  Stages", f"import, filter, normalize, compare, report")

        try:
            from core.plugin_loader import PluginLoader
            table.add_row("Plugin Loader", "available")
        except ImportError:
            table.add_row("Plugin Loader", "not available")

        try:
            from core.stats import StatsEngine
            table.add_row("Stats Engine", "available")
        except ImportError:
            table.add_row("Stats Engine", "not available")

        try:
            from tui.wizard import ConfigWizard
            table.add_row("Config Wizard", "available")
        except ImportError:
            table.add_row("Config Wizard", "not available")

        display._console.print(table)
    else:
        display.print_error(f"Pipeline modules not available: {_IMPORT_ERROR}")

    sys.exit(EXIT_SUCCESS)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    cli()
