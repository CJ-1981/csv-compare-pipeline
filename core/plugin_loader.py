"""
Plugin auto-discovery, loading, and registry management.

Scans a designated ``plugins/`` directory for ``.py`` files (excluding those
prefixed with ``_``), dynamically imports each module, invokes its
``register()`` function, and stores the returned capabilities in a central
registry.

Rule catalogue
~~~~~~~~~~~~~~
PLG-001  Every plugin must export a ``register()`` function that returns a
          capabilities dict.
PLG-002  Broken plugins are skipped with a warning; the pipeline continues.
PLG-003  Name conflicts in the plugin registry are detected and reported.
PLG-004  Plugin errors are caught and logged, never propagated.
PLG-005  Plugin execution timeout is configurable via ``signal.alarm``
          (Unix only).

Plugin contract
~~~~~~~~~~~~~~~
Each plugin file **must** export a ``register()`` function that returns a
dict with the following keys:

* ``name``          (str, required) – unique plugin identifier.
* ``version``       (str, required) – semantic version string.
* ``description``   (str, required) – human-readable summary.
* ``stats``         (list[dict], optional) – stat definitions.
* ``normalizers``   (list[dict], optional) – normalizer definitions.

Each **stat** entry:

.. code-block:: python

    {
        "name": str,
        "description": str,
        "compute": Callable[[PipelineContext, Dict[str, Any]], Any],
        "depends_on": List[str],
    }

Each **normalizer** entry:

.. code-block:: python

    {
        "name": str,
        "normalize": Callable[[Any], Any],
    }
"""
from __future__ import annotations

import importlib.util
import logging
import os
import signal
import sys
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from .engine import PipelineContext, PluginError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_PLUGIN_TIMEOUT: int = 30  # seconds (PLG-005)


# ---------------------------------------------------------------------------
# PluginLoader
# ---------------------------------------------------------------------------

class PluginLoader:
    """Auto-discovers, loads, validates, and registers pipeline plugins.

    Usage::

        loader = PluginLoader(plugins_dir="plugins", timeout=15)
        loader.discover_and_load_all()

        # Access registered plugins
        stats = loader.get_plugin_stats()
        normalizers = loader.get_plugin_normalizers()

    Parameters
    ----------
    plugins_dir:
        Path to the directory containing plugin ``.py`` files.
    timeout:
        Maximum seconds a plugin's ``register()`` function is allowed to
        run before being killed (PLG-005).  Set to ``0`` to disable
        timeout enforcement.
    """

    def __init__(
        self,
        plugins_dir: str = "plugins",
        timeout: int = _DEFAULT_PLUGIN_TIMEOUT,
    ) -> None:
        self._plugins_dir: str = plugins_dir
        self._timeout: int = timeout

        # Central registries
        self._plugin_metadata: Dict[str, Dict[str, Any]] = {}
        self._stat_registry: Dict[str, Dict[str, Any]] = {}
        self._normalizer_registry: Dict[str, Dict[str, Any]] = {}

        # Discovery tracking
        self._loaded_plugins: List[str] = []
        self._skipped_plugins: List[Dict[str, str]] = []
        self._errors: List[Dict[str, str]] = []

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def plugins_dir(self) -> str:
        """The configured plugins directory path."""
        return self._plugins_dir

    @property
    def timeout(self) -> int:
        """The configured plugin execution timeout in seconds."""
        return self._timeout

    @property
    def loaded_plugins(self) -> List[str]:
        """List of successfully loaded plugin names."""
        return list(self._loaded_plugins)

    @property
    def skipped_plugins(self) -> List[Dict[str, str]]:
        """List of skipped plugins with reasons."""
        return list(self._skipped_plugins)

    @property
    def errors(self) -> List[Dict[str, str]]:
        """List of errors encountered during plugin loading."""
        return list(self._errors)

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def discover_plugins(self, plugins_dir: Optional[str] = None) -> List[Dict[str, str]]:
        """Scan the plugins directory for valid ``.py`` plugin files.

        Files prefixed with ``_`` (e.g. ``_helper.py``) are silently
        skipped.  Only files ending in ``.py`` are considered.

        Parameters
        ----------
        plugins_dir:
            Override directory path.  Falls back to ``self.plugins_dir``.

        Returns
        -------
        list[dict]
            A list of dicts, each containing:

            * ``filepath`` – absolute path to the plugin file.
            * ``filename`` – base filename (e.g. ``"my_plugin.py"``).
            * ``module_name`` – Python-importable module name (filename
              stem).

        Raises
        ------
        (nothing – errors are logged, not propagated, per PLG-004)
        """
        target_dir = plugins_dir or self._plugins_dir
        discovered: List[Dict[str, str]] = []

        plugins_path = Path(target_dir)
        if not plugins_path.is_dir():
            logger.warning(
                "Plugins directory does not exist or is not a directory: %s",
                target_dir,
            )
            self._errors.append({
                "stage": "discover",
                "message": f"Plugins directory not found: {target_dir}",
                "severity": "warning",
            })
            return discovered

        try:
            entries = sorted(plugins_path.iterdir())
        except OSError as exc:
            logger.warning("Cannot list plugins directory '%s': %s", target_dir, exc)
            self._errors.append({
                "stage": "discover",
                "message": f"Cannot list directory: {exc}",
                "severity": "warning",
            })
            return discovered

        for entry in entries:
            # Skip directories and non-Python files
            if not entry.is_file():
                continue

            filename = entry.name

            # Skip non-.py files
            if not filename.endswith(".py"):
                continue

            # Skip _-prefixed files (private / utility modules)
            if filename.startswith("_"):
                logger.debug("Skipping private module: %s", filename)
                continue

            module_name = entry.stem
            discovered.append({
                "filepath": str(entry.resolve()),
                "filename": filename,
                "module_name": module_name,
            })

        logger.info(
            "Discovered %d plugin(s) in '%s'",
            len(discovered),
            target_dir,
        )
        return discovered

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    def load_plugin(self, filepath: str) -> Optional[Dict[str, Any]]:
        """Import a single plugin module and call its ``register()`` function.

        The module is loaded using :mod:`importlib.util` so that it does
        not pollute ``sys.modules``.

        Parameters
        ----------
        filepath:
            Absolute path to the plugin ``.py`` file.

        Returns
        -------
        dict | None
            The capabilities dict returned by ``register()``, or ``None``
            if loading failed.

        Raises
        ------
        (nothing – errors are caught and logged per PLG-004)
        """
        plugin_path = Path(filepath)
        module_name = plugin_path.stem

        try:
            # Dynamic import via importlib.util (isolated from sys.modules)
            spec = importlib.util.spec_from_file_location(module_name, filepath)
            if spec is None or spec.loader is None:
                raise PluginError(
                    f"Cannot create module spec for plugin: {filepath}",
                    {"rule": "PLG-001", "filepath": filepath},
                )

            module = importlib.util.module_from_spec(spec)

            # Execute the module code with timeout (PLG-005)
            if self._timeout > 0 and hasattr(signal, "SIGALRM"):
                capabilities = self._execute_with_timeout(
                    spec.loader.exec_module, module
                )
            else:
                spec.loader.exec_module(module)

            # Call register() – this is the plugin contract (PLG-001)
            if not hasattr(module, "register"):
                raise PluginError(
                    f"Plugin '{module_name}' has no 'register()' function. "
                    "Every plugin must export a register() function.",
                    {"rule": "PLG-001", "plugin": module_name, "filepath": filepath},
                )

            register_fn = getattr(module, "register")
            if not callable(register_fn):
                raise PluginError(
                    f"Plugin '{module_name}': 'register' attribute is not callable.",
                    {"rule": "PLG-001", "plugin": module_name},
                )

            # Call register() with timeout (PLG-005)
            if self._timeout > 0 and hasattr(signal, "SIGALRM"):
                capabilities = self._execute_with_timeout(register_fn)
            else:
                capabilities = register_fn()

            # Validate capabilities structure
            capabilities = self._validate_capabilities(capabilities, module_name)

            logger.info(
                "Loaded plugin '%s' v%s: %s",
                capabilities.get("name", module_name),
                capabilities.get("version", "?"),
                capabilities.get("description", ""),
            )

            return capabilities

        except PluginError:
            # PLG-004: catch and log, never propagate
            exc_info = sys.exc_info()
            error_msg = str(exc_info[1]) if exc_info[1] else "Unknown plugin error"
            logger.warning(
                "Plugin error in '%s' (PLG-004): %s",
                module_name,
                error_msg,
            )
            self._errors.append({
                "stage": "load",
                "plugin": module_name,
                "filepath": filepath,
                "message": error_msg,
                "severity": "error",
            })
            self._skipped_plugins.append({
                "plugin": module_name,
                "filepath": filepath,
                "reason": error_msg,
            })
            return None

        except Exception:
            # PLG-002 / PLG-004: catch all unexpected errors
            error_msg = traceback.format_exc()
            logger.warning(
                "Unexpected error loading plugin '%s' (PLG-002/PLG-004):\n%s",
                module_name,
                error_msg,
            )
            self._errors.append({
                "stage": "load",
                "plugin": module_name,
                "filepath": filepath,
                "message": f"Unexpected error: {error_msg}",
                "severity": "error",
            })
            self._skipped_plugins.append({
                "plugin": module_name,
                "filepath": filepath,
                "reason": f"Unexpected error during loading",
            })
            return None

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_plugin(self, capabilities: Dict[str, Any]) -> None:
        """Register a plugin's capabilities in the central registries.

        Stores the plugin's metadata and extracts stat / normalizer
        functions into their respective registries.

        Parameters
        ----------
        capabilities:
            The dict returned by a plugin's ``register()`` function.

        Raises
        ------
        (nothing – errors are logged, not propagated per PLG-004)
        """
        plugin_name = capabilities.get("name", "<unnamed>")

        try:
            # Store metadata
            self._plugin_metadata[plugin_name] = {
                "name": plugin_name,
                "version": capabilities.get("version", "0.0.0"),
                "description": capabilities.get("description", ""),
                "stats": capabilities.get("stats", []),
                "normalizers": capabilities.get("normalizers", []),
            }

            # Register stat functions
            stats: List[Dict[str, Any]] = capabilities.get("stats", [])
            for stat_def in stats:
                stat_name = stat_def.get("name", "")
                if not stat_name:
                    logger.warning(
                        "Plugin '%s' has a stat entry with no 'name'; skipping.",
                        plugin_name,
                    )
                    continue

                # Check for name conflicts (PLG-003)
                if stat_name in self._stat_registry:
                    existing_plugin = self._stat_registry[stat_name].get("plugin", "<unknown>")
                    logger.warning(
                        "Stat name conflict (PLG-003): '%s' is already registered "
                        "by plugin '%s'. Overwriting with plugin '%s'.",
                        stat_name,
                        existing_plugin,
                        plugin_name,
                    )

                compute_fn = stat_def.get("compute")
                if not callable(compute_fn):
                    logger.warning(
                        "Plugin '%s' stat '%s' has no callable 'compute' function; skipping.",
                        plugin_name,
                        stat_name,
                    )
                    continue

                self._stat_registry[stat_name] = {
                    "name": stat_name,
                    "description": stat_def.get("description", ""),
                    "compute": compute_fn,
                    "depends_on": stat_def.get("depends_on", []),
                    "plugin": plugin_name,
                }

            # Register normalizer functions
            normalizers: List[Dict[str, Any]] = capabilities.get("normalizers", [])
            for norm_def in normalizers:
                norm_name = norm_def.get("name", "")
                if not norm_name:
                    logger.warning(
                        "Plugin '%s' has a normalizer entry with no 'name'; skipping.",
                        plugin_name,
                    )
                    continue

                if norm_name in self._normalizer_registry:
                    existing_plugin = self._normalizer_registry[norm_name].get("plugin", "<unknown>")
                    logger.warning(
                        "Normalizer name conflict (PLG-003): '%s' is already "
                        "registered by plugin '%s'. Overwriting with plugin '%s'.",
                        norm_name,
                        existing_plugin,
                        plugin_name,
                    )

                normalize_fn = norm_def.get("normalize")
                if not callable(normalize_fn):
                    logger.warning(
                        "Plugin '%s' normalizer '%s' has no callable 'normalize' function; skipping.",
                        plugin_name,
                        norm_name,
                    )
                    continue

                self._normalizer_registry[norm_name] = {
                    "name": norm_name,
                    "normalize": normalize_fn,
                    "plugin": plugin_name,
                }

            self._loaded_plugins.append(plugin_name)

        except Exception:
            # PLG-004: catch and log
            error_msg = traceback.format_exc()
            logger.error(
                "Error registering plugin '%s' (PLG-004):\n%s",
                plugin_name,
                error_msg,
            )
            self._errors.append({
                "stage": "register",
                "plugin": plugin_name,
                "message": error_msg,
                "severity": "error",
            })

    # ------------------------------------------------------------------
    # Registry accessors
    # ------------------------------------------------------------------

    def get_plugin_stats(self) -> Dict[str, Dict[str, Any]]:
        """Return all registered plugin stat functions.

        Returns
        -------
        dict[str, dict]
            Mapping of stat name → stat definition dict containing
            ``name``, ``description``, ``compute`` (callable), and
            ``depends_on`` (list).
        """
        return dict(self._stat_registry)

    def get_plugin_normalizers(self) -> Dict[str, Dict[str, Any]]:
        """Return all registered plugin normalizer functions.

        Returns
        -------
        dict[str, dict]
            Mapping of normalizer name → normalizer definition dict
            containing ``name`` and ``normalize`` (callable).
        """
        return dict(self._normalizer_registry)

    def get_plugin_metadata(self) -> Dict[str, Dict[str, Any]]:
        """Return metadata for all loaded plugins.

        Returns
        -------
        dict[str, dict]
            Mapping of plugin name → metadata dict.
        """
        return dict(self._plugin_metadata)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_plugin_registry(self) -> None:
        """Check the plugin registry for name conflicts.

        Warns about any stat or normalizer name that appears more than
        once across different plugins (rule PLG-003).

        Raises
        ------
        PluginError
            If a critical name conflict is detected that could cause
            silent data corruption (i.e. two plugins register the same
            stat/normalizer name with different functions).
        """
        # Check stat name conflicts
        stat_name_to_plugins: Dict[str, List[str]] = {}
        for stat_name, stat_def in self._stat_registry.items():
            plugin = stat_def.get("plugin", "<unknown>")
            stat_name_to_plugins.setdefault(stat_name, []).append(plugin)

        for stat_name, plugins in stat_name_to_plugins.items():
            if len(plugins) > 1:
                logger.warning(
                    "Name conflict (PLG-003): stat '%s' is registered by "
                    "multiple plugins: %s",
                    stat_name,
                    plugins,
                )

        # Check normalizer name conflicts
        norm_name_to_plugins: Dict[str, List[str]] = {}
        for norm_name, norm_def in self._normalizer_registry.items():
            plugin = norm_def.get("plugin", "<unknown>")
            norm_name_to_plugins.setdefault(norm_name, []).append(plugin)

        for norm_name, plugins in norm_name_to_plugins.items():
            if len(plugins) > 1:
                logger.warning(
                    "Name conflict (PLG-003): normalizer '%s' is registered by "
                    "multiple plugins: %s",
                    norm_name,
                    plugins,
                )

        # Build conflict summary
        stat_conflicts = {
            name: plugins
            for name, plugins in stat_name_to_plugins.items()
            if len(plugins) > 1
        }
        norm_conflicts = {
            name: plugins
            for name, plugins in norm_name_to_plugins.items()
            if len(plugins) > 1
        }

        if stat_conflicts or norm_conflicts:
            details: Dict[str, Any] = {}
            if stat_conflicts:
                details["stat_conflicts"] = stat_conflicts
            if norm_conflicts:
                details["normalizer_conflicts"] = norm_conflicts

            raise PluginError(
                "Plugin name conflicts detected (PLG-003). "
                f"Stat conflicts: {len(stat_conflicts)}. "
                f"Normalizer conflicts: {len(norm_conflicts)}. "
                "See log for details.",
                {"rule": "PLG-003", **details},
            )

    # ------------------------------------------------------------------
    # Convenience: discover + load + register
    # ------------------------------------------------------------------

    def discover_and_load_all(self, plugins_dir: Optional[str] = None) -> Dict[str, Any]:
        """Discover, load, and register all plugins in one call.

        Parameters
        ----------
        plugins_dir:
            Override plugins directory.

        Returns
        -------
        dict
            Summary with keys ``loaded``, ``skipped``, ``errors``,
            ``stats_count``, ``normalizers_count``.
        """
        discovered = self.discover_plugins(plugins_dir)

        for plugin_info in discovered:
            capabilities = self.load_plugin(plugin_info["filepath"])
            if capabilities is not None:
                self.register_plugin(capabilities)

        # Validate after all plugins are registered
        try:
            self.validate_plugin_registry()
        except PluginError as exc:
            logger.warning("Plugin registry validation: %s", exc)

        return {
            "loaded": self._loaded_plugins,
            "skipped": self._skipped_plugins,
            "errors": self._errors,
            "stats_count": len(self._stat_registry),
            "normalizers_count": len(self._normalizer_registry),
        }

    # ------------------------------------------------------------------
    # Integration with StatsEngine
    # ------------------------------------------------------------------

    def integrate_with_stats_engine(self, stats_engine: Any) -> int:
        """Register all plugin stats into a :class:`StatsEngine` instance.

        Parameters
        ----------
        stats_engine:
            A ``StatsEngine`` instance (from ``core.stats``).

        Returns
        -------
        int
            Number of stats registered.
        """
        registered = 0
        for stat_name, stat_def in self._stat_registry.items():
            stats_engine.register_plugin(stat_name, {
                "compute_fn": stat_def["compute"],
                "depends_on": stat_def.get("depends_on", []),
                "description": stat_def.get("description", ""),
            })
            registered += 1
        return registered

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate_capabilities(
        self,
        capabilities: Any,
        module_name: str,
    ) -> Dict[str, Any]:
        """Validate the structure returned by ``register()``.

        Raises :class:`PluginError` on structural issues (PLG-001).

        Returns
        -------
        dict
            The validated capabilities dict.
        """
        if not isinstance(capabilities, dict):
            raise PluginError(
                f"Plugin '{module_name}': register() must return a dict, "
                f"got {type(capabilities).__name__}.",
                {"rule": "PLG-001", "plugin": module_name},
            )

        # Required fields
        for required_key in ("name", "version", "description"):
            if required_key not in capabilities:
                raise PluginError(
                    f"Plugin '{module_name}': register() returned dict "
                    f"missing required key '{required_key}'.",
                    {"rule": "PLG-001", "plugin": module_name, "missing": required_key},
                )

        name = capabilities["name"]
        if not isinstance(name, str) or not name.strip():
            raise PluginError(
                f"Plugin '{module_name}': 'name' must be a non-empty string.",
                {"rule": "PLG-001", "plugin": module_name},
            )

        # Validate stats entries if present
        stats = capabilities.get("stats")
        if stats is not None:
            if not isinstance(stats, list):
                raise PluginError(
                    f"Plugin '{name}': 'stats' must be a list.",
                    {"rule": "PLG-001", "plugin": name},
                )
            for idx, stat_def in enumerate(stats):
                self._validate_stat_entry(stat_def, name, idx)

        # Validate normalizer entries if present
        normalizers = capabilities.get("normalizers")
        if normalizers is not None:
            if not isinstance(normalizers, list):
                raise PluginError(
                    f"Plugin '{name}': 'normalizers' must be a list.",
                    {"rule": "PLG-001", "plugin": name},
                )
            for idx, norm_def in enumerate(normalizers):
                self._validate_normalizer_entry(norm_def, name, idx)

        return capabilities

    @staticmethod
    def _validate_stat_entry(
        stat_def: Any,
        plugin_name: str,
        index: int,
    ) -> None:
        """Validate a single stat entry from a plugin's capabilities."""
        if not isinstance(stat_def, dict):
            raise PluginError(
                f"Plugin '{plugin_name}': stat entry #{index} must be a dict, "
                f"got {type(stat_def).__name__}.",
                {"rule": "PLG-001", "plugin": plugin_name},
            )

        for required_key in ("name", "description", "compute"):
            if required_key not in stat_def:
                raise PluginError(
                    f"Plugin '{plugin_name}': stat entry #{index} missing "
                    f"required key '{required_key}'.",
                    {"rule": "PLG-001", "plugin": plugin_name, "entry": index},
                )

        if not callable(stat_def["compute"]):
            raise PluginError(
                f"Plugin '{plugin_name}': stat entry #{index} "
                f"'compute' must be callable.",
                {"rule": "PLG-001", "plugin": plugin_name, "entry": index},
            )

        depends_on = stat_def.get("depends_on", [])
        if not isinstance(depends_on, list):
            raise PluginError(
                f"Plugin '{plugin_name}': stat entry #{index} "
                f"'depends_on' must be a list.",
                {"rule": "PLG-001", "plugin": plugin_name, "entry": index},
            )

    @staticmethod
    def _validate_normalizer_entry(
        norm_def: Any,
        plugin_name: str,
        index: int,
    ) -> None:
        """Validate a single normalizer entry from a plugin's capabilities."""
        if not isinstance(norm_def, dict):
            raise PluginError(
                f"Plugin '{plugin_name}': normalizer entry #{index} must be "
                f"a dict, got {type(norm_def).__name__}.",
                {"rule": "PLG-001", "plugin": plugin_name},
            )

        for required_key in ("name", "normalize"):
            if required_key not in norm_def:
                raise PluginError(
                    f"Plugin '{plugin_name}': normalizer entry #{index} "
                    f"missing required key '{required_key}'.",
                    {"rule": "PLG-001", "plugin": plugin_name, "entry": index},
                )

        if not callable(norm_def["normalize"]):
            raise PluginError(
                f"Plugin '{plugin_name}': normalizer entry #{index} "
                f"'normalize' must be callable.",
                {"rule": "PLG-001", "plugin": plugin_name, "entry": index},
            )

    def _execute_with_timeout(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute a function with a ``signal.alarm`` timeout (PLG-005).

        Unix only.  Falls back to direct execution on platforms without
        ``SIGALRM`` (e.g. Windows).

        Parameters
        ----------
        func:
            The function to execute.
        *args, **kwargs:
            Arguments forwarded to *func*.

        Returns
        -------
        Any
            The return value of *func*.

        Raises
        ------
        PluginError
            If the function exceeds the timeout.
        """
        if not hasattr(signal, "SIGALRM"):
            # Non-Unix platform – execute without timeout
            return func(*args, **kwargs)

        old_handler = signal.getsignal(signal.SIGALRM)

        def _timeout_handler(signum: int, frame: Any) -> None:
            raise PluginError(
                f"Plugin execution exceeded timeout of {self._timeout}s (PLG-005).",
                {"rule": "PLG-005", "timeout": self._timeout},
            )

        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(self._timeout)

        try:
            result = func(*args, **kwargs)
        finally:
            signal.alarm(0)
            # Restore previous handler
            try:
                signal.signal(signal.SIGALRM, old_handler)
            except (ValueError, OSError):
                pass

        return result

    # ------------------------------------------------------------------
    # Reset
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """Clear all registries and tracking state.

        Useful for testing or re-initialisation.
        """
        self._plugin_metadata.clear()
        self._stat_registry.clear()
        self._normalizer_registry.clear()
        self._loaded_plugins.clear()
        self._skipped_plugins.clear()
        self._errors.clear()

    # ------------------------------------------------------------------
    # Representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"PluginLoader("
            f"plugins_dir={self._plugins_dir!r}, "
            f"timeout={self._timeout}, "
            f"loaded={len(self._loaded_plugins)}, "
            f"stats={len(self._stat_registry)}, "
            f"normalizers={len(self._normalizer_registry)}"
            f")"
        )
