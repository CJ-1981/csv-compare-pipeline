"""
CSV Importer stage – reads raw CSV files into the pipeline context.

Responsible for encoding detection (chardet), BOM stripping, optional
column pruning, chunked reading for large files, and duplicate-key
diagnostics.
"""
from __future__ import annotations

import codecs
import os
from typing import Any, Dict, List, Optional

from .engine import ImportError as PipelineImportError  # noqa: F401 – re-export for convenience
from .engine import PipelineContext, PipelineStage


class CSVImporter(PipelineStage):
    """Pipeline stage that ingests two CSV source files.

    Configuration keys (under ``config["import"]`` or at the source level):

    * ``delimiter``          – field separator (default ``","``).
    * ``encoding``           – explicit encoding string (auto-detected when omitted).
    * ``quotechar``          – quoting character (default ``'"'``).
    * ``use_columns``        – list of column names to keep (``None`` = all).
    * ``chunk_threshold_mb`` – file size (MB) above which chunked reading is
      used (default ``200``).
    * ``detect_sample_bytes`` – number of bytes to sample for encoding detection
      (default ``10000``).
    * ``encoding_confidence_threshold`` – minimum chardet confidence (default
      ``0.8``).

    Source-level overrides: keys above can also appear inside each source's
    config (``config["sources"]["a"]`` / ``config["sources"]["b"]``).  The
    source-level value takes precedence.
    """

    name = "import"

    # ------------------------------------------------------------------
    # Encoding detection
    # ------------------------------------------------------------------

    @staticmethod
    def detect_encoding(
        file_path: str,
        sample_bytes: int = 10_000,
        confidence_threshold: float = 0.8,
    ) -> str:
        """Detect file encoding using *chardet* on a leading byte sample.

        Parameters
        ----------
        file_path:
            Absolute or relative path to the file.
        sample_bytes:
            Number of bytes to read for detection (default 10 000).
        confidence_threshold:
            Minimum confidence score (0–1) to accept the detected encoding.
            Falls back to ``"utf-8"`` when below this threshold.

        Returns
        -------
        str
            The detected (or fallback) encoding string.
        """
        try:
            import chardet  # type: ignore[import-untyped]
        except Exception as exc:
            raise PipelineImportError(
                "The 'chardet' package is required for automatic encoding "
                "detection. Install it with: pip install chardet",
                {"rule": "IMP-002", "pip_package": "chardet"},
            ) from exc

        try:
            with open(file_path, "rb") as fh:
                raw = fh.read(sample_bytes)
        except OSError as exc:
            raise PipelineImportError(
                f"Cannot read file for encoding detection: {file_path}",
                {"rule": "IMP-003", "path": file_path},
            ) from exc

        result = chardet.detect(raw)
        detected = result.get("encoding", "").strip().lower()
        confidence = result.get("confidence", 0.0)

        if detected and confidence >= confidence_threshold:
            # Normalize common aliases
            encoding_alias_map: Dict[str, str] = {
                "ascii": "utf-8",
            }
            return encoding_alias_map.get(detected, detected)

        # Fallback
        return "utf-8"

    # ------------------------------------------------------------------
    # Chunked-reading heuristic
    # ------------------------------------------------------------------

    @staticmethod
    def should_use_chunks(file_path: str, threshold_mb: float = 200) -> bool:
        """Return ``True`` when *file_path* exceeds *threshold_mb*.

        Parameters
        ----------
        file_path:
            Path to the CSV file.
        threshold_mb:
            Size threshold in megabytes (default ``200``).

        Returns
        -------
        bool
        """
        try:
            size_bytes = os.path.getsize(file_path)
        except OSError as exc:
            raise PipelineImportError(
                f"Cannot stat file: {file_path}",
                {"rule": "IMP-005", "path": file_path},
            ) from exc
        return size_bytes > threshold_mb * 1024 * 1024

    # ------------------------------------------------------------------
    # BOM handling  (IMP-006)
    # ------------------------------------------------------------------

    @staticmethod
    def _strip_bom_if_present(file_path: str) -> bool:
        """Detect and remove a leading UTF-8 BOM (``\\xef\\xbb\\xbf``).

        Returns ``True`` if a BOM was found and stripped, ``False`` otherwise.

        The file is modified **in place**.
        """
        BOM_UTF8 = b"\xef\xbb\xbf"
        try:
            with open(file_path, "rb") as fh:
                header = fh.read(3)
        except OSError:
            return False

        if header == BOM_UTF8:
            try:
                with open(file_path, "rb") as fh:
                    content = fh.read()
                with open(file_path, "wb") as fh:
                    fh.write(content[len(BOM_UTF8):])
                return True
            except OSError:
                return False
        return False

    # ------------------------------------------------------------------
    # Duplicate key detection  (IMP-004)
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_duplicate_keys(df: Any, key_columns: Optional[List[str]] = None) -> List[str]:
        """Return a list of key values that appear more than once.

        Parameters
        ----------
        df:
            A pandas DataFrame.
        key_columns:
            Column(s) to treat as the composite key.  When ``None``, the
            first column is used.

        Returns
        -------
        list[str]
            Duplicate key values (as strings).  Empty when no duplicates.
        """
        import pandas as pd  # type: ignore[import-untyped]

        if key_columns is None:
            cols = [df.columns[0]]
        else:
            cols = [c for c in key_columns if c in df.columns]

        if not cols:
            return []

        duplicated_mask = df.duplicated(subset=cols, keep=False)
        dup_values = df.loc[duplicated_mask, cols].drop_duplicates()
        return [
            str(tuple(row)) if len(row) > 1 else str(row.iloc[0])
            for _, row in dup_values.iterrows()
        ]

    # ------------------------------------------------------------------
    # Core read helper
    # ------------------------------------------------------------------

    def read_csv(
        self,
        file_path: str,
        config: Dict[str, Any],
        label: str,
        context: PipelineContext,
    ) -> Any:
        """Read a single CSV file into a pandas DataFrame.

        All values are read as ``str`` per **IMP-001**.

        Parameters
        ----------
        file_path:
            Path to the CSV file.
        config:
            The full pipeline configuration (source-level overrides are
            resolved internally).
        label:
            Human-readable label (``"a"`` or ``"b"``).
        context:
            The shared pipeline context (metadata is written here).

        Returns
        -------
        pandas.DataFrame

        Raises
        ------
        ImportError (pipeline)
            On any I/O, parsing, or encoding error.
        """
        import pandas as pd  # type: ignore[import-untyped]

        # Merge global import config with source-level overrides
        global_import = config.get("import", {})
        source_cfg = config.get("sources", {}).get(label, {})
        merged: Dict[str, Any] = {**global_import}
        for k in ("delimiter", "encoding", "quotechar", "use_columns"):
            if k in source_cfg:
                merged[k] = source_cfg[k]

        # ---- BOM handling (IMP-006) ----
        bom_stripped = self._strip_bom_if_present(file_path)
        if bom_stripped:
            context.filter_log.append({
                "stage": "import",
                "source": label,
                "action": "bom_stripped",
                "message": f"UTF-8 BOM detected and removed from {file_path}",
            })

        # ---- Encoding detection ----
        explicit_encoding = merged.get("encoding")
        sample_bytes = int(merged.get("detect_sample_bytes", 10_000))
        confidence_threshold = float(merged.get("encoding_confidence_threshold", 0.8))

        if explicit_encoding:
            encoding: str = explicit_encoding
        else:
            encoding = self.detect_encoding(
                file_path,
                sample_bytes=sample_bytes,
                confidence_threshold=confidence_threshold,
            )

        # ---- CSV dialect parameters ----
        delimiter: str = merged.get("delimiter", ",")
        quotechar: str = merged.get("quotechar", '"')
        use_columns: Optional[List[str]] = merged.get("use_columns")

        # ---- Chunked reading decision ----
        chunk_threshold_mb = float(merged.get("chunk_threshold_mb", 200))
        use_chunks = self.should_use_chunks(file_path, threshold_mb=chunk_threshold_mb)

        # ---- Build read kwargs (IMP-001: all columns as str) ----
        read_kwargs: Dict[str, Any] = {
            "sep": delimiter,
            "encoding": encoding,
            "quotechar": quotechar,
            "dtype": str,           # IMP-001 – every column as string
            "keep_default_na": False,
            "na_values": [""],      # only treat explicit empty as NaN
        }

        # Column pruning at read time
        if use_columns is not None:
            read_kwargs["usecols"] = use_columns

        # ---- Read the file ----
        try:
            if use_chunks:
                chunks: List[pd.DataFrame] = []
                for chunk in pd.read_csv(file_path, **read_kwargs, chunksize=50_000):
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)
            else:
                df = pd.read_csv(file_path, **read_kwargs)
        except pd.errors.EmptyDataError as exc:
            raise PipelineImportError(
                f"Source '{label}' CSV file is empty: {file_path}",
                {"rule": "IMP-003", "source": label, "path": file_path},
            ) from exc
        except pd.errors.ParserError as exc:
            raise PipelineImportError(
                f"Failed to parse CSV for source '{label}': {exc}",
                {"rule": "IMP-003", "source": label, "path": file_path},
            ) from exc
        except UnicodeDecodeError as exc:
            raise PipelineImportError(
                f"Encoding error reading source '{label}' "
                f"(detected={encoding}): {exc}",
                {"rule": "IMP-002", "source": label, "encoding": encoding},
            ) from exc
        except OSError as exc:
            raise PipelineImportError(
                f"Cannot read file for source '{label}': {exc}",
                {"rule": "IMP-003", "source": label, "path": file_path},
            ) from exc
        except Exception as exc:
            raise PipelineImportError(
                f"Unexpected error reading source '{label}': {exc}",
                {"rule": "IMP-003", "source": label},
            ) from exc

        return df

    # ------------------------------------------------------------------
    # Stage interface
    # ------------------------------------------------------------------

    def validate_config(self, config_section: Dict) -> None:
        """Validate the ``import`` section of the pipeline config.

        Only structural checks – the actual file-existence check happens in
        ``PipelineEngine.validate_config`` (rule CFG-004).
        """
        if not isinstance(config_section, dict):
            raise PipelineImportError(
                "'import' configuration must be a mapping.",
                {"rule": "IMP-001"},
            )

        allowed_keys = {
            "delimiter", "encoding", "quotechar", "use_columns",
            "chunk_threshold_mb", "detect_sample_bytes",
            "encoding_confidence_threshold", "path",
        }
        unknown = set(config_section.keys()) - allowed_keys
        if unknown:
            raise PipelineImportError(
                f"Unknown import configuration key(s): {sorted(unknown)}",
                {"rule": "IMP-001", "unknown_keys": sorted(unknown)},
            )

    def validate_config(self, config_section: Dict) -> None:
        pass  # Validated by engine

    def execute(self, context: PipelineContext, config: Dict) -> None:
        """Read both source CSV files and populate the context."""
        from time import perf_counter

        sources = config.get("sources", {})
        src_a = sources.get("source_a", {})
        src_b = sources.get("source_b", {})

        # Key columns from compare section
        compare_cfg = config.get("compare", {})
        key_col = compare_cfg.get("key_column", {})
        key_a = key_col.get("source_a")
        key_b = key_col.get("source_b")
        key_columns = [k for k in [key_a, key_b] if k]

        # ---- Source A ----
        path_a = src_a.get("file", "")
        t0 = perf_counter()
        df_a = self.read_csv(path_a, config, label="source_a", context=context)
        elapsed_a = perf_counter() - t0

        context.df_source_a = df_a
        context.rows_source_a = len(df_a)
        context.columns_source_a = list(df_a.columns)

        dup_keys_a = self._detect_duplicate_keys(df_a, key_columns)
        if dup_keys_a:
            context.filter_log.append({
                "stage": "import", "source": "source_a",
                "action": "duplicate_keys_warning", "rule": "IMP-004",
                "message": f"Source A has {len(dup_keys_a)} duplicate key(s): {dup_keys_a[:5]}",
                "duplicate_count": len(dup_keys_a),
            })

        context.stats["import_source_a"] = {
            "path": path_a, "rows": context.rows_source_a,
            "columns": len(context.columns_source_a),
            "elapsed_seconds": round(elapsed_a, 4),
        }

        # ---- Source B ----
        path_b = src_b.get("file", "")
        t1 = perf_counter()
        df_b = self.read_csv(path_b, config, label="source_b", context=context)
        elapsed_b = perf_counter() - t1

        context.df_source_b = df_b
        context.rows_source_b = len(df_b)
        context.columns_source_b = list(df_b.columns)

        dup_keys_b = self._detect_duplicate_keys(df_b, key_columns)
        if dup_keys_b:
            context.filter_log.append({
                "stage": "import", "source": "source_b",
                "action": "duplicate_keys_warning", "rule": "IMP-004",
                "message": f"Source B has {len(dup_keys_b)} duplicate key(s): {dup_keys_b[:5]}",
                "duplicate_count": len(dup_keys_b),
            })

        context.stats["import_source_b"] = {
            "path": path_b, "rows": context.rows_source_b,
            "columns": len(context.columns_source_b),
            "elapsed_seconds": round(elapsed_b, 4),
        }

        context.stats["import_total_seconds"] = round(elapsed_a + elapsed_b, 4)
