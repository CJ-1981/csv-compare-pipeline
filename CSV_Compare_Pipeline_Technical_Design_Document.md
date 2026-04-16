# Technical Design Document

## CSV Compare Pipeline

**Configurable Data Comparison & Reporting Engine**

Version 1.0 | Architecture & Specifications | April 2026

---

# 1. Executive Summary

This document defines the complete technical architecture, stack specifications, and design rules for the CSV Compare Pipeline, a configurable Python-based tool designed to import, clean, normalize, compare, and report on data from two CSV files. The pipeline addresses the need for a reusable, extensible, and large-scale data reconciliation engine that operates without requiring proprietary spreadsheet software or manual intervention for each comparison job.

The system is built around a config-driven philosophy where every aspect of the comparison process, from column mapping and normalization rules to filter conditions and report formatting, is controlled through a single YAML configuration file. This design ensures that non-technical users can configure and execute comparison jobs without modifying any Python code. For advanced business logic requirements, a plugin system allows custom functions to be dropped into a designated directory and automatically discovered by the pipeline at runtime.

The pipeline is architected in clearly separated layers: a core engine that handles all data processing, a TUI layer for user interaction, and an output engine that generates reports in multiple formats. This separation ensures that each layer can be independently tested, extended, or replaced without affecting the others. The document below provides detailed specifications for each component, including interface contracts, data flow, error handling strategies, and design rules that must be followed during implementation.

# 2. Architecture Overview

## 2.1 High-Level Architecture

The pipeline follows a layered architecture pattern with strict separation of concerns. At the top level, three distinct layers interact through well-defined interfaces: the Presentation Layer (TUI), the Core Engine Layer (business logic), and the Output Layer (report generation). Each layer communicates only with its adjacent layers, and no layer bypasses another to access shared resources directly.

The Core Engine is the heart of the system and is completely decoupled from any user interface. It exposes a programmatic API that the TUI layer calls, and it produces intermediate results that the Output Layer consumes. This means the engine can be used independently, for example from a Jupyter notebook, a web API, or a cron job, without any modification to the core code.

Data flows through the engine in a linear pipeline pattern: Import, Filter, Normalize, Join, Compare, Compute Stats, and Generate Report. Each stage receives the output of the previous stage and produces output for the next. This design makes it easy to insert, remove, or reorder stages as requirements evolve. Every stage is implemented as a separate module with a standardized interface, making the system inherently modular.

## 2.2 Pipeline Flow

The execution flow proceeds through seven sequential stages, each of which is responsible for a specific data transformation. The Import stage reads both CSV files into pandas DataFrames, handling encoding detection, delimiter inference, and data type casting. The Filter stage applies user-configured row exclusion rules to each source independently, removing rows that do not meet the criteria for comparison. The Normalize stage applies value transformation rules, including alias grouping, case normalization, whitespace trimming, and special character removal.

The Join stage performs a key-based merge of the two filtered and normalized DataFrames, producing an inner join for matched keys and identifying keys that exist only in one source. The Compare stage evaluates each mapped column pair cell-by-cell (or set-by-set for array-type data), producing a match/mismatch status for each cell. The Stats stage computes aggregate statistics based on the configured stat library and custom expressions. Finally, the Report stage formats all results into the user's chosen output formats and writes them to disk.

## 2.3 Layer Separation Diagram

| Layer | Component | Responsibility | Interface |
| --- | --- | --- | --- |
| Presentation | TUI (rich + prompt_toolkit) | User interaction, progress, wizard | CLI commands |
| Presentation | CLI (click) | Argument parsing, command routing | Terminal I/O |
| Core | Engine (engine.py) | Orchestrate pipeline stages | PipelineContext |
| Core | Importer (importer.py) | CSV file reading and parsing | DataFrame output |
| Core | Filter (filter.py) | Row-level filtering per source | Filtered DataFrame |
| Core | Normalizer (normalizer.py) | Value grouping, cleaning, parsing | Normalized DataFrame |
| Core | Comparator (comparator.py) | Key join, column comparison | ComparisonResult |
| Core | Stats (stats.py) | Aggregate metric computation | StatsResult dict |
| Core | Plugin Loader (plugin_loader.py) | Auto-discover and load plugins | Plugin registry |
| Output | Reporter (reporter.py) | Format and write reports | File output |
| Output | Templates (html, md) | Report layout templates | Rendered output |

# 3. Core Engine

## 3.1 Engine Module (engine.py)

### 3.1.1 Responsibilities

The engine module serves as the central orchestrator for the entire pipeline. It is responsible for loading and validating the configuration file, instantiating each pipeline stage in the correct order, passing data between stages through a shared context object, and handling any errors that occur during pipeline execution. The engine must not contain any business logic itself; instead, it delegates all data transformation work to the specialized stage modules and coordinates their execution.

The engine receives its configuration as a validated and normalized dictionary, produced by the configuration loader. This dictionary is passed to every stage along with the pipeline context, which is a mutable object that accumulates intermediate results as the pipeline progresses. Each stage reads its required inputs from the context, performs its transformations, and writes its outputs back to the context. This design ensures that stages are loosely coupled and can be independently unit-tested by populating the context with mock data.

### 3.1.2 PipelineContext Object

The PipelineContext is a dataclass that serves as the shared state container for the entire pipeline execution. It is created by the engine at the start of each run and passed to every stage in sequence. The context contains references to the raw DataFrames, filtered DataFrames, normalized DataFrames, the joined comparison result, computed statistics, normalization logs, filter logs, and any additional metadata that stages may need. By centralizing all state in a single object, the system avoids the complexity of passing individual arguments between stages and makes it straightforward to inspect the pipeline state at any point for debugging or logging purposes.

### 3.1.3 Stage Interface Contract

Every pipeline stage must implement a standardized interface consisting of three methods: validate_config(), execute(), and describe(). The validate_config() method receives the relevant section of the configuration dictionary and raises a ConfigurationError if any required keys are missing or values are invalid. The execute() method receives the PipelineContext and the configuration section, performs its transformation, and updates the context with its results. The describe() method returns a human-readable string describing what the stage does, which is used by the TUI layer to display progress information.

## 3.2 Data Processing Library: pandas

### 3.2.1 Why pandas

pandas is the foundational data processing library for this pipeline. It provides high-performance, in-memory data structures (DataFrame and Series) that are optimized for tabular data manipulation. For CSV files exceeding 500MB, pandas supports chunked reading through its read_csv() chunksize parameter, which allows the pipeline to process data in manageable batches without loading the entire file into memory at once. This capability is critical for the large file support requirement.

pandas also provides built-in support for data type inference, missing value handling, string operations, merge operations, and group-by aggregations, all of which are heavily used by the pipeline stages. The library is widely adopted, well-documented, and has a stable API, which reduces the risk of breaking changes affecting the pipeline. Its integration with NumPy enables efficient vectorized operations that are orders of magnitude faster than equivalent Python loops.

### 3.2.2 Memory Management Strategy

For files larger than available RAM, the pipeline implements a chunked processing strategy. The Importer reads the CSV in fixed-size chunks (configurable, default 100,000 rows per chunk). Each chunk is processed through the Filter and Normalize stages independently, and only the rows that pass all filters are retained. The filtered chunks are then concatenated into a single DataFrame for the Join and Compare stages. This approach ensures that the memory footprint never exceeds the size of a single chunk plus the filtered output, rather than the full input file size.

Additionally, the pipeline supports optional column pruning at import time. If the user specifies only a subset of columns to compare, the Importer can drop all other columns during the initial read, significantly reducing memory usage. This is controlled by the use_columns configuration key in each source definition. When enabled, the Importer passes a usecols parameter to pandas read_csv(), which skips parsing of unwanted columns entirely.

### 3.2.3 Data Type Handling

The Importer performs intelligent data type inference using pandas dtype parameter. By default, all columns are read as strings (dtype=str) to preserve the exact cell values without unintended conversion. This is important because CSV comparison is fundamentally about string matching, and converting values to numeric or date types can introduce rounding, formatting, or timezone artifacts that cause false mismatches. However, the user can override this behavior by specifying explicit column types in the configuration, which is useful when numeric comparison with tolerance is needed.

# 4. Configuration System

## 4.1 Design Philosophy

The configuration system is the single most important design element of the pipeline. Its primary goal is to allow users to define every aspect of a comparison job without writing or modifying Python code. The configuration is stored in a YAML file that serves as the complete specification for a comparison job. The YAML format was chosen over JSON because it supports comments, which allow users to document their configuration decisions, and because its syntax is more readable for complex nested structures.

The configuration schema is versioned to ensure backward compatibility as the pipeline evolves. Each config file must include a schema_version key that indicates which version of the configuration schema it follows. The pipeline loads the appropriate schema validator based on this version, allowing older configuration files to continue working even after the pipeline has been updated with new features and configuration options.

## 4.2 Configuration Schema

The configuration file is organized into seven top-level sections: metadata, sources, key_column, column_mappings, pre_filters, normalization_rules, and report. Each section is validated independently by the configuration loader, and detailed error messages are produced if any section fails validation. The validation rules include type checking, required key verification, cross-reference validation (e.g., ensuring that column names referenced in mappings exist in the source files), and conflict detection (e.g., duplicate normalization group names).

### 4.2.1 Metadata Section

The metadata section contains non-functional information about the comparison job, including a human-readable title, a description, the schema version, and an optional list of tags for categorization. This information is used in the generated report header and in the TUI display. It has no effect on the comparison logic but provides important context for users reviewing the results.

### 4.2.2 Sources Section

The sources section defines the two input CSV files and their properties. Each source definition includes the file path (relative or absolute), delimiter character, encoding, quote character, and an optional list of columns to import. The engine validates that both files exist and are readable before starting the pipeline, and it produces clear error messages if a file cannot be found or if the specified encoding is incorrect.

### 4.2.3 Key Column and Column Mappings

The key_column section specifies which column in each source serves as the unique identifier for the join operation. This section supports column renaming, so the key column can have different names in each source file. The column_mappings section defines the pairs of columns that should be compared after the join. Each mapping entry specifies the source column name, the corresponding target column name, the comparison method (exact, set, or regex), and any normalization rules that should be applied before comparison.

### 4.2.4 Pre-Filters Section

The pre_filters section defines row-level exclusion rules that are applied independently to each source before the join operation. Each filter rule specifies a column, an operator (equals, not_equals, in, not_in, greater_than, less_than, contains, matches_regex, is_null, is_not_null), a value or list of values, and an action (skip or keep). Multiple filter rules within the same source are combined with AND logic, meaning a row must pass all rules to be retained.

### 4.2.5 Normalization Rules Section

The normalization_rules section defines how cell values should be transformed before comparison. There are three types of normalization rules. Global cleaning rules (trim_whitespace, remove_nulls, case_normalize) are applied to all compared columns uniformly. Alias groups define sets of values that should be treated as equivalent, with a display format template that controls how the normalized value is presented in the report (e.g., 'Model A({original})'). Column-specific rules allow per-column normalization overrides, including fixed-length parsing for contiguous text strings and array parsing for values that contain lists encoded as comma-separated strings or JSON arrays.

### 4.2.6 Report Section

The report section controls the output of the pipeline. It defines which report sections are enabled (summary, mismatch_details, unmatched_records, per_column_breakdown, normalization_log, filter_log), which statistics should be computed and displayed, which columns should appear in each report section, and the output file formats and locations. The report section is fully configurable, allowing users to tailor the report content to their specific needs without modifying the pipeline code.

## 4.3 Config Validation Rules

| Rule ID | Description | Error Level | Recovery |
| --- | --- | --- | --- |
| CFG-001 | Required top-level keys present | Fatal | Halt with message |
| CFG-002 | Schema version is supported | Fatal | Halt with message |
| CFG-003 | Source file paths exist and readable | Fatal | Halt with message |
| CFG-004 | Key columns exist in respective sources | Fatal | Halt with message |
| CFG-005 | Mapped columns exist in respective sources | Fatal | Halt with message |
| CFG-006 | No duplicate normalization group names | Fatal | Halt with message |
| CFG-007 | Filter operators are valid | Fatal | Halt with message |
| CFG-008 | Stat names in report ref exist in library | Fatal | Halt with message |
| CFG-009 | No name conflicts between inline and library stats | Fatal | Halt with message |
| CFG-010 | Output directory is writable | Warning | Create directory |
| CFG-011 | Plugin files in plugins/ are importable | Warning | Skip broken plugins |
| CFG-012 | Template files referenced exist | Warning | Fall back to default |

# 5. Import Engine (importer.py)

## 5.1 Responsibilities

The Import Engine is responsible for reading CSV files from disk and converting them into pandas DataFrames that the rest of the pipeline can operate on. This includes handling different file encodings, delimiters, quote characters, and line endings. The engine must detect these properties automatically when the user specifies 'auto' for any of them, but also allow explicit specification for cases where auto-detection fails.

Beyond basic file reading, the Import Engine performs initial data quality checks, including checking for duplicate key values within a single source, reporting the total row count and column count, and detecting whether the file uses a byte-order mark (BOM). These checks are performed before any filtering or normalization, and their results are stored in the PipelineContext for use in the summary report section.

## 5.2 Encoding Detection

When the encoding is set to 'auto', the Import Engine uses the chardet library to detect the file encoding. It reads the first 10,000 bytes of the file, passes them to chardet.detect(), and uses the detected encoding with a confidence threshold of 0.8. If the confidence is below this threshold, the engine falls back to UTF-8 with a warning message. The user can override this behavior by specifying an explicit encoding in the source configuration. Supported encodings include UTF-8, UTF-8-BOM, UTF-16, Latin-1, Windows-1252, and ASCII.

## 5.3 Chunked Reading Strategy

For files that exceed the configured memory threshold (default 200MB), the Import Engine activates chunked reading mode. In this mode, the file is read in fixed-size chunks specified by the chunk_size configuration parameter (default 100,000 rows). Each chunk is immediately passed through the Filter stage to remove unwanted rows before being appended to the output DataFrame. This streaming approach ensures that the memory footprint remains bounded regardless of the input file size. The chunk size can be tuned by the user based on available system memory and the expected density of filtered rows.

## 5.4 Design Rules

| Rule ID | Rule | Rationale |
| --- | --- | --- |
| IMP-001 | All columns read as string by default | Preserves exact cell values for comparison |
| IMP-002 | Chunked mode activates above configurable threshold | Prevents OOM on large files |
| IMP-003 | Encoding detection requires 0.8 confidence | Avoids mis-encoding with low-confidence guesses |
| IMP-004 | Duplicate key detection warns but does not halt | User may intentionally have non-unique keys |
| IMP-005 | Column pruning via use_cols at read time | Reduces memory for wide files |
| IMP-006 | BOM detection and stripping on first line | Prevents hidden characters in key columns |

# 6. Filter Engine (filter.py)

## 6.1 Responsibilities

The Filter Engine applies user-configured row exclusion rules to each source DataFrame independently, before the normalization and join stages. Its purpose is to remove rows that should not participate in the comparison, such as rows with zero stock values, rows with inactive statuses, or rows belonging to excluded regions. The Filter Engine operates on a rule-based system where each rule specifies a column, an operator, a comparison value, and an action (skip or keep).

A critical design decision is that filtering is applied independently to each source. This means the user can define different filter rules for Source A and Source B, reflecting the different data quality requirements of each source system. The filter results are logged in detail, including the number of rows removed by each rule and the total count of retained rows. This log is included in the filter_log report section and helps users understand the impact of their filter configuration.

## 6.2 Supported Operators

| Operator | Value Type | Example | Description |
| --- | --- | --- | --- |
| equals | scalar | Stock = 0 | Exact match, skip if equal |
| not_equals | scalar | Status != 'Deleted' | Skip if value matches |
| in | list | Region in ['EU', 'UK'] | Skip if value in list |
| not_in | list | Status not in ['Archived'] | Skip if value not in list |
| greater_than | scalar | Quantity > 0 | Numeric comparison |
| less_than | scalar | Age < 18 | Numeric comparison |
| contains | string | Name contains 'test' | Substring match |
| matches_regex | string | Code matches '^A\d{3}$' | Pattern match |
| is_null | none | Email is null | Skip if cell is empty/NaN |
| is_not_null | none | Name is not null | Skip if cell has a value |

## 6.3 Rule Evaluation Order

Filter rules within a single source are evaluated in the order they appear in the configuration file. Each rule is applied to the current state of the DataFrame, meaning that the result of one rule can affect the input to the next. This sequential evaluation model gives the user precise control over the filtering process. Rules are combined with AND logic: a row must pass all rules to be retained. If the user needs OR logic, they can achieve this by using the 'in' operator with a list of values. The engine logs the row count after each rule is applied, making it easy to diagnose unexpected filtering behavior.

## 6.4 Design Rules

| Rule ID | Rule | Rationale |
| --- | --- | --- |
| FLT-001 | Filters apply per-source, not globally | Sources may have different data quality |
| FLT-002 | Rules evaluated in config order, AND logic | Predictable, user-controlled execution |
| FLT-003 | Numeric operators coerce values to float | Handles string-encoded numbers gracefully |
| FLT-004 | Regex errors halt pipeline with clear message | Silent regex failures cause data loss |
| FLT-005 | Filter log records per-rule row counts | Enables audit trail for data reconciliation |

# 7. Normalization Engine (normalizer.py)

## 7.1 Responsibilities

The Normalization Engine transforms cell values into a standardized form before comparison. This is essential for handling real-world data where equivalent values may appear in different formats, casing, or naming conventions. The engine applies three categories of normalization rules: global cleaning rules that affect all compared columns, alias groups that map multiple values to a single canonical form, and column-specific rules that handle special parsing requirements such as fixed-length text segmentation and array string parsing.

The normalization process is applied after filtering but before the join, ensuring that the key column values are normalized consistently across both sources. This is critical because the join operation relies on exact key matching, and if one source has 'A001' while the other has 'a001', the join would fail to match them. By normalizing the key column first (using case normalization and trimming), the join correctly matches equivalent keys regardless of their original formatting.

## 7.2 Global Cleaning Rules

Global cleaning rules are applied uniformly to all compared columns. These include whitespace trimming (removing leading, trailing, and consecutive internal spaces), null value handling (replacing empty strings, 'N/A', 'NULL', 'null', and pandas NaN with a configurable sentinel value), and case normalization (converting all values to lowercase, uppercase, or title case). These rules are configured at the top level of the normalization_rules section and apply to every column listed in column_mappings.

## 7.3 Alias Groups and Value Mapping

Alias groups are the primary mechanism for handling business-specific value equivalence. Each alias group defines a canonical (normalized) name and a list of alias values that should be mapped to it. When a cell value matches any alias in the group, it is replaced with the normalized name. The display format for the normalized value is configurable through a template string that supports the {normalized} and {original} placeholders.

For example, if the alias group defines 'Model A' as the canonical name with aliases ['A001', 'A002', 'A003'], and the display format is '{normalized}({original})', then a cell containing 'A002' would be normalized to 'Model A(A002)'. This format preserves the original value in the report while clearly indicating that it has been grouped under the canonical name. The comparison between sources then operates on the canonical names, so 'A001' in Source A correctly matches 'A002' in Source B, both being recognized as 'Model A'.

## 7.4 Fixed-Length Parsing

Some data sources store multiple logical values in a single contiguous text string, where each value occupies a fixed number of characters. The Normalization Engine supports splitting such strings into their component parts using a configurable list of segment lengths. For example, if a notes field contains 'ABCDEFGHIJ123456789012345' and the segment lengths are [10, 15], the engine splits this into ['ABCDEFGHIJ', '123456789012345'] and compares each segment independently.

## 7.5 Array String Parsing

Array-type data can appear in various formats within CSV files: comma-separated strings ('a, b, c'), JSON-encoded arrays ('["a", "b", "c"]'), or bracket-quoted strings ('[a, b, c]'). The Normalization Engine detects and parses each format automatically, converting the cell value into a Python set for comparison purposes. When comparing array-type columns, the engine performs set-based comparison rather than string comparison, so the order of elements does not affect the result. The user specifies which columns contain array data through the array_parse flag in the column_mappings configuration.

## 7.6 Design Rules

| Rule ID | Rule | Rationale |
| --- | --- | --- |
| NRM-001 | Key column normalized before join | Ensures correct matching across sources |
| NRM-002 | Alias matching is case-insensitive | Handles inconsistent casing in source data |
| NRM-003 | Display format preserves original value | Audit trail for normalized values |
| NRM-004 | Fixed-length segments validated for total length | Detects malformed input data early |
| NRM-005 | Array parsing falls back to comma-split | Handles non-JSON array formats gracefully |
| NRM-006 | Normalization log records every transformation | Full traceability in reports |
| NRM-007 | Custom normalization via plugins supported | Business logic extensibility |

# 8. Comparator Engine (comparator.py)

## 8.1 Responsibilities

The Comparator Engine performs the core data comparison operation. It joins the two normalized DataFrames on the specified key column and evaluates each mapped column pair for every matched key. The engine produces a detailed comparison result that includes, for each key and each column pair, the source A value, the source B value, the normalized values, the match/mismatch status, and the type of difference detected (exact match, value difference, type difference, missing in one source, or array set difference).

## 8.2 Join Strategy

The join operation uses pandas merge() with an inner join by default, matching only keys that exist in both sources. The engine also identifies keys that exist only in Source A (left-only) and only in Source B (right-only), which are reported in the unmatched_records section. The user can configure the join type through the join_type configuration parameter, which supports 'inner', 'left', 'right', and 'outer' modes. Each mode produces a different set of matched and unmatched records.

## 8.3 Comparison Methods

Three comparison methods are supported. The 'exact' method performs a direct string equality check after normalization, suitable for simple scalar values. The 'set' method converts both values to sets and checks for set equality, used for array-type data where order does not matter. The 'regex' method applies a regular expression pattern to both values and checks whether the extracted groups match, useful for values that contain structured data with variable formatting.

## 8.4 Array Comparison Logic

When comparing array-type columns, the engine converts both values to sets and performs three-way comparison: it identifies values present only in Source A (only_in_a), values present only in Source B (only_in_b), and values present in both sources (common). A match is declared only if both sets are identical. For partial matches, the engine records the symmetric difference and includes it in the mismatch details, allowing users to see exactly which elements differ between the two sources.

## 8.5 Design Rules

| Rule ID | Rule | Rationale |
| --- | --- | --- |
| CMP-001 | Default join type is inner | Focus on records present in both sources |
| CMP-002 | Key column must be unique in each source (warn only) | Duplicate keys produce ambiguous comparisons |
| CMP-003 | Null vs null treated as match | Both missing is not a real mismatch |
| CMP-004 | Null vs value treated as mismatch with type 'missing' | Clearly distinguished from value diffs |
| CMP-005 | Set comparison ignores element order | Real-world arrays often unordered |
| CMP-006 | Comparison is case-sensitive after normalization | Normalization handles casing, not comparison |

# 9. Stats Engine (stats.py)

## 9.1 Three-Tiered Stat System

The Stats Engine implements a three-tiered computation system that provides increasing levels of flexibility while maintaining a clear separation between built-in functionality, user-defined expressions, and custom plugin logic. This design ensures that the majority of use cases can be addressed through configuration alone, while providing an escape hatch for truly unique business logic requirements.

### 9.1.1 Tier 1: Built-in Stat Library

The built-in stat library ships with the pipeline and provides a comprehensive set of commonly needed statistics. These include basic counts (total_rows_a, total_rows_b, matched_keys, only_in_a, only_in_b), match rates (match_rate_percent, per_column_match_rate), mismatch counts (total_mismatches, mismatches_by_column, high_risk_mismatches), and filter statistics (rows_filtered_a, rows_filtered_b, filter_breakdown). Each stat in the library has a unique name, a description, a list of dependencies (which context values it needs), and a compute function. Users reference stats by name in the report configuration, and the engine resolves dependencies automatically.

### 9.1.2 Tier 2: Inline Expression Stats

For statistics that are not covered by the built-in library, users can define inline expressions directly in the configuration file. These expressions support basic arithmetic operations, references to other stats (including built-in stats), and conditional logic. The expression engine evaluates these at runtime using safe evaluation, ensuring that no arbitrary code execution is possible. Inline stats are defined in the report section of the configuration and can reference any built-in stat by name.

### 9.1.3 Tier 3: Plugin Stats

For the most complex business logic requirements, users can define custom stat functions in Python plugin files. These plugins are placed in the plugins/ directory and automatically discovered by the Plugin Loader at startup. Each plugin can register one or more stat functions, along with their dependencies and descriptions. Plugin stats are referenced in the configuration exactly like built-in stats, making them transparent to the report configuration system.

## 9.2 Name Conflict Resolution

As discussed during the design phase, name conflicts between stats are treated as fatal errors. If an inline stat definition uses a name that already exists in the built-in library, or if two plugins register the same stat name, the pipeline halts with a clear error message identifying the conflict. This explicit-fail approach prevents silent overriding of stat definitions, which could lead to incorrect results that are difficult to diagnose. Users must use unique names for all custom stats and plugins.

## 9.3 Design Rules

| Rule ID | Rule | Rationale |
| --- | --- | --- |
| STAT-001 | Built-in stats are immutable from config | Prevents accidental override of core metrics |
| STAT-002 | Name conflicts are fatal errors | Explicit fail over silent misbehavior |
| STAT-003 | Inline expressions use safe evaluation only | No arbitrary code execution risk |
| STAT-004 | Stat dependencies resolved topologically | Correct evaluation order guaranteed |
| STAT-005 | Plugin stats auto-registered on load | Seamless integration with config system |
| STAT-006 | Stats computed once, cached in context | Performance: no redundant computation |

# 10. Plugin System (plugin_loader.py)

## 10.1 Plugin Architecture

The Plugin System provides a mechanism for extending the pipeline with custom business logic without modifying the core codebase. Plugins are Python files placed in the plugins/ directory that follow a simple registration protocol. At startup, the Plugin Loader scans the plugins/ directory, imports each Python file, and calls the register() function exported by each plugin. The register() function returns a dictionary describing the plugin's capabilities, including any custom stat functions, normalization functions, comparison methods, or output format handlers it provides.

## 10.2 Plugin Lifecycle

Each plugin goes through four lifecycle phases: discovery, registration, initialization, and execution. During discovery, the Plugin Loader scans the plugins/ directory for .py files and filters out files whose names start with an underscore (convention for private modules). During registration, each plugin's register() function is called and its capabilities are recorded in the plugin registry. During initialization, the pipeline calls the init(context) function of each plugin (if defined), passing the PipelineContext so the plugin can access configuration and data. During execution, the plugin's registered functions are called by the appropriate engine stage as needed.

## 10.3 Plugin API Contract

Every plugin must export a register() function that returns a dictionary with the following structure: a name field (string, unique identifier), a version field (string, semver format), a description field (string), and one or more capability sections (stats, normalizers, comparators, formatters). Each capability section is a dictionary mapping function names to function objects. Each function must accept a context parameter (the PipelineContext or a relevant subset) and return a value appropriate for its type.

Plugins can also define their own configuration sections by declaring a config_schema key in their registration dictionary. The configuration loader will merge plugin-specific configuration from the YAML file and pass it to the plugin during initialization. This allows plugins to have their own user-configurable parameters without polluting the main configuration namespace.

## 10.4 Design Rules

| Rule ID | Rule | Rationale |
| --- | --- | --- |
| PLG-001 | Plugins auto-discovered from plugins/ directory | Zero-config plugin installation |
| PLG-002 | Broken plugins skipped with warning | One bad plugin does not break pipeline |
| PLG-003 | Plugin function names must be unique globally | Conflict detection at registration |
| PLG-004 | Plugin exceptions caught and logged, not propagated | Isolation: plugin errors do not crash engine |
| PLG-005 | Plugin execution timeout configurable (default 30s) | Prevent hanging from poorly written plugins |
| PLG-006 | Plugin config schema validated independently | Early error detection for plugin misconfiguration |

# 11. Reporter Engine (reporter.py)

## 11.1 Output Format Support

The Reporter Engine generates comparison results in four output formats: Excel (.xlsx), CSV, HTML with CSS, and Markdown. Each format serves a different use case: Excel is the primary format for business users who need to review and annotate results; CSV is for system consumption and re-import into other tools; HTML with CSS is for sharing results via email or web browsers; and Markdown is for developers who want to include results in documentation or version control.

The user configures which formats to generate in the report.output section of the configuration file. Each format has its own configuration block that controls the output file name, the sheets or sections to include (for Excel and HTML), the styling options (for Excel and HTML), and any format-specific parameters. The Reporter Engine generates all configured formats in a single pipeline run, writing each file to the specified output directory.

## 11.2 Excel Output Specification

The Excel output uses the openpyxl library and generates a multi-sheet workbook. The workbook includes a Summary sheet with key statistics and a summary table, a Mismatch Details sheet with every cell-level comparison result, an Unmatched Records sheet with keys found in only one source, a Per-Column Breakdown sheet with per-column match rates, and optional Normalization Log and Filter Log sheets. Each sheet uses conditional formatting to highlight matches (green) and mismatches (red), with frozen header rows for easy scrolling. The Excel output also supports user-configurable styling, including header colors, font sizes, and column widths.

## 11.3 HTML Output Specification

The HTML output uses pre-built template files from the templates/ directory. The default template generates a self-contained HTML file with inline CSS that can be opened in any web browser or attached to an email without external dependencies. The HTML output includes collapsible sections for each report area, sortable tables for mismatch details, and a visual summary with progress bars for match rates. Users can provide their own CSS file through the css configuration parameter to override the default styling.

## 11.4 CSV and Markdown Output

The CSV output writes each report section as a separate CSV file, with one file per section. This format is designed for programmatic consumption and includes only the raw data without any formatting. Column headers match the configuration's column naming, and values are properly escaped for CSV format. The Markdown output generates a single .md file with the report content formatted using Markdown tables, headings, and code blocks. This format is ideal for including in Git repositories, wikis, or documentation systems that support Markdown rendering.

## 11.5 Design Rules

| Rule ID | Rule | Rationale |
| --- | --- | --- |
| RPT-001 | All formats generated in single run | Consistency across outputs |
| RPT-002 | Excel uses conditional formatting for match/mismatch | Visual clarity for reviewers |
| RPT-003 | HTML is self-contained (inline CSS) | No external dependencies for sharing |
| RPT-004 | CSV output has no formatting, raw data only | Programmatic consumption focus |
| RPT-005 | Report sections controlled by config toggle | User controls report content |
| RPT-006 | Timestamp included in every report | Traceability and version control |

# 12. TUI Layer

## 12.1 Library: rich

The Terminal User Interface layer uses the rich Python library for all terminal output formatting. rich provides a high-level API for rendering beautiful terminal output including styled tables, progress bars, panels, trees, and markdown-formatted text. It automatically handles terminal width detection, color support detection, and cross-platform compatibility (Linux, macOS, Windows), making it ideal for a data pipeline tool that needs to run in diverse environments.

The TUI layer is responsible for displaying pipeline progress (including animated progress bars for each stage), formatted result tables (including per-column breakdowns and mismatch summaries), error messages (with color-coded severity levels), and the configuration wizard interface. All TUI output is implemented through a dedicated display module (tui/display.py) that wraps the rich library and provides a consistent visual style across all output types.

## 12.2 Library: prompt_toolkit

The prompt_toolkit library is used for the interactive configuration wizard, which is triggered by the --init CLI flag. prompt_toolkit provides advanced terminal input features including auto-completion (for file paths, column names, and configuration options), input validation (ensuring that user responses match expected types and ranges), and multi-line input support (for complex configuration values). The wizard guides the user through the configuration process step by step, detecting available columns from the CSV files and offering intelligent defaults.

## 12.3 Library: click

The click library provides the CLI framework for the pipeline. It handles argument parsing, command routing, help text generation, and option validation. The pipeline exposes several CLI commands: run (execute the pipeline), init (launch the configuration wizard), validate (check configuration without executing), and version (display version information). Each command has its own set of options and help text, and click automatically generates formatted help output that explains each option.

## 12.4 CLI Command Specification

| Command | Options | Description |
| --- | --- | --- |
| run | --config, --output, --verbose, --dry-run | Execute the comparison pipeline |
| init | --output-dir | Create a new config from interactive wizard |
| validate | --config | Validate config without executing pipeline |
| version | (none) | Display pipeline version and exit |

## 12.5 Design Rules

| Rule ID | Rule | Rationale |
| --- | --- | --- |
| TUI-001 | All terminal output goes through rich | Consistent formatting and color support |
| TUI-002 | Progress bars for every pipeline stage | User visibility into long-running operations |
| TUI-003 | Error messages color-coded by severity | Quick visual identification of issues |
| TUI-004 | Tables auto-sized to terminal width | No truncation or wrapping issues |
| TUI-005 | CLI supports --verbose and --dry-run | Debugging and preview capabilities |
| TUI-006 | Wizard provides intelligent defaults | Reduce configuration friction for new users |

# 13. Error Handling and Logging

## 13.1 Error Classification

The pipeline uses a three-tier error classification system: fatal errors, recoverable errors, and warnings. Fatal errors are conditions that prevent the pipeline from producing any meaningful output, such as a missing source file, an invalid configuration schema, or a failed file read operation. When a fatal error occurs, the pipeline halts immediately and displays a detailed error message with the exact cause and location of the error, along with suggested corrective actions.

Recoverable errors are conditions that affect a portion of the data but do not invalidate the entire pipeline run. Examples include a single row that fails to parse, a normalization rule that produces an unexpected result, or a plugin that raises an exception during execution. When a recoverable error occurs, the pipeline logs the error, skips the affected row or operation, and continues processing the remaining data. The error is recorded in the report's error log section.

Warnings are informational messages that do not affect the pipeline's output but may indicate potential issues. Examples include low encoding detection confidence, high memory usage, or a deprecated configuration option. Warnings are displayed in the TUI output and recorded in the log file, but they do not affect the pipeline's exit code.

## 13.2 Exception Hierarchy

The pipeline defines a custom exception hierarchy rooted at PipelineError. Specific exception types include ConfigurationError (for config validation failures), ImportError (for file reading failures), FilterError (for invalid filter rules), NormalizationError (for transformation failures), ComparisonError (for join or comparison failures), PluginError (for plugin loading or execution failures), and OutputError (for report generation failures). Each exception type includes structured attributes that provide context about the error, such as the configuration key that caused the problem, the row number where the error occurred, or the plugin name that failed.

## 13.3 Logging Strategy

The pipeline uses Python's built-in logging module with structured log formatting. Logs are written to both the terminal (via rich console handler) and a log file (via file handler). The log file is created in the output directory alongside the generated reports, and its name includes a timestamp for traceability. Log levels are: DEBUG (detailed internal state, enabled with --verbose), INFO (pipeline stage progress and summary statistics), WARNING (potential issues that do not affect output), ERROR (recoverable errors that skip individual rows or operations), and CRITICAL (fatal errors that halt the pipeline).

## 13.4 Design Rules

| Rule ID | Rule | Rationale |
| --- | --- | --- |
| ERR-001 | Fatal errors halt pipeline with actionable message | User needs to know what to fix |
| ERR-002 | Recoverable errors logged and skipped | One bad row should not stop entire pipeline |
| ERR-003 | Custom exception hierarchy for all error types | Structured error handling and catching |
| ERR-004 | Log file always written to output directory | Post-run audit capability |
| ERR-005 | Exit codes: 0=success, 1=fatal, 2=recoverable errors | Scriptable integration with CI/CD |

# 14. Project File Structure

The following table shows the complete project directory structure with the purpose of each file and directory. This structure is designed to keep the codebase modular, testable, and easy to navigate. Core engine modules are in the core/ directory, TUI components are in the tui/ directory, plugins go in the plugins/ directory, and all generated output is written to the output/ directory.

| Path | Type | Purpose |
| --- | --- | --- |
| pipeline.py | File | CLI entry point (click commands) |
| config.yaml | File | User configuration (primary input) |
| requirements.txt | File | Python dependencies |
| core/__init__.py | File | Core engine package marker |
| core/engine.py | File | Pipeline orchestration and context management |
| core/importer.py | File | CSV file reading, encoding detection, chunking |
| core/filter.py | File | Row-level filtering with operator evaluation |
| core/normalizer.py | File | Value normalization, alias groups, parsing |
| core/comparator.py | File | Key join and cell-level comparison |
| core/stats.py | File | Stat computation engine (3-tier system) |
| core/plugin_loader.py | File | Auto-discovery and registration of plugins |
| core/reporter.py | File | Multi-format report generation |
| tui/__init__.py | File | TUI package marker |
| tui/display.py | File | Rich-based output formatting and progress |
| tui/wizard.py | File | Interactive config creation wizard |
| plugins/ | Directory | User plugin drop-in directory |
| templates/ | Directory | HTML and Markdown report templates |
| templates/report.html | File | Default HTML report template |
| templates/report.md | File | Default Markdown report template |
| css/ | Directory | User-supplied CSS overrides |
| output/ | Directory | Generated reports and log files |

# 15. Dependency Matrix

The following table lists all Python dependencies required by the pipeline, along with their minimum versions, primary purpose, and whether they are required or optional. All dependencies are managed through the requirements.txt file, and the pipeline validates that minimum version requirements are met at startup.

| Package | Min Version | Purpose | Required |
| --- | --- | --- | --- |
| pandas | 2.0 | Data processing, DataFrame operations | Yes |
| rich | 13.0 | TUI display, tables, progress bars | Yes |
| click | 8.0 | CLI framework and argument parsing | Yes |
| prompt_toolkit | 3.0 | Interactive prompts and autocompletion | Yes |
| PyYAML | 6.0 | YAML configuration file parsing | Yes |
| openpyxl | 3.1 | Excel (.xlsx) output generation | Yes |
| chardet | 5.0 | File encoding auto-detection | Yes |
| jinja2 | 3.1 | HTML template rendering | Optional |

# 16. Future Extension Points

The architecture is designed with several extension points that can be activated in future phases without requiring a major refactoring of the codebase. These extension points represent natural evolution paths based on anticipated user needs and technology trends. Each extension point is designed to layer on top of the existing architecture without disrupting the current module interfaces.

## 16.1 GUI Layer (Phase 2)

The current TUI layer can be supplemented with a graphical user interface built using a web framework such as Flask or FastAPI, paired with a frontend built with React or Vue.js. The GUI would communicate with the core engine through the same PipelineContext API that the TUI uses, meaning no changes to the core engine are needed. The GUI would provide drag-and-drop file uploading, interactive column mapping, real-time preview of comparison results, and a visual configuration editor. The web-based approach ensures cross-platform compatibility without the packaging challenges of native GUI frameworks like Tkinter or PyQt.

## 16.2 Textual TUI (Phase 2)

For users who prefer a richer terminal experience, the pipeline can be upgraded from rich to textual, the full TUI application framework built by the same team. textual provides interactive widgets including data tables with sorting and filtering, tabbed interfaces, form inputs, and even a built-in CSS layout engine. This upgrade would enable features like interactive browsing of mismatch results, clickable drill-down into specific rows, and a full-screen dashboard view of the comparison results, all within the terminal.

## 16.3 Database Source Support

The Import Engine can be extended to support direct database connections as data sources, in addition to CSV files. This would allow users to compare data directly from SQL databases (PostgreSQL, MySQL, SQLite) or cloud data warehouses (Snowflake, BigQuery) without first exporting to CSV. The extension would add new source type definitions to the configuration schema (e.g., source.type: 'postgresql') and implement corresponding reader classes that use SQLAlchemy or database-specific connectors to query the data.

## 16.4 Scheduled Execution

A scheduling system can be added to support recurring comparison jobs, such as daily data quality checks or nightly reconciliation runs. This would integrate with system schedulers (cron on Linux, Task Scheduler on Windows) through a simple configuration option that specifies the execution frequency and the config file to use. The pipeline would then run at the specified intervals and optionally send the generated reports via email or upload them to a shared location.

## 16.5 Jinja2 Template Customization

Currently held back for future extension, full Jinja2 template support would allow users to create completely custom report layouts for HTML and Markdown output. Users could provide their own Jinja2 template files with access to all pipeline data (comparison results, statistics, configuration), enabling highly specialized report formats tailored to specific business requirements. This extension would add a templates.custom section to the configuration where users can specify their template file paths.
