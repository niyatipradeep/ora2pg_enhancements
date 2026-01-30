# BR1.2.3 – Detect and Flag Oracle Complex Types for Manual Review

## 1. Objective

**Business Requirement:** BR1.2.3 – *Detect Oracle complex types during export (INSERT, TABLE, COPY) and flag them for manual review*  

**Expected Outcome:**  
During the Ora2Pg export process, any Oracle complex types (VARRAY, OBJECT, NESTED TABLE) should be automatically detected and logged with structured warnings. A complete log file (`ora2pg_export.log`) should be generated for each export run, containing all console output plus specific warnings for complex types requiring manual intervention.

**Validation:**  
- Execute `ora2pg -t TABLE -c ora2pg.conf` and verify that complex types are detected and logged.
- Execute `ora2pg -t COPY -c ora2pg.conf` and verify that complex types are detected during data export.
- Execute `ora2pg -t INSERT -c ora2pg.conf` and verify that complex types are detected during INSERT statement generation.
- Check `output/ora2pg_export.log` to confirm:
  - The log file contains the complete terminal output from the export session.
  - Complex type warnings are present in the format: `[WARNING] [COMPLEX_TYPE] Table: <TABLE>, Column: <COLUMN>, Type: <TYPE> - Manual review required`
  - The log file is overwritten (not appended) on each new export run.

---

## 2. Problem Description

Oracle databases often contain complex user-defined types (UDTs) such as:
- **VARRAY** (variable-size arrays)
- **OBJECT** types (composite structures)
- **NESTED TABLE** (collections)

These types do not have direct equivalents in PostgreSQL and require manual review and custom migration strategies. Without automatic detection, DBAs and developers may:
- Miss complex types during the migration planning phase
- Encounter data export failures at runtime
- Struggle to identify which tables and columns need special handling
- Lack a comprehensive audit trail of all migration activities

---

## 3. Solution Overview

A modification to the core Ora2Pg library (`lib/Ora2Pg.pm`) that:

1. **Initializes a dedicated export log file** (`ora2pg_export.log`) at the start of each Ora2Pg session, opened in overwrite mode to ensure only the current run's logs are present.

2. **Integrates log file writing into the existing `logit` function**, so that all console output (INFO, DEBUG, WARNING messages) is automatically captured to the log file in addition to being displayed on the terminal.

3. **Implements a `log_complex_type_warning` method** that:
   - Accepts table name, column name, and type name as parameters
   - Generates a structured warning message in the format: `[WARNING] [COMPLEX_TYPE] Table: <TABLE>, Column: <COLUMN>, Type: <TYPE> - Manual review required`
   - Uses the standard `logit` function to ensure the warning appears in both console and log file



---


## 4. Usage Instructions

### 4.1 Standard Workflow

1. **Configure Ora2Pg** (in `ora2pg.conf`):
   ```ini
   OUTPUT_DIR    /home/user/ora2pg/output
   TYPE          TABLE,COPY,INSERT
   SCHEMA        your_schema
   ```

2. **Run TABLE Export** (DDL):
   ```bash
   ora2pg -t TABLE -c ora2pg.conf
   ```
   - Console shows: `[WARNING] [COMPLEX_TYPE] Table: EMPLOYEES, Column: ADDRESS, Type: ADDRESS_TYPE - Manual review required`
   - Log file updated: `output/ora2pg_export.log`

3. **Run COPY Export** (Data):
   ```bash
   ora2pg -t COPY -c ora2pg.conf
   ```
   - Complex types detected before data extraction
   - Warnings logged for each affected table/column
   - Data export proceeds (using Ora2Pg's existing ROW() conversion)

4. **Run INSERT Export** (Data):
   ```bash
   ora2pg -t INSERT -c ora2pg.conf
   ```
   - Same detection as COPY
   - Warnings logged
   - INSERT statements generated with complex type representations

### 4.2 Log File Contents

**Location:** `<OUTPUT_DIR>/ora2pg_export.log`

**Contents:**
- **Complete session transcript**: All INFO, DEBUG, and WARNING messages from the export
- **Timestamped entries**: Each line prefixed with `[YYYY-MM-DD HH:MM:SS]` (when DEBUG=1)
- **Structured warnings**: Complex type detections in parseable format


