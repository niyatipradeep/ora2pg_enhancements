# Ora2Pg Enhancements

## BR1.2.1 – Checksum Validation for Complex Types

### 1. Objective

**Business Requirement:** BR1.2.1 – *Implement checksum validation to ensure data integrity during Oracle to PostgreSQL migration for complex types*  

**Expected Outcome:**  
When migrating complex Oracle types (OBJECT, VARRAY, NESTED TABLE) to PostgreSQL, the system should generate and compare checksums between exported and imported data to guarantee that all values (including NULLs, nested structures, and special characters) are correctly transferred without data loss or corruption.

**Validation:**  
- Export data from Oracle with checksum generation enabled
- Import data to PostgreSQL
- Run validation to compare export vs import checksums
- Verify 100% checksum match rate for all complex type columns

---

### 2. Problem Description

During Oracle to PostgreSQL migration, complex types undergo significant transformations:

### Format Differences
- **Oracle format:** Perl references like `ARRAY(0x559abc)` or nested structures
- **PostgreSQL format:** String representations like `{1,2,3}` or `("field1","field2")`

### Data Integrity Challenges
- **NULL handling:** Different representations between Oracle and PostgreSQL
  - Arrays: `{10,NULL,30}` vs empty positions
  - Composites: `(field1,,field3,)` - trailing empty values
- **Timestamp precision:** Oracle includes trailing zeros that PostgreSQL may strip
  - `2025-10-23 05:47:08.060520` (Oracle) vs `2025-10-23 05:47:08.06052` (PostgreSQL)
- **Format variations:** Oracle wraps data in parentheses and braces differently
  - `("{1,2,3}")` (Oracle export) vs `{1,2,3}` (PostgreSQL)

---

### 3. Solution Overview


#### 3.1 Core Components

**ChecksumValidator.pm** 
- Pattern-based complex type detection (no hardcoded type names)
- Semantic normalization (converts different formats to canonical form)
- SHA256 checksum generation
- Export/import checksum comparison

#### 3.2 Key Features

**1. Pattern-Based Type Detection**
```perl
sub is_complex_type {
    my ($self, $src_type) = @_;
    return 0 unless $src_type;
    
    # Matches ANY type ending with these patterns:
    return 1 if $src_type =~ /_TYPE$/i;      # EMPLOYEE_TYPE, ADDRESS_TYPE, ...
    return 1 if $src_type =~ /_ARRAY$/i;     # NUMBER_ARRAY, VARCHAR_ARRAY, ...
    return 1 if $src_type =~ /_VARRAY$/i;    # ITEMS_VARRAY, ...
    return 1 if $src_type =~ /_TABLE$/i;     # NESTED_TABLE_TYPE, ...
    return 1 if $src_type =~ /^VARRAY/i;
    return 1 if $src_type =~ /^NESTED TABLE/i;
    
    return 0;
}
```



### 4. Configuration Options


```bash


COMPLEX_CHECKSUM_VALIDATION  1

COMPLEX_CHECKSUM_ALGORITHM  SHA256

COMPLEX_CHECKSUM_VALIDATION_OUTPUT  test_validation_report.txt

# JSON file storing export and import checksums
COMPLEX_CHECKSUM_FILE  complex_checksums.json
```

---

### 5. Usage Workflow

#### Step 1: Export with Checksum Generation
```bash
# Configure ora2pg.conf
COMPLEX_CHECKSUM_VALIDATION  1

# Export data (checksums auto-generated)
ora2pg -t COPY -c ora2pg.conf

# Checksums stored in: complex_checksums.json
```

#### Step 2: Validate Checksums
```bash
# Run validation
ora2pg -t TEST_COMPLEX_TYPES -c ora2pg.conf

# View detailed report
cat test_validation_report.txt
```

---

## BR1.2.2 – Generate Test INSERT Statements with Complex Types

### 1. Objective

**Business Requirement:** BR1.2.2 – *Generate test insert statements with complex types*  

**Expected Outcome:**  
Generated test scripts (with `INSERT` statements containing complex Oracle types) should execute successfully on the target PostgreSQL database.

**Validation:**  
Execute the transformed SQL file against PostgreSQL and verify that all `INSERT` statements run without errors and that data is stored correctly.

---

### 2. Problem Description

Ora2pg (or similar tools) can generate `INSERT` statements where complex Oracle types (nested tables, VARRAYs, objects) are encoded into **string fields** with additional wrapping. These raw outputs are **not directly valid PostgreSQL literals**, for example:

- Nested / array-like:
  - `'({"(AI)"})'`
  - `'({"(Tech,Database,Cloud)"})'`
  - `'("{10,20,30,40,50}")'`
- Object/composite-like:
  - `'({"(456 Oak Ave,Los Angeles,90210,USA)"})'`
  - `'({"(123 Main St,,10001,)"})'` (with NULL-like empty slots)
  - `'({"(O'Reilly St,NYC,10001,US ku)"})'` (contains a single quote)

These cause syntax errors or incorrect data when executed in PostgreSQL.

---

### 3. Solution Overview

A Perl script that **post-processes** an ora2pg-style SQL file containing `INSERT` statements and:

1. Detects **wrapped complex values** inside string literals, such as:
   - `( {"(a,b,c)"} )`
   - `( "{1,2,3}" )`
   - `"{10,20,30}"`
2. Determines, using simple heuristics, whether the inner content should become:
   - a **PostgreSQL array literal**, e.g. `{AI,Tech,Cloud}`, or  
   - a **PostgreSQL composite/object literal**, e.g. `("456 Oak Ave","Los Angeles",90210,"USA")`
3. Rewrites those string literals into proper PostgreSQL text representations.
4. Properly **escapes single quotes** inside the resulting SQL (e.g., `O'Reilly` → `O''Reilly`), so the final file can be executed safely.

The script processes the SQL file **in-place**, after making a `.bak` backup.

---

### 4. Script Usage

 Usage: 
```
    ora2pg -t INSERT -c ora2pg.conf

    perl fix_insert_syntax.pl yourfile.sql
```

---

## BR1.2.3 – Detect and Flag Oracle Complex Types for Manual Review

### 1. Objective

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

### 2. Problem Description

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

### 3. Solution Overview

A modification to the core Ora2Pg library (`lib/Ora2Pg.pm`) that:

1. **Initializes a dedicated export log file** (`ora2pg_export.log`) at the start of each Ora2Pg session, opened in overwrite mode to ensure only the current run's logs are present.

2. **Integrates log file writing into the existing `logit` function**, so that all console output (INFO, DEBUG, WARNING messages) is automatically captured to the log file in addition to being displayed on the terminal.

3. **Implements a `log_complex_type_warning` method** that:
   - Accepts table name, column name, and type name as parameters
   - Generates a structured warning message in the format: `[WARNING] [COMPLEX_TYPE] Table: <TABLE>, Column: <COLUMN>, Type: <TYPE> - Manual review required`
   - Uses the standard `logit` function to ensure the warning appears in both console and log file



---


### 4. Usage Instructions

#### 4.1 Standard Workflow

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

#### 4.2 Log File Contents

**Location:** `<OUTPUT_DIR>/ora2pg_export.log`

**Contents:**
- **Complete session transcript**: All INFO, DEBUG, and WARNING messages from the export
- **Timestamped entries**: Each line prefixed with `[YYYY-MM-DD HH:MM:SS]` (when DEBUG=1)
- **Structured warnings**: Complex type detections in parseable format

---

## BR1.2.4 – Map Oracle Custom Types Using Default Mappings

### 1. Objective

**Business Requirement:** BR1.2.4 – *Map Oracle custom types using default mappings*  

**Expected Outcome:**  
Oracle custom types (OBJECT, VARRAY, NESTED TABLE) are automatically mapped to PostgreSQL composite types and arrays according to built-in rules, without requiring manual MODIFY_TYPE directives.

**Validation:**  
Verify that generated PostgreSQL types correspond correctly to Oracle types, and that table definitions use the mapped types automatically.

---

### 2. Problem Description

Before automatic type mapping, users had to manually configure every Oracle custom type in the `ora2pg.conf` file using `MODIFY_TYPE` directives:

```conf
# Manual configuration required for every custom type
MODIFY_TYPE    test_varray:numbers:numeric[]
MODIFY_TYPE    test_object:address:address_type
MODIFY_TYPE    test_nested:tags:text[]
```
---

### 3. Solution Overview

#### 3.1 TypeMapper Module

A new Perl module `Ora2Pg::TypeMapper` that:

1. **Analyzes Oracle custom types** from `ALL_TYPES` and `ALL_TYPE_ATTRS`
2. **Determines PostgreSQL equivalents** using pattern-based rules:
   - `COLLECTION` → PostgreSQL array (e.g., `numeric[]`, `text[]`)
   - `OBJECT` → PostgreSQL composite type with matching structure
3. **Generates automatic mappings** for table columns

#### 3.2 Key Features

**Automatic Detection:**
- Queries Oracle data dictionary for all custom types in the schema
- Analyzes type structure (attributes, data types, nested relationships)
- Maps Oracle primitive types to PostgreSQL equivalents

**Intelligent Mapping Rules:**
```perl
# COLLECTION types → PostgreSQL arrays
NUMBER_ARRAY (TABLE OF NUMBER)          → numeric[]
STRING_LIST (TABLE OF VARCHAR2)         → text[]
TAG_LIST (VARRAY(100) OF VARCHAR2)      → text[]

# OBJECT types → PostgreSQL composite types
ADDRESS_TYPE (street, city, zip)        → address_type
EMPLOYEE_TYPE (id, name, salary)        → employee_type
```

### 4. Configuration

**File:** `ora2pg.conf`

```conf
# Enable automatic type mapping (default: enabled)
AUTO_TYPE_MAPPING    1

# Generate detailed type mapping report
TYPE_MAPPING_REPORT  output/type_mapping_report.txt
```


### 5. Usage Examples


```bash
# Export schema with automatic type mapping
ora2pg -c ora2pg.conf -t TABLE -o schema.sql
ora2pg -c ora2pg.conf -t COPY 

# Check the logs
[INFO] Automatic type mapping enabled, analyzing custom types...
[INFO] Auto-mapped TEST_VARRAY.NUMBERS -> numeric[]
[INFO] Auto-mapped TEST_OBJECT.ADDRESS -> address_type
[INFO] Auto-mapped TEST_NESTED.TAGS -> text[]
```




---

## BR1.2.5 – Provide Manual Override Ability for Complex Type Mapping

### 1. Objective

**Business Requirement:** BR1.2.5 – *Provide manual override ability for complex type mapping*  

**Expected Outcome:**  
Users can manually specify type mappings using `MODIFY_TYPE` directives in `ora2pg.conf`, and these manual overrides take precedence over automatic mappings.

**Validation:**  
Confirm that user-specified overrides are reflected in the output schema and that automatic mappings do not overwrite manual configurations.

---

### 2. Problem Description

**Example:** Business logic requires a specific PostgreSQL type that differs from the automatic mapping:

```conf
# Automatic mapping would use: numeric[]
# But requires: integer[]
MODIFY_TYPE    sales_data:monthly_totals:integer[]
```


### 3. Configuration Syntax

#### 3.1 MODIFY_TYPE Directive

**Syntax:**
```conf
MODIFY_TYPE    table_name:column_name:postgresql_type
```

**Parameters:**
- `table_name`: Oracle table name (case-insensitive)
- `column_name`: Oracle column name (case-insensitive)
- `postgresql_type`: Target PostgreSQL type

**Examples:**
```conf
# Simple type override
MODIFY_TYPE    employees:salary:integer

# Array type override
MODIFY_TYPE    products:tags:text[]

# Composite type override
MODIFY_TYPE    orders:shipping_address:jsonb


---
```


#### 3.2 Implementation
1. `lib/Ora2Pg.pm` - Override checking logic during initialization
2. `lib/Ora2Pg/TypeMapper.pm` - Provides automatic mappings that can be overridden

---

## BR1.2.6 – Notify Unsupported or Partially Supported Types

### 1. Objective

**Business Requirement:** BR1.2.6 – *Notify users about unsupported or partially supported complex types*  

**Expected Outcome:**  
During schema export, ora2pg detects and reports all complex types (custom and built-in) that are unsupported or partially supported in PostgreSQL, with clear warnings and actionable recommendations.

**Validation:**  
Confirm that all unsupported/partially supported types are detected and reported in the final export summary with appropriate severity levels and descriptions.

---

### 2. Problem Description



**Before Warning System:**
```
[2024-09-17 23:45:12] Exporting tables...
[2024-09-17 23:45:15] Export completed
```

**Problem:**
- Partially supported/unsupported complex types exported without warnings
- Potential data loss undetected
- Manual review required to find issues
- Errors discovered only during PostgreSQL import





### 3. Solution Overview

#### 3.1 Three-Tier Classification System

```

   FULLY SUPPORTED (90-95% confidence)
     • Simple arrays (VARRAY, TABLE OF primitive)         
     • Simple objects with primitive attributes           
                                                         
    PARTIALLY SUPPORTED (40-89% confidence)              
     • Nested objects                                     
     • Arrays of objects                                  
     • Objects with LOBs                                  
     • Spatial types (SDO_GEOMETRY)                       
     • XML types (XMLTYPE)                                
     
                                                          
   UNSUPPORTED (0-39% confidence)                        
     • REF types (object references)                     
     • BFILE (external file pointers)                    
     • ANYDATA, ANYTYPE (dynamic types)  
```

### 4. Configuration


```conf
# Required: Enable automatic type mapping
AUTO_TYPE_MAPPING    1

# Optional: Generate detailed type mapping report
TYPE_MAPPING_REPORT    1
```
