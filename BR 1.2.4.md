# BR1.2.4 – Map Oracle Custom Types Using Default Mappings

## 1. Objective

**Business Requirement:** BR1.2.4 – *Map Oracle custom types using default mappings*  

**Expected Outcome:**  
Oracle custom types (OBJECT, VARRAY, NESTED TABLE) are automatically mapped to PostgreSQL composite types and arrays according to built-in rules, without requiring manual MODIFY_TYPE directives.

**Validation:**  
Verify that generated PostgreSQL types correspond correctly to Oracle types, and that table definitions use the mapped types automatically.

---

## 2. Problem Description

Before automatic type mapping, users had to manually configure every Oracle custom type in the `ora2pg.conf` file using `MODIFY_TYPE` directives:

```conf
# Manual configuration required for every custom type
MODIFY_TYPE    test_varray:numbers:numeric[]
MODIFY_TYPE    test_object:address:address_type
MODIFY_TYPE    test_nested:tags:text[]
```
---

## 3. Solution Overview

### 3.1 TypeMapper Module

A new Perl module `Ora2Pg::TypeMapper` that:

1. **Analyzes Oracle custom types** from `ALL_TYPES` and `ALL_TYPE_ATTRS`
2. **Determines PostgreSQL equivalents** using pattern-based rules:
   - `COLLECTION` → PostgreSQL array (e.g., `numeric[]`, `text[]`)
   - `OBJECT` → PostgreSQL composite type with matching structure
3. **Generates automatic mappings** for table columns

### 3.2 Key Features

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

## 4. Configuration

**File:** `ora2pg.conf`

```conf
# Enable automatic type mapping (default: enabled)
AUTO_TYPE_MAPPING    1

# Generate detailed type mapping report
TYPE_MAPPING_REPORT  output/type_mapping_report.txt
```


## 5. Usage Examples


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