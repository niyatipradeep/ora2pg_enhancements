# BR1.2.2 – Generate Test INSERT Statements with Complex Types

## 1. Objective

**Business Requirement:** BR1.2.2 – *Generate test insert statements with complex types*  

**Expected Outcome:**  
Generated test scripts (with `INSERT` statements containing complex Oracle types) should execute successfully on the target PostgreSQL database.

**Validation:**  
Execute the transformed SQL file against PostgreSQL and verify that all `INSERT` statements run without errors and that data is stored correctly.

---

## 2. Problem Description

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

## 3. Solution Overview

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

## 4. Script Usage

 Usage: 
```
    ora2pg -t INSERT -c ora2pg.conf

    perl fix_insert_syntax.pl yourfile.sql
```

---

