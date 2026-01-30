# BR1.2.5 – Provide Manual Override Ability for Complex Type Mapping

## 1. Objective

**Business Requirement:** BR1.2.5 – *Provide manual override ability for complex type mapping*  

**Expected Outcome:**  
Users can manually specify type mappings using `MODIFY_TYPE` directives in `ora2pg.conf`, and these manual overrides take precedence over automatic mappings.

**Validation:**  
Confirm that user-specified overrides are reflected in the output schema and that automatic mappings do not overwrite manual configurations.

---

## 2. Problem Description

**Example:** Business logic requires a specific PostgreSQL type that differs from the automatic mapping:

```conf
# Automatic mapping would use: numeric[]
# But requires: integer[]
MODIFY_TYPE    sales_data:monthly_totals:integer[]
```


## 3. Configuration Syntax

### 3.1 MODIFY_TYPE Directive

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


### 3.2 Implementation
1. `lib/Ora2Pg.pm` - Override checking logic during initialization
2. `lib/Ora2Pg/TypeMapper.pm` - Provides automatic mappings that can be overridden
