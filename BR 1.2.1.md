# BR1.2.1 – Checksum Validation for Complex Types

## 1. Objective

**Business Requirement:** BR1.2.1 – *Implement checksum validation to ensure data integrity during Oracle to PostgreSQL migration for complex types*  

**Expected Outcome:**  
When migrating complex Oracle types (OBJECT, VARRAY, NESTED TABLE) to PostgreSQL, the system should generate and compare checksums between exported and imported data to guarantee that all values (including NULLs, nested structures, and special characters) are correctly transferred without data loss or corruption.

**Validation:**  
- Export data from Oracle with checksum generation enabled
- Import data to PostgreSQL
- Run validation to compare export vs import checksums
- Verify 100% checksum match rate for all complex type columns

---

## 2. Problem Description

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

## 3. Solution Overview


### 3.1 Core Components

**ChecksumValidator.pm** 
- Pattern-based complex type detection (no hardcoded type names)
- Semantic normalization (converts different formats to canonical form)
- SHA256 checksum generation
- Export/import checksum comparison

### 3.2 Key Features

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



## 4. Configuration Options


```bash


COMPLEX_CHECKSUM_VALIDATION  1

COMPLEX_CHECKSUM_ALGORITHM  SHA256

COMPLEX_CHECKSUM_VALIDATION_OUTPUT  test_validation_report.txt

# JSON file storing export and import checksums
COMPLEX_CHECKSUM_FILE  complex_checksums.json
```

---

## 5. Usage Workflow

### Step 1: Export with Checksum Generation
```bash
# Configure ora2pg.conf
COMPLEX_CHECKSUM_VALIDATION  1

# Export data (checksums auto-generated)
ora2pg -t COPY -c ora2pg.conf

# Checksums stored in: complex_checksums.json
```

### Step 2: Validate Checksums
```bash
# Run validation
ora2pg -t TEST_COMPLEX_TYPES -c ora2pg.conf

# View detailed report
cat test_validation_report.txt
```

