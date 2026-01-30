# BR1.2.6 – Notify Unsupported or Partially Supported Types

## 1. Objective

**Business Requirement:** BR1.2.6 – *Notify users about unsupported or partially supported complex types*  

**Expected Outcome:**  
During schema export, ora2pg detects and reports all complex types (custom and built-in) that are unsupported or partially supported in PostgreSQL, with clear warnings and actionable recommendations.

**Validation:**  
Confirm that all unsupported/partially supported types are detected and reported in the final export summary with appropriate severity levels and descriptions.

---

## 2. Problem Description



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





## 3. Solution Overview

### 3.1 Three-Tier Classification System

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

## 4. Configuration


```conf
# Required: Enable automatic type mapping
AUTO_TYPE_MAPPING    1

# Optional: Generate detailed type mapping report
TYPE_MAPPING_REPORT    1
```

