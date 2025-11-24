  ✓ Fixed malformed array literals
  ✓ Fixed numeric array formatting

SET search_path = lolu,public;
BEGIN;

SET client_encoding TO 'UTF8';
SET synchronous_commit TO off;
SET search_path = lolu,public;

SET search_path = lolu,public;


COPY test_nested (id,title,tags,created_date) FROM STDIN;
1	Blog Post 1	{technology,programming,oracle}	2025-09-06 05:55:16
2	Blog Post 2	{database,migration,postgresql}	2025-09-06 05:55:33
\.

SET client_encoding TO 'UTF8';
SET synchronous_commit TO off;
SET search_path = lolu,public;

SET search_path = lolu,public;


COPY test_object (id,customer_name,address,created_date) FROM STDIN;
1	John Doe	{123 Main St,New York,10001,USA}	2025-09-06 05:09:12
2	Jane Smith	{456 Oak Ave,Los Angeles,90210,USA}	2025-09-06 05:54:59
\.

SET client_encoding TO 'UTF8';
SET synchronous_commit TO off;
SET search_path = lolu,public;

SET search_path = lolu,public;


COPY test_varray (id,name,numbers,created_date) FROM STDIN;
1	Test1	{10,20,30}	2025-09-06 05:04:16
2	Test2	{100,200}	2025-09-06 05:06:56
3	Test3	{1,2,3,4,5}	2025-09-06 05:07:40
\.

COMMIT;

