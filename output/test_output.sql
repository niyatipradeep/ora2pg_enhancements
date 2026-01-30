
SET search_path = lolu,public;
BEGIN;

SET client_encoding TO 'UTF8';
SET synchronous_commit TO off;
SET search_path = lolu,public;

SET search_path = lolu,public;


COPY test_nested_full (id,post_title,tags,created_date) FROM STDIN;
2	Single Tag	{AI}	2025-10-23 05:43:29.462918
3	Multiple Tags	{Tech,Database,Cloud}	2025-10-23 05:43:38.639431
5	Special Characters	{C++,Node.js,AI honors}	2025-10-23 05:43:58.006032
6	Reordered Tags	{Cloud,Tech,Database}	2025-10-23 05:44:03.621477
\.

SET client_encoding TO 'UTF8';
SET synchronous_commit TO off;
SET search_path = lolu,public;

SET search_path = lolu,public;


COPY test_object_full (id,customer_name,address,created_date) FROM STDIN;
3	Full Object	("456 Oak Ave","Los Angeles",90210,"USA")	2025-10-23 05:49:38.052061
2	Partial NULLs	("123 Main St",,10001,)	2025-10-23 05:49:45.493058
6	Another Full Object	("789 Pine Rd","San Francisco",94105,"USA")	2025-10-23 05:49:56.257679
1	All NULLs	(,,,)	2025-10-23 05:50:04.954709
4	Special Characters	("O'Reilly St","NYC",10001,"US ku")	2025-10-23 05:50:13.847262
\.

SET client_encoding TO 'UTF8';
SET synchronous_commit TO off;
SET search_path = lolu,public;

SET search_path = lolu,public;


COPY test_varray_full (id,name,numbers,created_date) FROM STDIN;
3	Multiple Elements	{10,20,30,40,50}	2025-10-23 05:46:46.579111
5	Floating Points	{1.1,2.25,3.75}	2025-10-23 05:47:08.060520
2	Single Element	{42}	2025-10-23 05:47:14.382607
6	Negative Values	{-10,-20,-30}	2025-10-23 05:47:24.586447
\.

COMMIT;

