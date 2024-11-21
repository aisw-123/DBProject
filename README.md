## Team Magnesium - DavisBase Project

## Team Members
* Sowmya Sivaramakrishnan (sxs230043)
* Nivedha Shankar (nxs230138)
* Korey Pickering (kap200004)
* Ishva Patel (irp190001)
* Aiswarya Madhu (axm230384)

## Output Screenshots :
Commands :

1. select * from davisbase_tables;

![image](https://user-images.githubusercontent.com/67775963/143494259-31d600e1-4420-41ec-9168-b53343ed1e9d.png)

2. create table zoo(name text unique, rowId int);

![image](https://user-images.githubusercontent.com/67775963/143494164-9051a76c-7eea-4d5c-9257-53b9a6283fe3.png)

![image](https://user-images.githubusercontent.com/67775963/143494219-b44bdebc-5ac5-41de-8a89-2d660e665c30.png)

3. select * from zoo;

![image](https://user-images.githubusercontent.com/67775963/143494351-08e0a30b-e5a6-451f-a52f-2d4746ae2645.png)

4. insert into zoo(name,rowID) values (lion,123);

![image](https://user-images.githubusercontent.com/67775963/143494406-c198f4c2-34b9-4e6d-8103-1be68bc3f7a5.png)

5. update zoo set rowId = 124 where name = lion;

![image](https://user-images.githubusercontent.com/67775963/143494510-5a35edf5-c7c2-4483-bf8b-49a3721753f1.png)

6. create index on zoo (name);

![image](https://user-images.githubusercontent.com/67775963/143494588-4d3ebfed-4595-407a-b5f2-7bc9773dc3ae.png)

![image](https://user-images.githubusercontent.com/67775963/143494630-739bf87d-2612-453c-ad4b-896208a355af.png)

7.  show tables;

![image](https://user-images.githubusercontent.com/67775963/143494683-12332c4e-896a-4d6e-9270-aad72db6e888.png)

8. drop table zoo;

![image](https://user-images.githubusercontent.com/67775963/143494799-98b7374d-8e96-4ada-8bff-e43552350084.png)

![image](https://user-images.githubusercontent.com/67775963/143494829-c605e75f-3b21-4fba-9c38-42abadb9a1b3.png)

## Required Features:

* The primary key column is defined as part of the Create Table process.
    * Support for a single column's primary key is all that is required.
    * Try inserting a duplicate key to see if it works.
* Make an index (only need to support creating an index on a single column)
    * It's worth noting that when a table is formed with a primary key column, an index is constructed implicitly and automatically on that column.
* Make changes to the record
    * Any changes to fixed-size data types in columns should be made "in place."
    * Any change to a text/string column that results in a larger string should delete the original record from the B+1 tree and create a new rowid at the far right leaf of the tree with the longer string value.
    * All indexes and primary keys in the table should be modified to point to the new rowid.
* Delete record 
    * To close the gap, simply remove the cell offset from the table header and shift the remaining offsets. Physically removing the record data or moving any additional records from the page's body is not required.
    * Table with Drop * (1) Remove a table file, (2) all associated indexes, and (3) meta-data table references to the table.
* Querying with the WHERE clause, i.e. WHERE column = value, SELECT * FROM table


## Supported Features 
* Data type validation is performed by INSERT, and any invalid insertions are aborted.
* Nullable Columns: NULL can only be placed into a nullable column; nullable columns are always nullable, and their default value is NULL.
* Unique Columns: By default, columns are not unique. INSERTing a duplicate value into a unique column will result in a failure.
* MetaData Updates: INSERT updates the record count in 'davisbase tables.tbl'.
* Display RowId: 'SHOW ROWID;' causes RowId to appear on 'SELECT';

## Design Premises : 
* When inputting a date, `Date` expects the following format: `YYYY-MM-DD`.
* When adding, `Time` anticipates milliseconds after midnight, but displays as `hh:mm:ss` in 24-hour or military time.
* When inserting, `DateTime` needs the following format: `YYYY-MM-DD hh:mm:ss`.
* When inserting, `Year` needs the following format: `YYYY` Because we don't check whether the given value is inside the range `[1872, 2127],` values outside of this range will overflow or underflow.
* When using `INSERT INTO () VALUES ()`, the supplied " will match on the columns of" using the NAMES of the columns, ie: the order of the columns passed in " is irrelevant - only the names must match the actual column names.
* The provided " is comma delimited when using `INSERT INTO () VALUES ()`, and strings do not need to be in quotes - quotes will be deleted. This entails










