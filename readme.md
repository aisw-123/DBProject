## Team Magnesium - DavisBase Project

## Team Members

- Sowmya Sivaramakrishnan (sxs230043)
- Nivedha Shankar (nxs230138)
- Korey Pickering (kap200004)
- Ishva Patel (irp190001)
- Aiswarya Madhu (axm230384)

### Navigating the Prompt
- Upon loading the MagnesiumSQL DBMS you will be greeted with the opening prompt. You may begin by using the `help;` command:
  ![70%](help.png)
### Commands
All commands are listed under the help display. All commands must be terminated by a semicolon: `;`.
#### Show Tables

#### DDL Commands
##### Create Table
The `CREATE TABLE` command is used to insert a new table and follows the following format syntax:
```java
CREATE TABLE table_name ( column_name1 data_type1 [PRIMARY KEY][NOT NULL][UNIQUE], column_name2 data_type2 [NOT NULL][UNIQUE], ... );
```
Example:
![](creatcommand.png)
After table creation, the new table should display from the SHOW TABLE command, the record count of davis_base tables should increase to include the new table.
![](aftcreat.png)
The fun.tbl file will be located in the data folder:
![](databef.png)
##### Dropping Tables
The `DROP TABLE` command is used for deleting a table and follows the following format syntax:
```java
DROP TABLE table_name;
```
Example:
![](droptabcom.png)
After dropping the table, the SHOW TABLE command will no longer display the table, the record for it in davisbase_tables will be decremented, and the tbl file should no longer be located in the data folder:
![](tabgone.png)
##### Create Index
The `CREATE INDEX` command is used to insert a new table and follows the following format syntax:
```java
CREATE INDEX index_name ON table_name (column_name);
```
Example:
![](creatindex.png)
The index file index_id.ndx will be located also in the data folder:
![](indexloc.png)
##### Dropping an Index

#### DML Commands
##### Insert Command
The `INSERT INTO` command is used to insert a new value into a single table using the following format syntax:
```java
INSERT INTO TABLE table_name VALUES (value1, value2, value3, ...);
```
Example:
![](insert.png)
After a successful insertion, you will be able to see the inserted row using the select statement.
![](afterinsert.png)
##### Deleting Rows
The `DELETE` command is used to delete a row from a table. The deletion command requires a condition in the where clause. The syntax for deleting a table:
```java
DELETE FROM TABLE table_name [WHERE condition];
```
Example:
![](delete.png)
After deletion, you will no longer be able to find the record in the table:
![](afterdelete.png)
#### DQL Commands
##### Selection
The `SELECT` command has three possible clauses, but requires both the SELECT and FROM clause. Multi-line statements don’t terminate until a semicolon is reached. The following is the correct syntax for selection:
```java
SELECT * FROM table_name WHERE column_name=value; 

SELECT * FROM table_name WHERE column_name1>value1 AND column_name2>=value2; 

SELECT * FROM table_name WHERE NOT column_name=value; 

SELECT * FROM table_name WHERE column_name1 > value1 OR NOT column_name2 >= value2;
```
Examples:
![](selecting.png)

Displaying the rowid requires the first selection option to be rowid followed by a comma using the following syntax:
```java
SELECT rowid, * FROM table_name;
```
Examples:
![](selectrowid.png)
