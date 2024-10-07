# Databases and Tables

Every database exists as a pair of files - a Primary file (`.wak`), a a Log file (`.wal`).

In terms of storing information, the Primary file is the most important. The Log file is simple a Write-Ahead Log (WAL).

_Note: The following is purely planning. The following may or may not be true._

### Databases

The master database contains a `databases` table. This table lists all databases (including `master`, as tracking itself is useful).

Note this database largely mirrors the DATABASE_INFO page at page index 1 of every `.wak` file.

| col              | description                                          |
| ---------------- | ---------------------------------------------------- |
| id               | The unique u16 id of the database.                   |
| name             | The name of the database. Max length 128 characters. |
| created_date     | The date the DB was created.                         |
| database_version | u8                                                   |

### Tables

The master database also contains a `tables` table:

| col          | description                                                  |
| ------------ | ------------------------------------------------------------ |
| id           | The unique u16 id of the table.                              |
| database_id  | The unique u16 id of the database that the table belongs to. |
| name         | The name of the table. Max length 128 characters.            |
| created_date | The date the table was created.                              |

and a `columns` table:

| col            | description                                                              |
| -------------- | ------------------------------------------------------------------------ |
| id             | The unique u16 id of the column.                                         |
| table_id       | The unique u16 id of the table that the column belongs to.               |
| name           | The name of the table. Max length 128 characters.                        |
| position       | The position in the table the column is at.                              |
| is_nullable    | If the column can store NULL.                                            |
| default_value  | The default value of the column, if a value is not specified.            |
| data_type      | The column type.                                                         |
| max_str_length | The max length of values in the column. Only applicable to string types. |
| num_precision  | The precision of the value. Only applicable to number types.             |
| created_date   | The date the table was created.                                          |
