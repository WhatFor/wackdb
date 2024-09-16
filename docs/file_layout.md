# File Layout

A database is made up of a Primary File and a Log File.
The Primary File stores data and indicies. The Log file stores all transactions not yet committed to page.
The Primary File is paged. The Log file is append only and is not paged.

All managed databases exist as their own files, including system databases.
System databases store information about the system - including other databases.

## Primary File

Primary files have the .wak extension.

A Primary File follows the structure:

- Page 0: The File Info page.
- Page 1: The Database Info page.

## Pages

Pages are 8192 (8^13) bytes.

All pages have a header, 32 bytes in size:

| col                     | size    | description                                                           |
| ----------------------- | ------- | --------------------------------------------------------------------- |
| page_id                 | 4 bytes | The ID of the page                                                    |
| header_version          | 1 byte  | the WackDb Header version (for backwards compat)                      |
| page_type               | 1 byte  | The type of page                                                      |
| checksum                | 2 bytes | The checksum of the page content, for safety.                         |
| flags                   | 2 bytes | Any flags to be set on the page, such as 'can_compact'.               |
| allocated_slot_count    | 2 bytes | How many slots have been allocated to the page.                       |
| free_space              | 2 bytes | The number of free bytes within the page.                             |
| free_space_start_offset | 2 bytes | The start of the free space within the page.                          |
| free_space_end_offset   | 2 bytes | The end of the free space within the page.                            |
| total_allocated_bytes   | 2 bytes | The total number of bytes allocated to the page. Excludes the header. |

| page type       | description                                                                                 |
| --------------- | ------------------------------------------------------------------------------------------- |
| 0: FileInfo     | Info describing the database file. There will only be 1 of this page type, at page index 0. |
| 1: DatabaseInfo | Info describing the database. There will be only 1 of this page type, at page index 1.      |

## File Info Page

This page exists at page index 0; The first page.

The File Info page describes the entire database file.

The file info is stored in slot 1 of this page.

| col               | size    | description                                                                          |
| ----------------- | ------- | ------------------------------------------------------------------------------------ |
| magic_string      | 4 bytes | A specific string of 4 bytes at the start of every .wak file to make sure it's ours. |
| file_type         | 1 byte  | The type of file this is.                                                            |
| sector_size_bytes | 2 bytes | The sector size of the current machine. Not used at the moment, but hopefully...     |
| created_date_unix | 2 bytes | The Unix date when the database file was created.                                    |

| file type  | description                                                                            |
| ---------- | -------------------------------------------------------------------------------------- |
| 0: Primary | A primary data file containing data and indicies.                                      |
| 1: Log     | A append-only WAL (write-ahead-log) for quickly storing updates without writing pages. |

## Database Info Page

This page exists at page index 1; The second page.

This page describes the database this file represents.

The database info is stored in slot 1 of this page.

| col               | size      | description                                                                                                  |
| ----------------- | --------- | ------------------------------------------------------------------------------------------------------------ |
| database_name_len | 1 byte    | How long, in bytes, the database name is.                                                                    |
| database_name     | 128 bytes | The database name. Has a max length of 128 bytes, but the true length is described by the database_name_len. |
| database_version  | 1 byte    | The WackDB version.                                                                                          |
| database_id       | 2 bytes   | The unique ID of this database.                                                                              |
