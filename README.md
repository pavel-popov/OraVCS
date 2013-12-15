# OraVCS - put Oracle Database schema under VCS

## What?

Inspired by [OraDDL], my previous script for exporting Oracle schema as text files to put them under Version Control
System (git in my case), this is entirely rewritten and improved version.

It allows you to work with different VCSs as well as with different schemas and instances of Oracle Database from
single installed copy of OraVCS.


## Why?

Every project connected with database development should decide where to put source files - DDL-statements of Tables,
Views, Indexes and other objects and sources of Packages, Procedures, Functions and Types. I think that the best way
to working on project is to put this DDLs under Version Source Control system and treat that data like a source code.

This tool is written to do automatic upload current version of database objects, put them into directory and
commit to repository.


## How?


## License

See [License](license.txt).

