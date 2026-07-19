# SQL Server Maintenance Solution
[![licence badge]][licence]
[![stars badge]][stars]
[![forks badge]][forks]
[![issues badge]][issues]
[![bug report badge]][bug report]
[![SQL Server bug badge]][SQL Server bug]
[![feature request badge]][feature request]

## Getting Started

Download [MaintenanceSolution.sql](/MaintenanceSolution.sql).
This script creates all the objects and jobs that you need.

You can also download the objects as separate scripts:
 - [DatabaseBackup.sql](/DatabaseBackup.sql): Stored procedure to back up databases
 - [DatabaseIntegrityCheck.sql](/DatabaseIntegrityCheck.sql): Stored procedure to check the integrity of databases
 - [IndexOptimize.sql](/IndexOptimize.sql): Stored procedure to rebuild and reorganize indexes and update statistics
 - [CommandExecute.sql](/CommandExecute.sql): Stored procedure to execute and log commands
 - [CommandLog.sql](/CommandLog.sql): Table to log commands
 - [Queue.sql](/Queue.sql): Table for processing databases in parallel
 - [QueueDatabase.sql](/QueueDatabase.sql): Table for processing databases in parallel

Note that you always need CommandExecute; DatabaseBackup, DatabaseIntegrityCheck, and IndexOptimize use it.

When you update DatabaseBackup, DatabaseIntegrityCheck, or IndexOptimize, you should also update CommandExecute.

You need CommandLog if you are going to use the option to log commands to a table.

Supported versions: SQL Server 2017, SQL Server 2019, SQL Server 2022, SQL Server 2025, Azure SQL Database, and Azure SQL Managed Instance.

## Documentation

 - [SQL Server Backup](https://ola.hallengren.com/sql-server-backup.html)
 - [SQL Server Integrity Check](https://ola.hallengren.com/sql-server-integrity-check.html)
 - [SQL Server Index and Statistics Maintenance](https://ola.hallengren.com/sql-server-index-and-statistics-maintenance.html)
 - [Frequently Asked Questions](https://ola.hallengren.com/frequently-asked-questions.html)
 - [Version History](https://ola.hallengren.com/versions.html)

A copy of the documentation is also available in this repository: [docs](/docs).

[licence badge]:https://img.shields.io/badge/license-MIT-blue.svg
[stars badge]:https://img.shields.io/github/stars/olahallengren/sql-server-maintenance-solution.svg
[forks badge]:https://img.shields.io/github/forks/olahallengren/sql-server-maintenance-solution.svg
[issues badge]:https://img.shields.io/github/issues/olahallengren/sql-server-maintenance-solution.svg
[bug report badge]:https://img.shields.io/github/issues/olahallengren/sql-server-maintenance-solution/Bug%20Report.svg
[SQL Server bug badge]:https://img.shields.io/github/issues/olahallengren/sql-server-maintenance-solution/SQL%20Server%20Bug.svg
[feature request badge]:https://img.shields.io/github/issues/olahallengren/sql-server-maintenance-solution/Feature%20Request.svg

[licence]:https://github.com/olahallengren/sql-server-maintenance-solution/blob/master/LICENSE
[stars]:https://github.com/olahallengren/sql-server-maintenance-solution/stargazers
[forks]:https://github.com/olahallengren/sql-server-maintenance-solution/network
[issues]:https://github.com/olahallengren/sql-server-maintenance-solution/issues
[bug report]:https://github.com/olahallengren/sql-server-maintenance-solution/issues?q=is%3Aopen+is%3Aissue+label%3A%22Bug+Report%22
[SQL Server bug]:https://github.com/olahallengren/sql-server-maintenance-solution/issues?q=is%3Aopen+is%3Aissue+label%3A%22SQL+Server+Bug%22
[feature request]:https://github.com/olahallengren/sql-server-maintenance-solution/issues?q=is%3Aopen+is%3Aissue+label%3A%22Feature+Request%22
