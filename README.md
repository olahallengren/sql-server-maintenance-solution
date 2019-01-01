# SQL Server Maintenance Solution
[![licence badge]][licence]
[![stars badge]][stars]
[![forks badge]][forks]
[![issues badge]][issues]
[![bug_report badge]][bug_report]
[![feature_request badge]][feature_request]

## Getting Started

Download [MaintenanceSolution.sql](/MaintenanceSolution.sql).
This script creates all the objects and jobs that you need.

You can also download the objects as separate scripts:
 - [DatabaseBackup](/DatabaseBackup.sql): SQL Server Backup
 - [DatabaseIntegrityCheck](/DatabaseIntegrityCheck.sql): SQL Server Integrity Check
 - [IndexOptimize](/IndexOptimize.sql): SQL Server Index and Statistics Maintenance
 - [CommandExecute](/CommandExecute.sql): Stored procedure to execute and log commands
 - [CommandLog](/CommandLog.sql): Table to log commands

Note that you always need CommandExecute; DatabaseBackup, DatabaseIntegrityCheck, and IndexOptimize are using it.
You need CommandLog if you are going to use the option to log commands to a table.

Supported versions: SQL Server 2005, SQL Server 2008, SQL Server 2008 R2, SQL Server 2012, SQL Server 2014, SQL Server 2016, SQL Server 2017, Azure SQL Database, and Azure SQL Database Managed Instance

## Documentation

<ul>
<li>Backup: https://ola.hallengren.com/sql-server-backup.html</li>
<li>Integrity Check: https://ola.hallengren.com/sql-server-integrity-check.html</li>
<li>Index and Statistics Maintenance: https://ola.hallengren.com/sql-server-index-and-statistics-maintenance.html</li>
</ul>

[licence badge]:https://img.shields.io/badge/license-MIT-blue.svg
[stars badge]:https://img.shields.io/github/stars/olahallengren/sql-server-maintenance-solution.svg
[forks badge]:https://img.shields.io/github/forks/olahallengren/sql-server-maintenance-solution.svg
[issues badge]:https://img.shields.io/github/issues/olahallengren/sql-server-maintenance-solution.svg
[bug_report badge]:https://img.shields.io/github/issues/olahallengren/sql-server-maintenance-solution/bug_report.svg
[feature_request badge]:https://img.shields.io/github/issues/olahallengren/sql-server-maintenance-solution/feature_request.svg

[licence]:https://github.com/olahallengren/sql-server-maintenance-solution/blob/master/LICENSE
[stars]:https://github.com/olahallengren/sql-server-maintenance-solution/stargazers
[forks]:https://github.com/olahallengren/sql-server-maintenance-solution/network
[issues]:https://github.com/olahallengren/sql-server-maintenance-solution/issues
[bug_report]:https://github.com/olahallengren/sql-server-maintenance-solution/issues?q=is%3Aopen+is%3Aissue+label%3Abug_report
[feature_request]:https://github.com/olahallengren/sql-server-maintenance-solution/issues?q=is%3Aopen+is%3Aissue+label%3Afeature_request
