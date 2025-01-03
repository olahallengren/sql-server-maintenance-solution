# SQL Server Maintenance Solution
[![licence badge]][licence]
[![stars badge]][stars]
[![forks badge]][forks]
[![issues badge]][issues]
[![bug report badge]][bug report]
[![feature request badge]][feature request]

## Notes regarding this Repo

This fork from https://github.com/olahallengren/sql-server-maintenance-solution is to address requests for support to use Amazon RDS for SQL Server.

This repo will accept pull requests related to that specific functionality but larger functionality requests should continue to be presented to the upstream repo as the owner of the original code. Unfortunately, we are a small team and do not have the bandwidth to review and accept PRs outside of this scope.

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

Supported versions: SQL Server 2008, SQL Server 2008 R2, SQL Server 2012, SQL Server 2014, SQL Server 2016, SQL Server 2017, SQL Server 2019, SQL Server 2022, Azure SQL Database, Azure SQL Managed Instance and [Supported Amazon RDS for SQL Server Versions](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_SQLServer.html#SQLServer.Concepts.General.VersionSupport)

## Documentation

<ul>
<li>Backup: https://ola.hallengren.com/sql-server-backup.html</li>
<li>Integrity Check: https://ola.hallengren.com/sql-server-integrity-check.html</li>
<li>Index and Statistics Maintenance: https://ola.hallengren.com/sql-server-index-and-statistics-maintenance.html</li>
<li>Amazon RDS for SQL Server Backup Integration: URL to be published shortly</li>
</ul>

[licence badge]:https://img.shields.io/badge/license-MIT-blue.svg
[stars badge]:https://img.shields.io/github/stars/amazon-contributing/aws-sql-server-maintenance-solution.svg
[forks badge]:https://img.shields.io/github/forks/amazon-contributing/aws-sql-server-maintenance-solution.svg
[issues badge]:https://img.shields.io/github/issues/amazon-contributing/aws-sql-server-maintenance-solution.svg
[bug report badge]:https://img.shields.io/github/issues/amazon-contributing/aws-sql-server-maintenance-solution/Bug%20Report.svg
[feature request badge]:https://img.shields.io/github/issues/amazon-contributing/aws-sql-server-maintenance-solution/Feature%20Request.svg

[licence]:https://github.com/olahallengren/sql-server-maintenance-solution/blob/master/LICENSE
[stars]:https://github.com/amazon-contributing/aws-sql-server-maintenance-solution/stargazers
[forks]:https://github.com/amazon-contributing/aws-sql-server-maintenance-solution/network
[issues]:https://github.com/amazon-contributing/aws-sql-server-maintenance-solution/issues
[bug report]:https://github.com/amazon-contributing/aws-sql-server-maintenance-solution/issues?q=is%3Aopen+is%3Aissue+label%3A%22Bug+Report%22
[feature request]:https://github.com/amazon-contributing/aws-sql-server-maintenance-solution/issues?q=is%3Aopen+is%3Aissue+label%3A%22Feature+Request%22

