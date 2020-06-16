# Custom Solution
Main differences are in the DatabaseIntegrityCheck script and added the CheckTableObjects table

Added Parameters:<br>
 - @Resumable - valid values are 'Y' or 'N'
    - defaults to 'N'
    - Must be used in conjunction with @TimeLimit
    - Can only be used if the CheckTable command is specified
    - Runs CheckAlloc, then CheckCatalog, then CheckTable
      - This is a different order than original Ola, which was CheckAlloc, CheckTable, CheckCatalog
    - Will only run CHECKTABLE checks once per day, as it makes sure that "dbo.CheckTableObjects.LastCheckDate <> CAST(@StartTime as date)"
      - This is to prevent a loop of it just going through the same tables over and over during the time window
      - To reset either Truncate the CheckTableObjects table, or update LastCheckDate to an older date
        - ```UPDATE dbo.CheckTableObjects SET LastCheckDate = DATEADD(DAY, -1, LastCheckDate) ```

Example:
```sql    
EXECUTE [dbo].[DatabaseIntegrityCheck] @Databases = 'ALL_DATABASES', @CheckCommands = 'CHECKALLOC,CHECKCATALOG,CHECKTABLE', @TimeLimit = 18000, @LogToTable = 'Y', @Execute = 'Y', @Resumable = 'Y'
```
# SQL Server Maintenance Solution
[![licence badge]][licence]
[![stars badge]][stars]
[![forks badge]][forks]
[![issues badge]][issues]
[![bug report badge]][bug report]
[![feature request badge]][feature request]
[![question badge]][question]

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

Supported versions: SQL Server 2008, SQL Server 2008 R2, SQL Server 2012, SQL Server 2014, SQL Server 2016, SQL Server 2017, SQL Server 2019, Azure SQL Database, and Azure SQL Database Managed Instance

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
[bug report badge]:https://img.shields.io/github/issues/olahallengren/sql-server-maintenance-solution/Bug%20Report.svg
[feature request badge]:https://img.shields.io/github/issues/olahallengren/sql-server-maintenance-solution/Feature%20Request.svg
[question badge]:https://img.shields.io/github/issues/olahallengren/sql-server-maintenance-solution/Question.svg

[licence]:https://github.com/olahallengren/sql-server-maintenance-solution/blob/master/LICENSE
[stars]:https://github.com/olahallengren/sql-server-maintenance-solution/stargazers
[forks]:https://github.com/olahallengren/sql-server-maintenance-solution/network
[issues]:https://github.com/olahallengren/sql-server-maintenance-solution/issues
[bug report]:https://github.com/olahallengren/sql-server-maintenance-solution/issues?q=is%3Aopen+is%3Aissue+label%3A%22Bug+Report%22
[feature request]:https://github.com/olahallengren/sql-server-maintenance-solution/issues?q=is%3Aopen+is%3Aissue+label%3A%22Feature+Request%22
[question]:https://github.com/olahallengren/sql-server-maintenance-solution/issues?q=is%3Aopen+is%3Aissue+label%3AQuestion
