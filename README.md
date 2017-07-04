# SQL Server Maintenance Solution
[![licence badge]][licence]
[![stars badge]][stars]
[![forks badge]][forks]
[![issues badge]][issues]

Documentation link: [ola.hallengren.com](https://ola.hallengren.com)

The SQL Server Maintenance Solution comprises scripts for running backups, integrity checks, and index and statistics maintenance
on all editions of Microsoft SQL Server 2005, SQL Server 2008, SQL Server 2008 R2, SQL Server 2012, SQL Server 2014 and SQL Server 2016.
The solution is based on stored procedures, the sqlcmd utility, and SQL Server Agent jobs.
Ola Hallengren designed the solution for the most mission-critical environments, and it is used in many 
[organizations](https://ola.hallengren.com/organizations.html) around the world.
Numerous SQL Server community experts recommend the SQL Server Maintenance Solution, which has been a Gold winner in the
[2013](http://sqlmag.com/sql-server/best-free-sql-server-tool-2013), 
[2012](http://sqlmag.com/sql-server/2012-sql-server-pro-editors-best-and-community-choice-awards), 
[2011](http://sqlmag.com/sql-server/2011-sql-server-magazine-editors-best-and-community-choice-awards), 
and [2010](http://sqlmag.com/sql-server/2010-sql-server-magazine-editors-best-and-community-choice-awards)
SQL Server Magazine Awards.
The SQL Server Maintenance Solution is free with [MIT License](/LICENSE)


## Getting Started

Download [MaintenanceSolution.sql](/MaintenanceSolution.sql).
This script creates all the objects and jobs that you need.

You can also download the objects as separate scripts:
 - [DatabaseBackup](/DatabaseBackup.sql): SQL Server Backup
 - [DatabaseIntegrityCheck](/DatabaseIntegrityCheck.sql): SQL Server Integrity Check
 - [IndexOptimize](/IndexOptimize.sql): SQL Server Index and Statistics Maintenance
 - [CommandExecute](/CommandExecute.sql): Stored procedure to execute and log commands
 - [CommandLog](/CommandLog.sql): Table to log commands

Note that you always need CommandExecute, DatabaseBackup, DatabaseIntegrityCheck, and IndexOptimize are using it.
You need CommandLog if you are going to use the option to log commands to a table.


[licence badge]:https://img.shields.io/badge/license-MIT-blue.svg
[stars badge]:https://img.shields.io/github/stars/olahallengren/sql-server-maintenance-solution.svg
[forks badge]:https://img.shields.io/github/forks/olahallengren/sql-server-maintenance-solution.svg
[issues badge]:https://img.shields.io/github/issues/olahallengren/sql-server-maintenance-solution.svg
[contributors_badge]:https://img.shields.io/github/contributors/olahallengren/sql-server-maintenance-solution.svg

[licence]:https://github.com/olahallengren/sql-server-maintenance-solutionblob/master/LICENSE.md
[stars]:https://github.com/olahallengren/sql-server-maintenance-solutionstargazers
[forks]:https://github.com/olahallengren/sql-server-maintenance-solutionnetwork
[issues]:https://github.com/olahallengren/sql-server-maintenance-solutionissues
[contributors]:https://github.com/olahallengren/sql-server-maintenance-solutiongraphs/contributors
