/*

SQL Server Maintenance Solution - SQL Server 2017, SQL Server 2019, SQL Server 2022, SQL Server 2025, and Azure SQL Managed Instance

Backup: https://ola.hallengren.com/sql-server-backup.html
Integrity Check: https://ola.hallengren.com/sql-server-integrity-check.html
Index and Statistics Maintenance: https://ola.hallengren.com/sql-server-index-and-statistics-maintenance.html

License: https://ola.hallengren.com/license.html

GitHub: https://github.com/olahallengren/sql-server-maintenance-solution

Version:                    

You can contact me by e-mail at ola@hallengren.com.

Ola Hallengren
https://ola.hallengren.com

*/

USE [master] -- Specify the database in which the objects will be created.

SET NOCOUNT ON

DECLARE @CreateJobs nvarchar(max)          = 'Y'         -- Specify whether jobs should be created.
DECLARE @BackupDirectory nvarchar(max)     = NULL        -- Specify the backup root directory. If no directory is specified, the default backup directory is used.
DECLARE @BackupURL nvarchar(max)           = NULL        -- Specify the backup root URL.
DECLARE @CleanupTime int                   = NULL        -- Time in hours, after which backup files are deleted. If no time is specified, then no backup files are deleted.
DECLARE @OutputFileDirectory nvarchar(max) = NULL        -- Specify the output file directory. If no directory is specified, then the SQL Server error log directory is used.
DECLARE @LogToTable nvarchar(max)          = 'Y'         -- Log commands to a table.

DECLARE @ErrorMessage nvarchar(max)

IF IS_SRVROLEMEMBER('sysadmin') = 0 AND NOT (EXISTS (SELECT * FROM sys.databases WHERE [name] = 'rdsadmin') AND SUSER_SNAME(0x01) = 'rdsa')
BEGIN
  SET @ErrorMessage = 'You need to be a member of the SysAdmin server role to install the SQL Server Maintenance Solution.'
  RAISERROR(@ErrorMessage,16,1) WITH NOWAIT
END

IF @BackupDirectory IS NOT NULL AND @BackupURL IS NOT NULL
BEGIN
  SET @ErrorMessage = 'Only one of the variables @BackupDirectory and @BackupURL can be set.'
  RAISERROR(@ErrorMessage,16,1) WITH NOWAIT
END

IF @BackupURL IS NOT NULL AND @CleanupTime IS NOT NULL
BEGIN
  SET @ErrorMessage = 'The variable @CleanupTime is not supported with backup to URL.'
  RAISERROR(@ErrorMessage,16,1) WITH NOWAIT
END

IF OBJECT_ID('tempdb..#Config') IS NOT NULL DROP TABLE #Config

CREATE TABLE #Config ([Name] nvarchar(max),
                      [Value] nvarchar(max))

INSERT INTO #Config ([Name], [Value]) VALUES('CreateJobs', @CreateJobs)
INSERT INTO #Config ([Name], [Value]) VALUES('BackupDirectory', @BackupDirectory)
INSERT INTO #Config ([Name], [Value]) VALUES('BackupURL', @BackupURL)
INSERT INTO #Config ([Name], [Value]) VALUES('CleanupTime', @CleanupTime)
INSERT INTO #Config ([Name], [Value]) VALUES('OutputFileDirectory', @OutputFileDirectory)
INSERT INTO #Config ([Name], [Value]) VALUES('LogToTable', @LogToTable)
INSERT INTO #Config ([Name], [Value]) VALUES('DatabaseName', DB_NAME())
GO