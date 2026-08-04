IF (SELECT [Value] FROM #Config WHERE Name = 'CreateJobs') = 'Y'
    AND SERVERPROPERTY('EngineEdition') NOT IN(4, 5)
    AND (IS_SRVROLEMEMBER('sysadmin') = 1 OR (EXISTS (SELECT * FROM sys.databases WHERE [name] = 'rdsadmin') AND SUSER_SNAME(0x01) = 'rdsa'))
    AND NOT (EXISTS (SELECT * FROM #Config WHERE Name = 'BackupDirectory' AND [Value] IS NOT NULL) AND EXISTS (SELECT * FROM #Config WHERE Name = 'BackupURL' AND [Value] IS NOT NULL))
    AND NOT (EXISTS (SELECT * FROM #Config WHERE Name = 'BackupURL' AND [Value] IS NOT NULL) AND EXISTS (SELECT * FROM #Config WHERE Name = 'CleanupTime' AND [Value] IS NOT NULL))
BEGIN

  DECLARE @BackupDirectory nvarchar(max)
  DECLARE @BackupURL nvarchar(max)
  DECLARE @CleanupTime int
  DECLARE @OutputFileDirectory nvarchar(max)
  DECLARE @LogToTable nvarchar(max)
  DECLARE @DatabaseName nvarchar(max)

  DECLARE @HostPlatform nvarchar(max)
  DECLARE @DirectorySeparator nvarchar(max)
  DECLARE @LogDirectory nvarchar(max)

  DECLARE @TokenServer nvarchar(max)
  DECLARE @TokenJobName nvarchar(max)
  DECLARE @TokenStepID nvarchar(max)
  DECLARE @TokenStepName nvarchar(max)
  DECLARE @TokenDate nvarchar(max)
  DECLARE @TokenTime nvarchar(max)
  DECLARE @TokenLogDirectory nvarchar(max)

  DECLARE @JobDescription nvarchar(max)
  DECLARE @JobCategory nvarchar(max)
  DECLARE @JobOwner nvarchar(max)

  DECLARE @Jobs TABLE (JobID int IDENTITY,
                       [Name] nvarchar(max),
                       CommandTSQL nvarchar(max),
                       CommandCmdExec nvarchar(max),
                       DatabaseName varchar(max),
                       Selected bit DEFAULT 0,
                       Completed bit DEFAULT 0)

  DECLARE @CurrentJobID int
  DECLARE @CurrentJobName nvarchar(max)
  DECLARE @CurrentCommandTSQL nvarchar(max)
  DECLARE @CurrentCommandCmdExec nvarchar(max)
  DECLARE @CurrentDatabaseName nvarchar(max)

  DECLARE @CurrentJobStepCommand nvarchar(max)
  DECLARE @CurrentJobStepSubSystem nvarchar(max)
  DECLARE @CurrentJobStepDatabaseName nvarchar(max)
  DECLARE @CurrentOutputFileName nvarchar(max)

  DECLARE @AmazonRDS bit = CASE WHEN SERVERPROPERTY('EngineEdition') IN (5, 8) THEN 0 WHEN EXISTS (SELECT * FROM sys.databases WHERE [name] = 'rdsadmin') AND SUSER_SNAME(0x01) = 'rdsa' THEN 1 ELSE 0 END

  SELECT @HostPlatform = host_platform
  FROM sys.dm_os_host_info

  SELECT @DirectorySeparator = CASE
  WHEN @HostPlatform = 'Windows' THEN '\'
  WHEN @HostPlatform = 'Linux' THEN '/'
  END

  SET @TokenServer = '$' + '(ESCAPE_SQUOTE(SRVR))'
  SET @TokenStepID = '$' + '(ESCAPE_SQUOTE(STEPID))'
  SET @TokenDate = '$' + '(ESCAPE_SQUOTE(DATE))'
  SET @TokenTime = '$' + '(ESCAPE_SQUOTE(TIME))'
  SET @TokenJobName = '$' + '(ESCAPE_SQUOTE(JOBNAME))'
  SET @TokenStepName = '$' + '(ESCAPE_SQUOTE(STEPNAME))'

  IF @HostPlatform = 'Windows'
  BEGIN
    SET @TokenLogDirectory = '$' + '(ESCAPE_SQUOTE(SQLLOGDIR))'
  END

  SELECT @BackupDirectory = Value
  FROM #Config
  WHERE [Name] = 'BackupDirectory'

  SELECT @BackupURL = Value
  FROM #Config
  WHERE [Name] = 'BackupURL'

  SELECT @CleanupTime = Value
  FROM #Config
  WHERE [Name] = 'CleanupTime'

  SELECT @OutputFileDirectory = Value
  FROM #Config
  WHERE [Name] = 'OutputFileDirectory'

  SELECT @LogToTable = Value
  FROM #Config
  WHERE [Name] = 'LogToTable'

  SELECT @DatabaseName = Value
  FROM #Config
  WHERE [Name] = 'DatabaseName'

  SELECT @LogDirectory = [path]
  FROM sys.dm_os_server_diagnostics_log_configurations

  IF @OutputFileDirectory IS NOT NULL AND RIGHT(@OutputFileDirectory,1) = @DirectorySeparator
  BEGIN
    SET @OutputFileDirectory = LEFT(@OutputFileDirectory, LEN(@OutputFileDirectory) - 1)
  END

  IF @LogDirectory IS NOT NULL AND RIGHT(@LogDirectory,1) = @DirectorySeparator
  BEGIN
    SET @LogDirectory = LEFT(@LogDirectory, LEN(@LogDirectory) - 1)
  END

  SET @JobDescription = 'Source: https://ola.hallengren.com'
  SET @JobCategory = 'Database Maintenance'

  IF @AmazonRDS = 0
  BEGIN
    SET @JobOwner = SUSER_SNAME(0x01)
  END

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName)
  VALUES('DatabaseBackup - SYSTEM_DATABASES - FULL',
         'EXECUTE [dbo].[DatabaseBackup]' + CHAR(13) + CHAR(10) + '@Databases = ''SYSTEM_DATABASES'',' + CHAR(13) + CHAR(10) + CASE WHEN @BackupURL IS NOT NULL THEN '@URL = N''' + REPLACE(@BackupURL,'''','''''') + '''' ELSE '@Directory = ' + ISNULL('N''' + REPLACE(@BackupDirectory,'''','''''') + '''','NULL') END + ',' + CHAR(13) + CHAR(10) + '@BackupType = ''FULL'',' + CHAR(13) + CHAR(10) + '@Verify = ''Y'',' + CHAR(13) + CHAR(10) + '@CleanupTime = ' + ISNULL(CAST(@CleanupTime AS nvarchar),'NULL') + ',' + CHAR(13) + CHAR(10) + '@Checksum = ''Y'',' + CHAR(13) + CHAR(10) + '@LogToTable = ''' + @LogToTable + '''',
         @DatabaseName)

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName)
  VALUES('DatabaseBackup - USER_DATABASES - DIFF',
         'EXECUTE [dbo].[DatabaseBackup]' + CHAR(13) + CHAR(10) + '@Databases = ''USER_DATABASES'',' + CHAR(13) + CHAR(10) + CASE WHEN @BackupURL IS NOT NULL THEN '@URL = N''' + REPLACE(@BackupURL,'''','''''') + '''' ELSE '@Directory = ' + ISNULL('N''' + REPLACE(@BackupDirectory,'''','''''') + '''','NULL') END + ',' + CHAR(13) + CHAR(10) + '@BackupType = ''DIFF'',' + CHAR(13) + CHAR(10) + '@Verify = ''Y'',' + CHAR(13) + CHAR(10) + '@CleanupTime = ' + ISNULL(CAST(@CleanupTime AS nvarchar),'NULL') + ',' + CHAR(13) + CHAR(10) + '@Checksum = ''Y'',' + CHAR(13) + CHAR(10) + '@LogToTable = ''' + @LogToTable + '''',
          @DatabaseName)

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName)
  VALUES('DatabaseBackup - USER_DATABASES - FULL',
         'EXECUTE [dbo].[DatabaseBackup]' + CHAR(13) + CHAR(10) + '@Databases = ''USER_DATABASES'',' + CHAR(13) + CHAR(10) + CASE WHEN @BackupURL IS NOT NULL THEN '@URL = N''' + REPLACE(@BackupURL,'''','''''') + '''' ELSE '@Directory = ' + ISNULL('N''' + REPLACE(@BackupDirectory,'''','''''') + '''','NULL') END + ',' + CHAR(13) + CHAR(10) + '@BackupType = ''FULL'',' + CHAR(13) + CHAR(10) + '@Verify = ''Y'',' + CHAR(13) + CHAR(10) + '@CleanupTime = ' + ISNULL(CAST(@CleanupTime AS nvarchar),'NULL') + ',' + CHAR(13) + CHAR(10) + '@Checksum = ''Y'',' + CHAR(13) + CHAR(10) + '@LogToTable = ''' + @LogToTable + '''',
         @DatabaseName)

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName)
  VALUES('DatabaseBackup - USER_DATABASES - LOG',
         'EXECUTE [dbo].[DatabaseBackup]' + CHAR(13) + CHAR(10) + '@Databases = ''USER_DATABASES'',' + CHAR(13) + CHAR(10) + CASE WHEN @BackupURL IS NOT NULL THEN '@URL = N''' + REPLACE(@BackupURL,'''','''''') + '''' ELSE '@Directory = ' + ISNULL('N''' + REPLACE(@BackupDirectory,'''','''''') + '''','NULL') END + ',' + CHAR(13) + CHAR(10) + '@BackupType = ''LOG'',' + CHAR(13) + CHAR(10) + '@Verify = ''Y'',' + CHAR(13) + CHAR(10) + '@CleanupTime = ' + ISNULL(CAST(@CleanupTime AS nvarchar),'NULL') + ',' + CHAR(13) + CHAR(10) + '@Checksum = ''Y'',' + CHAR(13) + CHAR(10) + '@LogToTable = ''' + @LogToTable + '''',
         @DatabaseName)

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName)
  VALUES('DatabaseIntegrityCheck - SYSTEM_DATABASES',
         'EXECUTE [dbo].[DatabaseIntegrityCheck]' + CHAR(13) + CHAR(10) + '@Databases = ''SYSTEM_DATABASES'',' + CHAR(13) + CHAR(10) + '@NoInformationalMessages = ''Y'',' + CHAR(13) + CHAR(10) + '@LogToTable = ''' + @LogToTable + '''',
         @DatabaseName)

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName)
  VALUES('DatabaseIntegrityCheck - USER_DATABASES',
         'EXECUTE [dbo].[DatabaseIntegrityCheck]' + CHAR(13) + CHAR(10) + '@Databases = ''USER_DATABASES'',' + CHAR(13) + CHAR(10) + '@NoInformationalMessages = ''Y'',' + CHAR(13) + CHAR(10) + '@LogToTable = ''' + @LogToTable + '''',
         @DatabaseName)

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName)
  VALUES('IndexOptimize - USER_DATABASES',
         'EXECUTE [dbo].[IndexOptimize]' + CHAR(13) + CHAR(10) + '@Databases = ''USER_DATABASES'',' + CHAR(13) + CHAR(10) + '@LogToTable = ''' + @LogToTable + '''',
         @DatabaseName)

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName)
  VALUES('sp_delete_backuphistory',
         'DECLARE @CleanupDate datetime' + CHAR(13) + CHAR(10) + 'SET @CleanupDate = DATEADD(dd,-30,GETDATE())' + CHAR(13) + CHAR(10) + 'EXECUTE dbo.sp_delete_backuphistory @oldest_date = @CleanupDate',
         'msdb')

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName)
  VALUES('sp_purge_jobhistory',
         'DECLARE @CleanupDate datetime' + CHAR(13) + CHAR(10) + 'SET @CleanupDate = DATEADD(dd,-30,GETDATE())' + CHAR(13) + CHAR(10) + 'EXECUTE dbo.sp_purge_jobhistory @oldest_date = @CleanupDate',
         'msdb')

  INSERT INTO @Jobs ([Name], CommandTSQL, DatabaseName)
  VALUES('CommandLog Cleanup',
         'DELETE FROM [dbo].[CommandLog]' + CHAR(13) + CHAR(10) + 'WHERE StartTime < DATEADD(dd,-30,GETDATE())',
         @DatabaseName)

  INSERT INTO @Jobs ([Name], CommandCmdExec)
  VALUES('Output File Cleanup',
         'powershell.exe -NoProfile -Command "Get-ChildItem -LiteralPath ''' + REPLACE(COALESCE(@OutputFileDirectory,@TokenLogDirectory,@LogDirectory),'''','''''') + ''' -Filter ''*_*_*_*.txt'' -File | Where-Object { $_.LastWriteTime.Date -le (Get-Date).Date.AddDays(-30) } | ForEach-Object { Write-Output (''del '' + $_.FullName); Remove-Item -LiteralPath $_.FullName }"')

  IF @AmazonRDS = 1
  BEGIN
   UPDATE @Jobs
   SET Selected = 1
   WHERE [Name] IN('DatabaseIntegrityCheck - USER_DATABASES','IndexOptimize - USER_DATABASES','CommandLog Cleanup')
  END
  ELSE IF SERVERPROPERTY('EngineEdition') = 8
  BEGIN
   UPDATE @Jobs
   SET Selected = 1
   WHERE [Name] IN('DatabaseIntegrityCheck - SYSTEM_DATABASES','DatabaseIntegrityCheck - USER_DATABASES','IndexOptimize - USER_DATABASES','CommandLog Cleanup','sp_delete_backuphistory','sp_purge_jobhistory')
  END
  ELSE IF @HostPlatform = 'Windows'
  BEGIN
   UPDATE @Jobs
   SET Selected = 1
  END
  ELSE IF @HostPlatform = 'Linux'
  BEGIN
   UPDATE @Jobs
   SET Selected = 1
   WHERE CommandTSQL IS NOT NULL
  END

  WHILE EXISTS (SELECT * FROM @Jobs WHERE Completed = 0 AND Selected = 1)
  BEGIN
    SELECT TOP 1 @CurrentJobID = JobID,
                 @CurrentJobName = [Name],
                 @CurrentCommandTSQL = CommandTSQL,
                 @CurrentCommandCmdExec = CommandCmdExec,
                 @CurrentDatabaseName = DatabaseName
    FROM @Jobs
    WHERE Completed = 0
    AND Selected = 1
    ORDER BY JobID ASC

    IF @CurrentCommandTSQL IS NOT NULL
    BEGIN
      SET @CurrentJobStepSubSystem = 'TSQL'
      SET @CurrentJobStepCommand = @CurrentCommandTSQL
      SET @CurrentJobStepDatabaseName = @CurrentDatabaseName
    END
    ELSE IF @CurrentCommandCmdExec IS NOT NULL AND @HostPlatform = 'Windows'
    BEGIN
      SET @CurrentJobStepSubSystem = 'CMDEXEC'
      SET @CurrentJobStepCommand = @CurrentCommandCmdExec
      SET @CurrentJobStepDatabaseName = NULL
    END

    IF @AmazonRDS = 0 AND SERVERPROPERTY('EngineEdition') <> 8
    BEGIN
      SET @CurrentOutputFileName = COALESCE(@OutputFileDirectory,@TokenLogDirectory,@LogDirectory) + @DirectorySeparator + @TokenJobName + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
      IF LEN(@CurrentOutputFileName) > 200 SET @CurrentOutputFileName = NULL
    END

    IF @CurrentJobStepSubSystem IS NOT NULL AND @CurrentJobStepCommand IS NOT NULL AND NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @CurrentJobName)
    BEGIN
      EXECUTE msdb.dbo.sp_add_job @job_name = @CurrentJobName, @description = @JobDescription, @category_name = @JobCategory, @owner_login_name = @JobOwner
      EXECUTE msdb.dbo.sp_add_jobstep @job_name = @CurrentJobName, @step_name = @CurrentJobName, @subsystem = @CurrentJobStepSubSystem, @command = @CurrentJobStepCommand, @output_file_name = @CurrentOutputFileName, @database_name = @CurrentJobStepDatabaseName
      EXECUTE msdb.dbo.sp_add_jobserver @job_name = @CurrentJobName
    END

    UPDATE Jobs
    SET Completed = 1
    FROM @Jobs Jobs
    WHERE JobID = @CurrentJobID

    SET @CurrentJobID = NULL
    SET @CurrentJobName = NULL
    SET @CurrentCommandTSQL = NULL
    SET @CurrentCommandCmdExec = NULL
    SET @CurrentDatabaseName = NULL
    SET @CurrentJobStepCommand = NULL
    SET @CurrentJobStepSubSystem = NULL
    SET @CurrentJobStepDatabaseName = NULL
    SET @CurrentOutputFileName = NULL

  END

END
GO

DECLARE @job_id uniqueidentifier
DECLARE @step_id int
DECLARE @command nvarchar(max)
DECLARE @AmazonRDS bit = CASE WHEN SERVERPROPERTY('EngineEdition') IN (5, 8) THEN 0 WHEN EXISTS (SELECT * FROM sys.databases WHERE [name] = 'rdsadmin') AND SUSER_SNAME(0x01) = 'rdsa' THEN 1 ELSE 0 END

IF @AmazonRDS = 0
BEGIN

  DECLARE JobCursor CURSOR LOCAL FAST_FORWARD FOR SELECT job_id, step_id, command FROM msdb.dbo.sysjobsteps WHERE command LIKE '%DatabaseBackup%@CheckSum%' COLLATE SQL_Latin1_General_CP1_CS_AS OR command LIKE '%DatabaseBackup%@ModificationLevel%' OR command LIKE '%DatabaseBackup%@LogSizeSinceLastLogBackup%' OR command LIKE '%DatabaseBackup%@TimeSinceLastLogBackup%'

  OPEN JobCursor

  FETCH JobCursor INTO @job_id, @step_id, @command

  WHILE @@FETCH_STATUS = 0
  BEGIN
    SET @command = REPLACE(@command, '@CheckSum', '@Checksum')
    SET @command = REPLACE(@command, '@ModificationLevel', '@MinModificationLevel')
    SET @command = REPLACE(@command, '@LogSizeSinceLastLogBackup', '@MinLogSizeSinceLastLogBackup')
    SET @command = REPLACE(@command, '@TimeSinceLastLogBackup', '@MinTimeSinceLastLogBackup')

    EXECUTE msdb.dbo.sp_update_jobstep @job_id = @job_id, @step_id = @step_id, @command = @command

    FETCH NEXT FROM JobCursor INTO @job_id, @step_id, @command
  END

  CLOSE JobCursor

  DEALLOCATE JobCursor
END
GO
