/*

SQL Server Maintenance Solution - Azure SQL Database

Integrity Check: https://ola.hallengren.com/sql-server-integrity-check.html
Index and Statistics Maintenance: https://ola.hallengren.com/sql-server-index-and-statistics-maintenance.html

License: https://ola.hallengren.com/license.html

GitHub: https://github.com/olahallengren/sql-server-maintenance-solution

Version: 2026-07-21 14:38:46

You can contact me by e-mail at ola@hallengren.com.

Ola Hallengren
https://ola.hallengren.com

*/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CommandLog]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[CommandLog](
  [ID] [int] IDENTITY(1,1) NOT NULL,
  [DatabaseName] [sysname] NULL,
  [SchemaName] [sysname] NULL,
  [ObjectName] [sysname] NULL,
  [ObjectType] [char](2) NULL,
  [IndexName] [sysname] NULL,
  [IndexType] [tinyint] NULL,
  [StatisticsName] [sysname] NULL,
  [PartitionNumber] [int] NULL,
  [ExtendedInfo] [xml] NULL,
  [Command] [nvarchar](max) NOT NULL,
  [CommandType] [nvarchar](60) NOT NULL,
  [StartTime] [datetime2](7) NOT NULL,
  [EndTime] [datetime2](7) NULL,
  [ErrorNumber] [int] NULL,
  [ErrorMessage] [nvarchar](max) NULL,
 CONSTRAINT [PK_CommandLog] PRIMARY KEY CLUSTERED
(
  [ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
)
END
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CommandExecute]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[CommandExecute] AS'
END
GO
ALTER PROCEDURE [dbo].[CommandExecute]

@DatabaseContext nvarchar(max),
@Command nvarchar(max),
@CommandType nvarchar(max),
@Mode int,
@Comment nvarchar(max) = NULL,
@DatabaseName nvarchar(max) = NULL,
@SchemaName nvarchar(max) = NULL,
@ObjectName nvarchar(max) = NULL,
@ObjectType nvarchar(max) = NULL,
@IndexName nvarchar(max) = NULL,
@IndexType int = NULL,
@StatisticsName nvarchar(max) = NULL,
@PartitionNumber int = NULL,
@EncryptionKey nvarchar(max) = NULL,
@EncryptionKeyPlaceholder nvarchar(max) = NULL,
@ExtendedInfo xml = NULL,
@LockMessageSeverity int = 16,
@ExecuteAsUser nvarchar(max) = NULL,
@LogToTable nvarchar(max),
@Execute nvarchar(max)

AS

BEGIN

  ----------------------------------------------------------------------------------------------------
  --// Source:  https://ola.hallengren.com                                                        //--
  --// License: https://ola.hallengren.com/license.html                                           //--
  --// GitHub:  https://github.com/olahallengren/sql-server-maintenance-solution                  //--
  --// Version: 2026-07-21 14:38:46                                                               //--
  ----------------------------------------------------------------------------------------------------

  SET NOCOUNT ON

  DECLARE @StartMessage nvarchar(max)
  DECLARE @EndMessage nvarchar(max)
  DECLARE @ErrorMessage nvarchar(max)
  DECLARE @ErrorMessageOriginal nvarchar(max)
  DECLARE @Severity int

  DECLARE @Errors TABLE (ID int IDENTITY PRIMARY KEY,
                         [Message] nvarchar(max) NOT NULL,
                         Severity int NOT NULL,
                         [State] int)

  DECLARE @CurrentMessage nvarchar(max)
  DECLARE @CurrentSeverity int
  DECLARE @CurrentState int

  DECLARE @sp_executesql nvarchar(max) = QUOTENAME(@DatabaseContext) + '.sys.sp_executesql'

  DECLARE @StartTime datetime2
  DECLARE @EndTime datetime2

  DECLARE @ID int

  DECLARE @Error int = 0
  DECLARE @ReturnCode int = 0

  DECLARE @EmptyLine nvarchar(max) = CHAR(9)

  DECLARE @RevertCommand nvarchar(max)

  DECLARE @CommandMasked nvarchar(max)

  ----------------------------------------------------------------------------------------------------
  --// Check core requirements                                                                    //--
  ----------------------------------------------------------------------------------------------------

  IF NOT (SELECT uses_ansi_nulls FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('ANSI_NULLS has to be set to ON for the stored procedure.', 16, 1)
  END

  IF NOT (SELECT uses_quoted_identifier FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('QUOTED_IDENTIFIER has to be set to ON for the stored procedure.', 16, 1)
  END

  IF @LogToTable = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandLog')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The table CommandLog is missing. Download https://ola.hallengren.com/scripts/CommandLog.sql.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------
  --// Check input parameters                                                                     //--
  ----------------------------------------------------------------------------------------------------

  IF @DatabaseContext IS NULL OR NOT EXISTS (SELECT * FROM sys.databases WHERE name = @DatabaseContext)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @DatabaseContext is not supported.', 16, 1)
  END

  IF @Command IS NULL OR @Command = ''
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @Command is not supported.', 16, 1)
  END

  IF @CommandType IS NULL OR @CommandType = '' OR LEN(@CommandType) > 60
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @CommandType is not supported.', 16, 1)
  END

  IF @Mode NOT IN(1,2) OR @Mode IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @Mode is not supported.', 16, 1)
  END

  IF (@EncryptionKey IS NULL AND @EncryptionKeyPlaceholder IS NOT NULL) OR (@EncryptionKey IS NOT NULL AND @EncryptionKeyPlaceholder IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The parameters @EncryptionKey and @EncryptionKeyPlaceholder must be specified together.', 16, 1)
  END

  IF @LockMessageSeverity NOT IN(10,16) OR @LockMessageSeverity IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @LockMessageSeverity is not supported.', 16, 1)
  END

  IF LEN(@ExecuteAsUser) > 128
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @ExecuteAsUser is not supported.', 16, 1)
  END

  IF @LogToTable NOT IN('Y','N') OR @LogToTable IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @LogToTable is not supported.', 16, 1)
  END

  IF @Execute NOT IN('Y','N') OR @Execute IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @Execute is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------
  --// Raise errors                                                                               //--
  ----------------------------------------------------------------------------------------------------

  DECLARE ErrorCursor CURSOR LOCAL FAST_FORWARD FOR SELECT [Message], Severity, [State] FROM @Errors ORDER BY [ID] ASC

  OPEN ErrorCursor

  FETCH ErrorCursor INTO @CurrentMessage, @CurrentSeverity, @CurrentState

  WHILE @@FETCH_STATUS = 0
  BEGIN
    RAISERROR('%s', @CurrentSeverity, @CurrentState, @CurrentMessage) WITH NOWAIT
    RAISERROR(@EmptyLine, 10, 1) WITH NOWAIT

    FETCH NEXT FROM ErrorCursor INTO @CurrentMessage, @CurrentSeverity, @CurrentState
  END

  CLOSE ErrorCursor

  DEALLOCATE ErrorCursor

  IF EXISTS (SELECT * FROM @Errors WHERE Severity >= 16)
  BEGIN
    SET @ReturnCode = 50000
    GOTO ReturnCode
  END

  ----------------------------------------------------------------------------------------------------
  --// Execute as user                                                                            //--
  ----------------------------------------------------------------------------------------------------

  IF @ExecuteAsUser IS NOT NULL
  BEGIN
    SET @Command = 'EXECUTE AS USER = ''' + REPLACE(@ExecuteAsUser,'''','''''') + '''; ' + @Command + '; REVERT;'

    SET @RevertCommand = 'REVERT'
  END

  ----------------------------------------------------------------------------------------------------
  --// Mask encryption key                                                                        //--
  ----------------------------------------------------------------------------------------------------

  SET @CommandMasked = CASE WHEN @EncryptionKeyPlaceholder IS NULL THEN @Command ELSE REPLACE(@Command,@EncryptionKeyPlaceholder,'********') END

  SET @Command = CASE WHEN @EncryptionKeyPlaceholder IS NULL THEN @Command ELSE REPLACE(@Command,@EncryptionKeyPlaceholder,REPLACE(ISNULL(@EncryptionKey,''),'''','''''')) END

  ----------------------------------------------------------------------------------------------------
  --// Log initial information                                                                    //--
  ----------------------------------------------------------------------------------------------------

  SET @StartTime = SYSDATETIME()

  SET @StartMessage = 'Date and time: ' + CONVERT(nvarchar(max),@StartTime,120)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Database context: ' + QUOTENAME(@DatabaseContext)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Command: ' + @CommandMasked
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  IF @Comment IS NOT NULL
  BEGIN
    SET @StartMessage = 'Comment: ' + @Comment
    RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT
  END

  IF @LogToTable = 'Y'
  BEGIN
    INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, IndexName, IndexType, StatisticsName, PartitionNumber, ExtendedInfo, CommandType, Command, StartTime)
    VALUES (@DatabaseName, @SchemaName, @ObjectName, @ObjectType, @IndexName, @IndexType, @StatisticsName, @PartitionNumber, @ExtendedInfo, @CommandType, @CommandMasked, @StartTime)

    SET @ID = SCOPE_IDENTITY()
  END

  ----------------------------------------------------------------------------------------------------
  --// Execute command                                                                            //--
  ----------------------------------------------------------------------------------------------------

  IF @Mode = 1 AND @Execute = 'Y'
  BEGIN
    EXECUTE @sp_executesql @stmt = @Command
    SET @Error = @@ERROR
    SET @ReturnCode = @Error
  END

  IF @Mode = 2 AND @Execute = 'Y'
  BEGIN
    BEGIN TRY
      EXECUTE @sp_executesql @stmt = @Command
    END TRY
    BEGIN CATCH
      SET @Error = ERROR_NUMBER()
      SET @ErrorMessageOriginal = ERROR_MESSAGE()

      SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar(max)) + ', ' + ISNULL(ERROR_MESSAGE(),'')
      SET @Severity = CASE WHEN ERROR_NUMBER() IN(1205, 1222, 5245) THEN @LockMessageSeverity ELSE ERROR_SEVERITY() END
      RAISERROR('%s',@Severity,1,@ErrorMessage) WITH NOWAIT

      IF NOT (ERROR_NUMBER() IN(1205, 1222, 5245) AND @LockMessageSeverity = 10)
      BEGIN
        SET @ReturnCode = ERROR_NUMBER()
      END

      IF @ExecuteAsUser IS NOT NULL
      BEGIN
        EXECUTE @sp_executesql @RevertCommand
      END
    END CATCH
  END

  ----------------------------------------------------------------------------------------------------
  --// Log completing information                                                                 //--
  ----------------------------------------------------------------------------------------------------

  SET @EndTime = SYSDATETIME()

  SET @EndMessage = 'Outcome: ' + CASE WHEN @Execute = 'N' THEN 'Not Executed' WHEN @Error = 0 THEN 'Succeeded' ELSE 'Failed' END
  RAISERROR('%s',10,1,@EndMessage) WITH NOWAIT

  SET @EndMessage = 'Duration: ' + CASE WHEN (DATEDIFF(SECOND,@StartTime,@EndTime) / (24 * 3600)) > 0 THEN CAST((DATEDIFF(SECOND,@StartTime,@EndTime) / (24 * 3600)) AS nvarchar(max)) + '.' ELSE '' END + CONVERT(nvarchar(max),DATEADD(SECOND,DATEDIFF(SECOND,@StartTime,@EndTime),'1900-01-01'),108)
  RAISERROR('%s',10,1,@EndMessage) WITH NOWAIT

  SET @EndMessage = 'Date and time: ' + CONVERT(nvarchar(max),@EndTime,120)
  RAISERROR('%s',10,1,@EndMessage) WITH NOWAIT

  RAISERROR(@EmptyLine,10,1) WITH NOWAIT

  IF @LogToTable = 'Y'
  BEGIN
    UPDATE dbo.CommandLog
    SET EndTime = @EndTime,
        ErrorNumber = CASE WHEN @Execute = 'N' THEN NULL ELSE @Error END,
        ErrorMessage = @ErrorMessageOriginal
    WHERE ID = @ID
  END

  ReturnCode:
  IF @ReturnCode <> 0
  BEGIN
    RETURN @ReturnCode
  END

  ----------------------------------------------------------------------------------------------------

END
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DatabaseIntegrityCheck]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[DatabaseIntegrityCheck] AS'
END
GO
ALTER PROCEDURE [dbo].[DatabaseIntegrityCheck]

@Databases nvarchar(max) = NULL,
@CheckCommands nvarchar(max) = 'CHECKDB',
@PhysicalOnly nvarchar(max) = 'N',
@DataPurity nvarchar(max) = 'N',
@NoIndex nvarchar(max) = 'N',
@ExtendedLogicalChecks nvarchar(max) = 'N',
@NoInformationalMessages nvarchar(max) = 'N',
@TabLock nvarchar(max) = 'N',
@FileGroups nvarchar(max) = NULL,
@Objects nvarchar(max) = NULL,
@MaxDOP int = NULL,
@AvailabilityGroups nvarchar(max) = NULL,
@AvailabilityGroupReplicas nvarchar(max) = 'ALL',
@Updateability nvarchar(max) = 'ALL',
@TimeLimit int = NULL,
@LockTimeout int = NULL,
@LockMessageSeverity int = 16,
@StringDelimiter nvarchar(max) = ',',
@DatabaseOrder nvarchar(max) = NULL,
@DatabasesInParallel nvarchar(max) = 'N',
@LogToTable nvarchar(max) = 'N',
@Execute nvarchar(max) = 'Y'

AS

BEGIN

  ----------------------------------------------------------------------------------------------------
  --// Source:  https://ola.hallengren.com                                                        //--
  --// License: https://ola.hallengren.com/license.html                                           //--
  --// GitHub:  https://github.com/olahallengren/sql-server-maintenance-solution                  //--
  --// Version: 2026-07-21 14:38:46                                                               //--
  ----------------------------------------------------------------------------------------------------

  SET NOCOUNT ON

  DECLARE @StartMessage nvarchar(max)
  DECLARE @EndMessage nvarchar(max)
  DECLARE @DatabaseMessage nvarchar(max)
  DECLARE @ErrorMessage nvarchar(max)
  DECLARE @Severity int

  DECLARE @StartTime datetime2 = SYSDATETIME()
  DECLARE @SchemaName nvarchar(max) = OBJECT_SCHEMA_NAME(@@PROCID)
  DECLARE @ObjectName nvarchar(max) = OBJECT_NAME(@@PROCID)
  DECLARE @VersionTimestamp nvarchar(max) = SUBSTRING(OBJECT_DEFINITION(@@PROCID),CHARINDEX('--// Version: ',OBJECT_DEFINITION(@@PROCID)) + LEN('--// Version: ') + 1, 19)

  DECLARE @Parameters TABLE (ID int IDENTITY PRIMARY KEY,
                             [Name] nvarchar(max) NOT NULL,
                             ValueNvarchar nvarchar(max),
                             ValueInt int,
                             ValueDatetime datetime2)

  DECLARE @ParametersString nvarchar(max)
  DECLARE @CurrentParameterName nvarchar(max)
  DECLARE @CurrentParameterValueNvarchar nvarchar(max)
  DECLARE @CurrentParameterValueInt int
  DECLARE @CurrentParameterValueDatetime datetime2
  DECLARE @CurrentParameterDelimiter nvarchar(max)
  DECLARE @CurrentParameterMessage nvarchar(max)

  DECLARE @HostPlatform nvarchar(max)
  DECLARE @ContainedAvailabilityGroupListenerConnection bit = 0

  DECLARE @QueueID int
  DECLARE @QueueStartTime datetime2

  DECLARE @CurrentDBID int
  DECLARE @CurrentDatabaseName nvarchar(max)

  DECLARE @CurrentDatabase_sp_executesql nvarchar(max)

  DECLARE @CurrentUserAccess nvarchar(max)
  DECLARE @CurrentIsReadOnly bit
  DECLARE @CurrentDatabaseState nvarchar(max)
  DECLARE @CurrentInStandby bit
  DECLARE @CurrentRecoveryModel nvarchar(max)

  DECLARE @CurrentAvailabilityGroupReplicaID uniqueidentifier
  DECLARE @CurrentAvailabilityGroupID uniqueidentifier
  DECLARE @CurrentAvailabilityGroup nvarchar(max)
  DECLARE @CurrentAvailabilityGroupRole nvarchar(max)
  DECLARE @CurrentAvailabilityGroupBackupPreference nvarchar(max)
  DECLARE @CurrentSecondaryRoleAllowConnections nvarchar(max)
  DECLARE @CurrentIsPreferredBackupReplica bit
  DECLARE @CurrentDistributedAvailabilityGroup nvarchar(max)
  DECLARE @CurrentDistributedAvailabilityGroupReplicaID uniqueidentifier
  DECLARE @CurrentDistributedAvailabilityGroupRole nvarchar(max)
  DECLARE @CurrentDatabaseMirroringRole nvarchar(max)

  DECLARE @CurrentFGID int
  DECLARE @CurrentFileGroupID int
  DECLARE @CurrentFileGroupName nvarchar(max)
  DECLARE @CurrentFileGroupExists bit

  DECLARE @CurrentOID int
  DECLARE @CurrentSchemaID int
  DECLARE @CurrentSchemaName nvarchar(max)
  DECLARE @CurrentObjectID int
  DECLARE @CurrentObjectName nvarchar(max)
  DECLARE @CurrentObjectType nvarchar(max)
  DECLARE @CurrentObjectExists bit

  DECLARE @CurrentDatabaseContext nvarchar(max)
  DECLARE @CurrentCommand nvarchar(max)
  DECLARE @CurrentCommandOutput int
  DECLARE @CurrentCommandType nvarchar(max)

  DECLARE @Errors TABLE (ID int IDENTITY PRIMARY KEY,
                         [Message] nvarchar(max) NOT NULL,
                         Severity int NOT NULL,
                         [State] int)

  DECLARE @CurrentMessage nvarchar(max)
  DECLARE @CurrentSeverity int
  DECLARE @CurrentState int

  DECLARE @tmpDatabases TABLE (ID int IDENTITY,
                               DatabaseName nvarchar(128),
                               DatabaseType nvarchar(1),
                               AvailabilityGroup bit,
                               [Snapshot] bit,
                               StartPosition int,
                               LastCommandTime datetime2,
                               DatabaseSize bigint,
                               LastGoodCheckDbTime datetime2,
                               [Order] int DEFAULT 0,
                               Selected bit DEFAULT 0,
                               Completed bit DEFAULT 0,
                               PRIMARY KEY (Selected, Completed, [Order], ID))

  DECLARE @tmpAvailabilityGroups TABLE (ID int IDENTITY PRIMARY KEY,
                                        AvailabilityGroupName nvarchar(128),
                                        StartPosition int,
                                        Selected bit DEFAULT 0)

  DECLARE @tmpDatabasesAvailabilityGroups TABLE (DatabaseName nvarchar(128),
                                                 AvailabilityGroupName nvarchar(128))

  DECLARE @tmpFileGroups TABLE (ID int IDENTITY,
                                FileGroupID int,
                                FileGroupName nvarchar(128),
                                StartPosition int,
                                [Order] int DEFAULT 0,
                                Selected bit DEFAULT 0,
                                Completed bit DEFAULT 0,
                                PRIMARY KEY (Selected, Completed, [Order], ID))

  DECLARE @tmpObjects TABLE (ID int IDENTITY,
                             SchemaID int,
                             SchemaName nvarchar(128),
                             ObjectID int,
                             ObjectName nvarchar(128),
                             ObjectType nvarchar(2),
                             StartPosition int,
                             [Order] int DEFAULT 0,
                             Selected bit DEFAULT 0,
                             Completed bit DEFAULT 0,
                             PRIMARY KEY (Selected, Completed, [Order], ID))

  DECLARE @SelectedDatabases TABLE (DatabaseName nvarchar(max),
                                    DatabaseType nvarchar(1),
                                    AvailabilityGroup bit,
                                    StartPosition int,
                                    Selected bit)

  DECLARE @SelectedAvailabilityGroups TABLE (AvailabilityGroupName nvarchar(max),
                                             StartPosition int,
                                             Selected bit)

  DECLARE @SelectedFileGroups TABLE (DatabaseName nvarchar(max),
                                     FileGroupName nvarchar(max),
                                     StartPosition int,
                                     Selected bit)

  DECLARE @SelectedObjects TABLE (DatabaseName nvarchar(max),
                                  SchemaName nvarchar(max),
                                  ObjectName nvarchar(max),
                                  StartPosition int,
                                  Selected bit)

  DECLARE @SelectedCheckCommands TABLE (CheckCommand nvarchar(max))

  DECLARE @Error int = 0
  DECLARE @ReturnCode int = 0

  DECLARE @EmptyLine nvarchar(max) = CHAR(9)

  DECLARE @ProductVersion nvarchar(max) = CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))
  DECLARE @ProductUpdateType nvarchar(max) = CAST(SERVERPROPERTY('ProductUpdateType') AS nvarchar(max))
  DECLARE @EngineEdition int = CAST(SERVERPROPERTY('EngineEdition') AS int)
  DECLARE @Edition nvarchar(max) = CAST(SERVERPROPERTY('Edition') AS nvarchar(max))
  DECLARE @IsHadrEnabled bit = CAST(SERVERPROPERTY('IsHadrEnabled') AS bit)
  DECLARE @IsClustered bit = CAST(SERVERPROPERTY('IsClustered') AS bit)
  DECLARE @ServerName nvarchar(max) = CAST(SERVERPROPERTY('ServerName') AS nvarchar(max))

  DECLARE @Version numeric(18,10) = CAST(PARSENAME(@ProductVersion,4) + '.' + PARSENAME(@ProductVersion,3) + PARSENAME(@ProductVersion,2) AS numeric(18,10))

  IF @EngineEdition = 8 AND @ProductVersion = '12.0.2000.8' AND @ProductUpdateType = 'CU'
  BEGIN
    SET @Version = 16.01000
  END

  IF @EngineEdition <> 5
  BEGIN
    SELECT @HostPlatform = host_platform
    FROM sys.dm_os_host_info
  END

  IF @EngineEdition <> 5
  BEGIN
    IF EXISTS (SELECT * FROM sys.databases WHERE name = 'msdb' AND database_id <> 4)
    AND EXISTS (SELECT * FROM sys.dm_exec_connections dm_exec_connections INNER JOIN sys.availability_group_listener_ip_addresses availability_group_listener_ip_addresses ON dm_exec_connections.local_net_address = availability_group_listener_ip_addresses.ip_address WHERE dm_exec_connections.session_id = @@SPID)
    BEGIN
      SET @ContainedAvailabilityGroupListenerConnection = 1
    END
  END

  DECLARE @AmazonRDS bit = CASE WHEN @EngineEdition IN (5, 8) THEN 0 WHEN EXISTS (SELECT * FROM sys.databases WHERE [name] = 'rdsadmin') AND SUSER_SNAME(0x01) = 'rdsa' THEN 1 ELSE 0 END

  ----------------------------------------------------------------------------------------------------
  --// Log initial information                                                                    //--
  ----------------------------------------------------------------------------------------------------

  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@Databases', @Databases)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@CheckCommands', @CheckCommands)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@PhysicalOnly', @PhysicalOnly)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@DataPurity', @DataPurity)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@NoIndex', @NoIndex)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@ExtendedLogicalChecks', @ExtendedLogicalChecks)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@NoInformationalMessages', @NoInformationalMessages)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@TabLock', @TabLock)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@FileGroups', @FileGroups)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@Objects', @Objects)
  INSERT INTO @Parameters ([Name], ValueInt) VALUES('@MaxDOP', @MaxDOP)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@AvailabilityGroups', @AvailabilityGroups)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@AvailabilityGroupReplicas', @AvailabilityGroupReplicas)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@Updateability', @Updateability)
  INSERT INTO @Parameters ([Name], ValueInt) VALUES('@TimeLimit', @TimeLimit)
  INSERT INTO @Parameters ([Name], ValueInt) VALUES('@LockTimeout', @LockTimeout)
  INSERT INTO @Parameters ([Name], ValueInt) VALUES('@LockMessageSeverity', @LockMessageSeverity)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@StringDelimiter', @StringDelimiter)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@DatabaseOrder', @DatabaseOrder)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@DatabasesInParallel', @DatabasesInParallel)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@LogToTable', @LogToTable)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@Execute', @Execute)

  SELECT @ParametersString = STRING_AGG(CAST([Name] + ' = ' + CASE WHEN ValueNvarchar IS NOT NULL THEN '''' + REPLACE(ValueNvarchar,'''','''''') + '''' WHEN ValueInt IS NOT NULL THEN CAST(ValueInt AS nvarchar(max)) WHEN ValueDatetime IS NOT NULL THEN '''' + CONVERT(nvarchar(max), ValueDatetime, 21) + '''' ELSE 'NULL' END AS nvarchar(max)), ', ') WITHIN GROUP (ORDER BY [ID] ASC)
  FROM @Parameters

  SET @StartMessage = 'Date and time: ' + CONVERT(nvarchar(max),@StartTime,120)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Server: ' + @ServerName
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Version: ' + @ProductVersion
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Edition: ' + @Edition
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  IF @EngineEdition = 8
  BEGIN
    SET @StartMessage = 'Update type: ' + @ProductUpdateType
    RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT
  END

  IF @EngineEdition <> 5
  BEGIN
    SET @StartMessage = 'Platform: ' + ISNULL(@HostPlatform, 'N/A')
    RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT
  END

  IF @EngineEdition <> 5
  BEGIN
    SET @StartMessage = 'Contained availability group connection: ' + CASE WHEN @ContainedAvailabilityGroupListenerConnection = 1 THEN 'Yes' WHEN @ContainedAvailabilityGroupListenerConnection = 0 THEN 'No' ELSE 'N/A' END
    RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT
  END

  SET @StartMessage = 'Database: ' + QUOTENAME(DB_NAME())
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Procedure: ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@ObjectName)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Version: ' + @VersionTimestamp
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Source: https://ola.hallengren.com'
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT
  RAISERROR(@EmptyLine,10,1) WITH NOWAIT

  SET @StartMessage = 'Command:'
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'EXECUTE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@ObjectName)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  DECLARE ParameterCursor CURSOR LOCAL FAST_FORWARD FOR SELECT [Name], ValueNvarchar, ValueInt, ValueDatetime, CASE WHEN [ID] = MAX([ID]) OVER() THEN '' ELSE ',' END FROM @Parameters ORDER BY [ID] ASC

  OPEN ParameterCursor

  FETCH ParameterCursor INTO @CurrentParameterName, @CurrentParameterValueNvarchar, @CurrentParameterValueInt, @CurrentParameterValueDatetime, @CurrentParameterDelimiter

  WHILE @@FETCH_STATUS = 0
  BEGIN
    SET @CurrentParameterMessage = @CurrentParameterName + ' = ' + CASE WHEN @CurrentParameterValueNvarchar IS NOT NULL THEN '''' + REPLACE(@CurrentParameterValueNvarchar,'''','''''') + '''' WHEN @CurrentParameterValueInt IS NOT NULL THEN CAST(@CurrentParameterValueInt AS nvarchar(max)) WHEN @CurrentParameterValueDatetime IS NOT NULL THEN '''' + CONVERT(nvarchar(max), @CurrentParameterValueDatetime, 21) + '''' ELSE 'NULL' END + @CurrentParameterDelimiter

    RAISERROR('%s',10,1,@CurrentParameterMessage) WITH NOWAIT

    FETCH NEXT FROM ParameterCursor INTO @CurrentParameterName, @CurrentParameterValueNvarchar, @CurrentParameterValueInt, @CurrentParameterValueDatetime, @CurrentParameterDelimiter
  END

  CLOSE ParameterCursor

  DEALLOCATE ParameterCursor

  RAISERROR(@EmptyLine,10,1) WITH NOWAIT

  ----------------------------------------------------------------------------------------------------
  --// Check core requirements                                                                    //--
  ----------------------------------------------------------------------------------------------------

  IF NOT (SELECT uses_ansi_nulls FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('ANSI_NULLS has to be set to ON for the stored procedure.', 16, 1)
  END

  IF NOT (SELECT uses_quoted_identifier FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('QUOTED_IDENTIFIER has to be set to ON for the stored procedure.', 16, 1)
  END

  IF NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'P' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandExecute')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The stored procedure CommandExecute is missing. Download https://ola.hallengren.com/scripts/CommandExecute.sql.', 16, 1)
  END

  IF EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'P' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandExecute' AND OBJECT_DEFINITION(objects.[object_id]) NOT LIKE '%@DatabaseContext%')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The stored procedure CommandExecute needs to be updated. Download https://ola.hallengren.com/scripts/CommandExecute.sql.', 16, 1)
  END

  IF @LogToTable = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandLog')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The table CommandLog is missing. Download https://ola.hallengren.com/scripts/CommandLog.sql.', 16, 1)
  END

  IF @DatabasesInParallel = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'Queue')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The table Queue is missing. Download https://ola.hallengren.com/scripts/Queue.sql.', 16, 1)
  END

  IF @DatabasesInParallel = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'QueueDatabase')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The table QueueDatabase is missing. Download https://ola.hallengren.com/scripts/QueueDatabase.sql.', 16, 1)
  END

  IF @@TRANCOUNT <> 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The transaction count is not 0.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------
  --// Select databases                                                                           //--
  ----------------------------------------------------------------------------------------------------

  SET @Databases = REPLACE(@Databases, CHAR(10), '')
  SET @Databases = REPLACE(@Databases, CHAR(13), '')

  WHILE CHARINDEX(@StringDelimiter + ' ', @Databases) > 0 SET @Databases = REPLACE(@Databases, @StringDelimiter + ' ', @StringDelimiter)
  WHILE CHARINDEX(' ' + @StringDelimiter, @Databases) > 0 SET @Databases = REPLACE(@Databases, ' ' + @StringDelimiter, @StringDelimiter)

  SET @Databases = LTRIM(RTRIM(@Databases));

  WITH Databases1 (StartPosition, EndPosition, DatabaseItem) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, 1), 0), LEN(@Databases) + 1) AS EndPosition,
         SUBSTRING(@Databases, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, 1), 0), LEN(@Databases) + 1) - 1) AS DatabaseItem
  WHERE @Databases IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, EndPosition + 1), 0), LEN(@Databases) + 1) AS EndPosition,
         SUBSTRING(@Databases, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, EndPosition + 1), 0), LEN(@Databases) + 1) - EndPosition - 1) AS DatabaseItem
  FROM Databases1
  WHERE EndPosition < LEN(@Databases) + 1
  ),
  Databases2 (DatabaseItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN DatabaseItem LIKE '-%' THEN RIGHT(DatabaseItem,LEN(DatabaseItem) - 1) ELSE DatabaseItem END AS DatabaseItem,
         StartPosition,
         CASE WHEN DatabaseItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
  FROM Databases1
  ),
  Databases3 (DatabaseItem, DatabaseType, AvailabilityGroup, StartPosition, Selected) AS
  (
  SELECT CASE WHEN DatabaseItem IN('ALL_DATABASES','SYSTEM_DATABASES','USER_DATABASES','AVAILABILITY_GROUP_DATABASES') THEN '%' ELSE DatabaseItem END AS DatabaseItem,
         CASE WHEN DatabaseItem = 'SYSTEM_DATABASES' THEN 'S' WHEN DatabaseItem = 'USER_DATABASES' THEN 'U' ELSE NULL END AS DatabaseType,
         CASE WHEN DatabaseItem = 'AVAILABILITY_GROUP_DATABASES' THEN 1 ELSE NULL END AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases2
  ),
  Databases4 (DatabaseName, DatabaseType, AvailabilityGroup, StartPosition, Selected) AS
  (
  SELECT CASE WHEN LEFT(DatabaseItem,1) = '[' AND RIGHT(DatabaseItem,1) = ']' THEN PARSENAME(DatabaseItem,1) ELSE DatabaseItem END AS DatabaseItem,
         DatabaseType,
         AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases3
  )
  INSERT INTO @SelectedDatabases (DatabaseName, DatabaseType, AvailabilityGroup, StartPosition, Selected)
  SELECT DatabaseName,
         DatabaseType,
         AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases4
  OPTION (MAXRECURSION 0)

  IF @IsHadrEnabled = 1
  BEGIN
    INSERT INTO @tmpAvailabilityGroups (AvailabilityGroupName)
    SELECT name AS AvailabilityGroupName
    FROM sys.availability_groups

    INSERT INTO @tmpDatabasesAvailabilityGroups (DatabaseName, AvailabilityGroupName)
    SELECT databases.name,
           availability_groups.name
    FROM sys.databases databases
    INNER JOIN sys.availability_replicas availability_replicas ON databases.replica_id = availability_replicas.replica_id
    INNER JOIN sys.availability_groups availability_groups ON availability_replicas.group_id = availability_groups.group_id
  END

  INSERT INTO @tmpDatabases (DatabaseName, DatabaseType, AvailabilityGroup, [Snapshot])
  SELECT [name] AS DatabaseName,
         CASE WHEN name IN('master','msdb','model') OR is_distributor = 1 THEN 'S' ELSE 'U' END AS DatabaseType,
         NULL AS AvailabilityGroup,
         CASE WHEN source_database_id IS NOT NULL THEN 1 ELSE 0 END AS [Snapshot]
  FROM sys.databases
  ORDER BY [name] ASC

  UPDATE tmpDatabases
  SET AvailabilityGroup = CASE WHEN EXISTS (SELECT * FROM @tmpDatabasesAvailabilityGroups WHERE DatabaseName = tmpDatabases.DatabaseName) THEN 1 ELSE 0 END
  FROM @tmpDatabases tmpDatabases

  UPDATE tmpDatabases
  SET tmpDatabases.Selected = SelectedDatabases.Selected
  FROM @tmpDatabases tmpDatabases
  INNER JOIN @SelectedDatabases SelectedDatabases
  ON tmpDatabases.DatabaseName LIKE REPLACE(REPLACE(SelectedDatabases.DatabaseName,'[','[[]'),'_','[_]')
  AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
  AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
  AND NOT ((tmpDatabases.DatabaseName = 'tempdb' OR tmpDatabases.[Snapshot] = 1) AND tmpDatabases.DatabaseName <> SelectedDatabases.DatabaseName)
  WHERE SelectedDatabases.Selected = 1

  UPDATE tmpDatabases
  SET tmpDatabases.Selected = SelectedDatabases.Selected
  FROM @tmpDatabases tmpDatabases
  INNER JOIN @SelectedDatabases SelectedDatabases
  ON tmpDatabases.DatabaseName LIKE REPLACE(REPLACE(SelectedDatabases.DatabaseName,'[','[[]'),'_','[_]')
  AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
  AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
  AND NOT ((tmpDatabases.DatabaseName = 'tempdb' OR tmpDatabases.[Snapshot] = 1) AND tmpDatabases.DatabaseName <> SelectedDatabases.DatabaseName)
  WHERE SelectedDatabases.Selected = 0

  UPDATE tmpDatabases
  SET tmpDatabases.StartPosition = SelectedDatabases2.StartPosition
  FROM @tmpDatabases tmpDatabases
  INNER JOIN (SELECT tmpDatabases.DatabaseName, MIN(SelectedDatabases.StartPosition) AS StartPosition
              FROM @tmpDatabases tmpDatabases
              INNER JOIN @SelectedDatabases SelectedDatabases
              ON tmpDatabases.DatabaseName LIKE REPLACE(REPLACE(SelectedDatabases.DatabaseName,'[','[[]'),'_','[_]')
              AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
              AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
              WHERE SelectedDatabases.Selected = 1
              GROUP BY tmpDatabases.DatabaseName) SelectedDatabases2
  ON tmpDatabases.DatabaseName = SelectedDatabases2.DatabaseName

  IF @Databases IS NOT NULL AND (NOT EXISTS(SELECT * FROM @SelectedDatabases) OR EXISTS(SELECT * FROM @SelectedDatabases WHERE DatabaseName IS NULL OR DATALENGTH(DatabaseName) = 0))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @Databases is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------
  --// Select availability groups                                                                 //--
  ----------------------------------------------------------------------------------------------------

  IF @AvailabilityGroups IS NOT NULL AND @IsHadrEnabled = 1
  BEGIN

    SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, CHAR(10), '')
    SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, CHAR(13), '')

    WHILE CHARINDEX(@StringDelimiter + ' ', @AvailabilityGroups) > 0 SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, @StringDelimiter + ' ', @StringDelimiter)
    WHILE CHARINDEX(' ' + @StringDelimiter, @AvailabilityGroups) > 0 SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, ' ' + @StringDelimiter, @StringDelimiter)

    SET @AvailabilityGroups = LTRIM(RTRIM(@AvailabilityGroups));

    WITH AvailabilityGroups1 (StartPosition, EndPosition, AvailabilityGroupItem) AS
    (
    SELECT 1 AS StartPosition,
           ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, 1), 0), LEN(@AvailabilityGroups) + 1) AS EndPosition,
           SUBSTRING(@AvailabilityGroups, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, 1), 0), LEN(@AvailabilityGroups) + 1) - 1) AS AvailabilityGroupItem
    WHERE @AvailabilityGroups IS NOT NULL
    UNION ALL
    SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
           ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, EndPosition + 1), 0), LEN(@AvailabilityGroups) + 1) AS EndPosition,
           SUBSTRING(@AvailabilityGroups, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, EndPosition + 1), 0), LEN(@AvailabilityGroups) + 1) - EndPosition - 1) AS AvailabilityGroupItem
    FROM AvailabilityGroups1
    WHERE EndPosition < LEN(@AvailabilityGroups) + 1
    ),
    AvailabilityGroups2 (AvailabilityGroupItem, StartPosition, Selected) AS
    (
    SELECT CASE WHEN AvailabilityGroupItem LIKE '-%' THEN RIGHT(AvailabilityGroupItem,LEN(AvailabilityGroupItem) - 1) ELSE AvailabilityGroupItem END AS AvailabilityGroupItem,
           StartPosition,
           CASE WHEN AvailabilityGroupItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
    FROM AvailabilityGroups1
    ),
    AvailabilityGroups3 (AvailabilityGroupItem, StartPosition, Selected) AS
    (
    SELECT CASE WHEN AvailabilityGroupItem = 'ALL_AVAILABILITY_GROUPS' THEN '%' ELSE AvailabilityGroupItem END AS AvailabilityGroupItem,
           StartPosition,
           Selected
    FROM AvailabilityGroups2
    ),
    AvailabilityGroups4 (AvailabilityGroupName, StartPosition, Selected) AS
    (
    SELECT CASE WHEN LEFT(AvailabilityGroupItem,1) = '[' AND RIGHT(AvailabilityGroupItem,1) = ']' THEN PARSENAME(AvailabilityGroupItem,1) ELSE AvailabilityGroupItem END AS AvailabilityGroupItem,
           StartPosition,
           Selected
    FROM AvailabilityGroups3
    )
    INSERT INTO @SelectedAvailabilityGroups (AvailabilityGroupName, StartPosition, Selected)
    SELECT AvailabilityGroupName, StartPosition, Selected
    FROM AvailabilityGroups4
    OPTION (MAXRECURSION 0)

    UPDATE tmpAvailabilityGroups
    SET tmpAvailabilityGroups.Selected = SelectedAvailabilityGroups.Selected
    FROM @tmpAvailabilityGroups tmpAvailabilityGroups
    INNER JOIN @SelectedAvailabilityGroups SelectedAvailabilityGroups
    ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'[','[[]'),'_','[_]')
    WHERE SelectedAvailabilityGroups.Selected = 1

    UPDATE tmpAvailabilityGroups
    SET tmpAvailabilityGroups.Selected = SelectedAvailabilityGroups.Selected
    FROM @tmpAvailabilityGroups tmpAvailabilityGroups
    INNER JOIN @SelectedAvailabilityGroups SelectedAvailabilityGroups
    ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'[','[[]'),'_','[_]')
    WHERE SelectedAvailabilityGroups.Selected = 0

    UPDATE tmpAvailabilityGroups
    SET tmpAvailabilityGroups.StartPosition = SelectedAvailabilityGroups2.StartPosition
    FROM @tmpAvailabilityGroups tmpAvailabilityGroups
    INNER JOIN (SELECT tmpAvailabilityGroups.AvailabilityGroupName, MIN(SelectedAvailabilityGroups.StartPosition) AS StartPosition
                FROM @tmpAvailabilityGroups tmpAvailabilityGroups
                INNER JOIN @SelectedAvailabilityGroups SelectedAvailabilityGroups
                ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'[','[[]'),'_','[_]')
                WHERE SelectedAvailabilityGroups.Selected = 1
                GROUP BY tmpAvailabilityGroups.AvailabilityGroupName) SelectedAvailabilityGroups2
    ON tmpAvailabilityGroups.AvailabilityGroupName = SelectedAvailabilityGroups2.AvailabilityGroupName

    UPDATE tmpDatabases
    SET tmpDatabases.StartPosition = tmpAvailabilityGroups.StartPosition,
        tmpDatabases.Selected = 1
    FROM @tmpDatabases tmpDatabases
    INNER JOIN @tmpDatabasesAvailabilityGroups tmpDatabasesAvailabilityGroups ON tmpDatabases.DatabaseName = tmpDatabasesAvailabilityGroups.DatabaseName
    INNER JOIN @tmpAvailabilityGroups tmpAvailabilityGroups ON tmpDatabasesAvailabilityGroups.AvailabilityGroupName = tmpAvailabilityGroups.AvailabilityGroupName
    WHERE tmpAvailabilityGroups.Selected = 1

  END

  IF @AvailabilityGroups IS NOT NULL AND (NOT EXISTS(SELECT * FROM @SelectedAvailabilityGroups) OR EXISTS(SELECT * FROM @SelectedAvailabilityGroups WHERE AvailabilityGroupName IS NULL OR AvailabilityGroupName = '') OR @IsHadrEnabled = 0)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @AvailabilityGroups is not supported.', 16, 1)
  END

  IF (@Databases IS NULL AND @AvailabilityGroups IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('You need to specify one of the parameters @Databases and @AvailabilityGroups.', 16, 2)
  END

  IF (@Databases IS NOT NULL AND @AvailabilityGroups IS NOT NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('You can only specify one of the parameters @Databases and @AvailabilityGroups.', 16, 3)
  END

  ----------------------------------------------------------------------------------------------------
  --// Select filegroups                                                                          //--
  ----------------------------------------------------------------------------------------------------

  SET @FileGroups = REPLACE(@FileGroups, CHAR(10), '')
  SET @FileGroups = REPLACE(@FileGroups, CHAR(13), '')

  WHILE CHARINDEX(@StringDelimiter + ' ', @FileGroups) > 0 SET @FileGroups = REPLACE(@FileGroups, @StringDelimiter + ' ', @StringDelimiter)
  WHILE CHARINDEX(' ' + @StringDelimiter, @FileGroups) > 0 SET @FileGroups = REPLACE(@FileGroups, ' ' + @StringDelimiter, @StringDelimiter)

  SET @FileGroups = LTRIM(RTRIM(@FileGroups));

  WITH FileGroups1 (StartPosition, EndPosition, FileGroupItem) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FileGroups, 1), 0), LEN(@FileGroups) + 1) AS EndPosition,
         SUBSTRING(@FileGroups, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FileGroups, 1), 0), LEN(@FileGroups) + 1) - 1) AS FileGroupItem
  WHERE @FileGroups IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FileGroups, EndPosition + 1), 0), LEN(@FileGroups) + 1) AS EndPosition,
         SUBSTRING(@FileGroups, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FileGroups, EndPosition + 1), 0), LEN(@FileGroups) + 1) - EndPosition - 1) AS FileGroupItem
  FROM FileGroups1
  WHERE EndPosition < LEN(@FileGroups) + 1
  ),
  FileGroups2 (FileGroupItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN FileGroupItem LIKE '-%' THEN RIGHT(FileGroupItem,LEN(FileGroupItem) - 1) ELSE FileGroupItem END AS FileGroupItem,
         StartPosition,
         CASE WHEN FileGroupItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
  FROM FileGroups1
  ),
  FileGroups3 (FileGroupItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN FileGroupItem = 'ALL_FILEGROUPS' THEN '%.%' ELSE FileGroupItem END AS FileGroupItem,
         StartPosition,
         Selected
  FROM FileGroups2
  ),
  FileGroups4 (DatabaseName, FileGroupName, StartPosition, Selected) AS
  (
  SELECT CASE WHEN PARSENAME(FileGroupItem,4) IS NULL AND PARSENAME(FileGroupItem,3) IS NULL THEN PARSENAME(FileGroupItem,2) ELSE NULL END AS DatabaseName,
         CASE WHEN PARSENAME(FileGroupItem,4) IS NULL AND PARSENAME(FileGroupItem,3) IS NULL THEN PARSENAME(FileGroupItem,1) ELSE NULL END AS FileGroupName,
         StartPosition,
         Selected
  FROM FileGroups3
  )
  INSERT INTO @SelectedFileGroups (DatabaseName, FileGroupName, StartPosition, Selected)
  SELECT DatabaseName, FileGroupName, StartPosition, Selected
  FROM FileGroups4
  OPTION (MAXRECURSION 0)

  ----------------------------------------------------------------------------------------------------
  --// Select objects                                                                             //--
  ----------------------------------------------------------------------------------------------------

  SET @Objects = REPLACE(@Objects, CHAR(10), '')
  SET @Objects = REPLACE(@Objects, CHAR(13), '')

  WHILE CHARINDEX(@StringDelimiter + ' ', @Objects) > 0 SET @Objects = REPLACE(@Objects, @StringDelimiter + ' ', @StringDelimiter)
  WHILE CHARINDEX(' ' + @StringDelimiter, @Objects) > 0 SET @Objects = REPLACE(@Objects, ' ' + @StringDelimiter, @StringDelimiter)

  SET @Objects = LTRIM(RTRIM(@Objects));

  WITH Objects1 (StartPosition, EndPosition, ObjectItem) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Objects, 1), 0), LEN(@Objects) + 1) AS EndPosition,
         SUBSTRING(@Objects, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Objects, 1), 0), LEN(@Objects) + 1) - 1) AS ObjectItem
  WHERE @Objects IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Objects, EndPosition + 1), 0), LEN(@Objects) + 1) AS EndPosition,
         SUBSTRING(@Objects, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Objects, EndPosition + 1), 0), LEN(@Objects) + 1) - EndPosition - 1) AS ObjectItem
  FROM Objects1
  WHERE EndPosition < LEN(@Objects) + 1
  ),
  Objects2 (ObjectItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN ObjectItem LIKE '-%' THEN RIGHT(ObjectItem,LEN(ObjectItem) - 1) ELSE ObjectItem END AS ObjectItem,
          StartPosition,
         CASE WHEN ObjectItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
  FROM Objects1
  ),
  Objects3 (ObjectItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN ObjectItem = 'ALL_OBJECTS' THEN '%.%.%' ELSE ObjectItem END AS ObjectItem,
         StartPosition,
         Selected
  FROM Objects2
  ),
  Objects4 (DatabaseName, SchemaName, ObjectName, StartPosition, Selected) AS
  (
  SELECT CASE WHEN PARSENAME(ObjectItem,4) IS NULL THEN PARSENAME(ObjectItem,3) ELSE NULL END AS DatabaseName,
         CASE WHEN PARSENAME(ObjectItem,4) IS NULL THEN PARSENAME(ObjectItem,2) ELSE NULL END AS SchemaName,
         CASE WHEN PARSENAME(ObjectItem,4) IS NULL THEN PARSENAME(ObjectItem,1) ELSE NULL END AS ObjectName,
         StartPosition,
         Selected
  FROM Objects3
  )
  INSERT INTO @SelectedObjects (DatabaseName, SchemaName, ObjectName, StartPosition, Selected)
  SELECT DatabaseName, SchemaName, ObjectName, StartPosition, Selected
  FROM Objects4
  OPTION (MAXRECURSION 0)

  ----------------------------------------------------------------------------------------------------
  --// Select check commands                                                                      //--
  ----------------------------------------------------------------------------------------------------

  SET @CheckCommands = REPLACE(@CheckCommands, @StringDelimiter + ' ', @StringDelimiter);

  WITH CheckCommands (StartPosition, EndPosition, CheckCommand) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @CheckCommands, 1), 0), LEN(@CheckCommands) + 1) AS EndPosition,
         SUBSTRING(@CheckCommands, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @CheckCommands, 1), 0), LEN(@CheckCommands) + 1) - 1) AS CheckCommand
  WHERE @CheckCommands IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @CheckCommands, EndPosition + 1), 0), LEN(@CheckCommands) + 1) AS EndPosition,
         SUBSTRING(@CheckCommands, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @CheckCommands, EndPosition + 1), 0), LEN(@CheckCommands) + 1) - EndPosition - 1) AS CheckCommand
  FROM CheckCommands
  WHERE EndPosition < LEN(@CheckCommands) + 1
  )
  INSERT INTO @SelectedCheckCommands (CheckCommand)
  SELECT CheckCommand
  FROM CheckCommands
  OPTION (MAXRECURSION 0)

  ----------------------------------------------------------------------------------------------------
  --// Check input parameters                                                                     //--
  ----------------------------------------------------------------------------------------------------

  IF EXISTS (SELECT * FROM @SelectedCheckCommands WHERE CheckCommand NOT IN('CHECKDB','CHECKFILEGROUP','CHECKALLOC','CHECKTABLE','CHECKCATALOG'))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @CheckCommands is not supported.', 16, 1)
  END

  IF EXISTS (SELECT * FROM @SelectedCheckCommands GROUP BY CheckCommand HAVING COUNT(*) > 1)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @CheckCommands is not supported.', 16, 2)
  END

  IF NOT EXISTS (SELECT * FROM @SelectedCheckCommands)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @CheckCommands is not supported.', 16, 3)
  END

  IF EXISTS (SELECT * FROM @SelectedCheckCommands WHERE CheckCommand IN('CHECKDB')) AND EXISTS (SELECT CheckCommand FROM @SelectedCheckCommands WHERE CheckCommand IN('CHECKFILEGROUP','CHECKALLOC','CHECKTABLE','CHECKCATALOG'))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @CheckCommands is not supported.', 16, 4)
  END

  IF EXISTS (SELECT * FROM @SelectedCheckCommands WHERE CheckCommand IN('CHECKFILEGROUP')) AND EXISTS (SELECT CheckCommand FROM @SelectedCheckCommands WHERE CheckCommand IN('CHECKALLOC','CHECKTABLE'))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @CheckCommands is not supported.', 16, 5)
  END

  ----------------------------------------------------------------------------------------------------

  IF @PhysicalOnly NOT IN ('Y','N') OR @PhysicalOnly IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @PhysicalOnly is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @DataPurity NOT IN ('Y','N') OR @DataPurity IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @DataPurity is not supported.', 16, 1)
  END

  IF @PhysicalOnly = 'Y' AND @DataPurity = 'Y'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The parameters @PhysicalOnly and @DataPurity cannot be used together.', 16, 2)
  END

  ----------------------------------------------------------------------------------------------------

  IF @NoIndex NOT IN ('Y','N') OR @NoIndex IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @NoIndex is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @ExtendedLogicalChecks NOT IN ('Y','N') OR @ExtendedLogicalChecks IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @ExtendedLogicalChecks is not supported.', 16, 1)
  END

  IF @PhysicalOnly = 'Y' AND @ExtendedLogicalChecks = 'Y'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The parameters @PhysicalOnly and @ExtendedLogicalChecks cannot be used together.', 16, 2)
  END

  ----------------------------------------------------------------------------------------------------

  IF @NoInformationalMessages NOT IN ('Y','N') OR @NoInformationalMessages IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @NoInformationalMessages is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @TabLock NOT IN ('Y','N') OR @TabLock IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @TabLock is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS(SELECT * FROM @SelectedFileGroups WHERE DatabaseName IS NULL OR FileGroupName IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @FileGroups is not supported.', 16, 1)
  END

  IF @FileGroups IS NOT NULL AND NOT EXISTS(SELECT * FROM @SelectedFileGroups)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @FileGroups is not supported.', 16, 2)
  END

  IF @FileGroups IS NOT NULL AND NOT EXISTS (SELECT * FROM @SelectedCheckCommands WHERE CheckCommand = 'CHECKFILEGROUP')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @FileGroups is not supported.', 16, 3)
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS(SELECT * FROM @SelectedObjects WHERE DatabaseName IS NULL OR SchemaName IS NULL OR ObjectName IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @Objects is not supported.', 16, 1)
  END

  IF (@Objects IS NOT NULL AND NOT EXISTS(SELECT * FROM @SelectedObjects))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @Objects is not supported.', 16, 2)
  END

  IF (@Objects IS NOT NULL AND NOT EXISTS (SELECT * FROM @SelectedCheckCommands WHERE CheckCommand = 'CHECKTABLE'))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @Objects is not supported.', 16, 3)
  END

  ----------------------------------------------------------------------------------------------------

  IF @MaxDOP < 0 OR @MaxDOP > 64
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @MaxDOP is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @AvailabilityGroupReplicas NOT IN('ALL','PRIMARY','SECONDARY','PREFERRED_BACKUP_REPLICA') OR @AvailabilityGroupReplicas IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @AvailabilityGroupReplicas is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @Updateability NOT IN('READ_ONLY','READ_WRITE','ALL') OR @Updateability IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @Updateability is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @TimeLimit < 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @TimeLimit is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @LockTimeout < 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @LockTimeout is not supported.', 16, 1)
  END

  IF @LockTimeout > 86400
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @LockTimeout is not supported.', 16, 2)
  END

  ----------------------------------------------------------------------------------------------------

  IF @LockMessageSeverity NOT IN(10, 16) OR @LockMessageSeverity IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @LockMessageSeverity is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @StringDelimiter IS NULL OR LEN(@StringDelimiter) <> 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @StringDelimiter is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @DatabaseOrder NOT IN('DATABASE_NAME_ASC','DATABASE_NAME_DESC','DATABASE_SIZE_ASC','DATABASE_SIZE_DESC','DATABASE_LAST_GOOD_CHECK_ASC','DATABASE_LAST_GOOD_CHECK_DESC','REPLICA_LAST_GOOD_CHECK_ASC','REPLICA_LAST_GOOD_CHECK_DESC')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @DatabaseOrder is not supported.', 16, 1)
  END

  IF @DatabaseOrder IN('DATABASE_LAST_GOOD_CHECK_ASC','DATABASE_LAST_GOOD_CHECK_DESC') AND NOT (@Version >= 14.03029 OR (@EngineEdition = 8 AND @ProductUpdateType = 'Continuous'))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @DatabaseOrder is not supported. DATABASEPROPERTYEX(''DatabaseName'', ''LastGoodCheckDbTime'') is not available in this version of SQL Server.', 16, 2)
  END

  IF @DatabaseOrder IN('REPLICA_LAST_GOOD_CHECK_ASC','REPLICA_LAST_GOOD_CHECK_DESC') AND @LogToTable = 'N'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @DatabaseOrder is not supported. You need to provide the parameter @LogToTable = ''Y''.', 16, 3)
  END

  IF @DatabaseOrder IN('DATABASE_LAST_GOOD_CHECK_ASC','DATABASE_LAST_GOOD_CHECK_DESC','REPLICA_LAST_GOOD_CHECK_ASC','REPLICA_LAST_GOOD_CHECK_DESC') AND @CheckCommands <> 'CHECKDB'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @DatabaseOrder is not supported. You need to provide the parameter @CheckCommands = ''CHECKDB''.', 16, 4)
  END

  IF @DatabaseOrder IS NOT NULL AND @EngineEdition = 5
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @DatabaseOrder is not supported. This parameter is not supported in Azure SQL Database.', 16, 5)
  END

  ----------------------------------------------------------------------------------------------------

  IF @DatabasesInParallel NOT IN('Y','N') OR @DatabasesInParallel IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @DatabasesInParallel is not supported.', 16, 1)
  END

  IF @DatabasesInParallel = 'Y' AND @EngineEdition = 5
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @DatabasesInParallel is not supported. This parameter is not supported in Azure SQL Database.', 16, 2)
  END

  ----------------------------------------------------------------------------------------------------

  IF @LogToTable NOT IN('Y','N') OR @LogToTable IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @LogToTable is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @Execute NOT IN('Y','N') OR @Execute IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @Execute is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS(SELECT * FROM @Errors)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The documentation is available at https://ola.hallengren.com/sql-server-integrity-check.html.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------
  --// Check that selected databases and availability groups exist                                //--
  ----------------------------------------------------------------------------------------------------

  SELECT @ErrorMessage = STRING_AGG(CAST(QUOTENAME(DatabaseName) AS nvarchar(max)), ', ')
                         WITHIN GROUP (ORDER BY DatabaseName ASC)
  FROM @SelectedDatabases
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases)

  IF @ErrorMessage IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The following databases in the @Databases parameter do not exist: ' + @ErrorMessage + '.', 10, 1)
  END

  SELECT @ErrorMessage = STRING_AGG(CAST(QUOTENAME(DatabaseName) AS nvarchar(max)), ', ')
                         WITHIN GROUP (ORDER BY DatabaseName ASC)
  FROM @SelectedFileGroups
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases)

  IF @ErrorMessage IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The following databases in the @FileGroups parameter do not exist: ' + @ErrorMessage + '.', 10, 1)
  END

  SELECT @ErrorMessage = STRING_AGG(CAST(QUOTENAME(DatabaseName) AS nvarchar(max)), ', ')
                         WITHIN GROUP (ORDER BY DatabaseName ASC)
  FROM @SelectedObjects
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases)

  IF @ErrorMessage IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The following databases in the @Objects parameter do not exist: ' + @ErrorMessage + '.', 10, 1)
  END

  SELECT @ErrorMessage = STRING_AGG(CAST(QUOTENAME(AvailabilityGroupName) AS nvarchar(max)), ', ')
                         WITHIN GROUP (ORDER BY AvailabilityGroupName ASC)
  FROM @SelectedAvailabilityGroups
  WHERE AvailabilityGroupName NOT LIKE '%[%]%'
  AND AvailabilityGroupName NOT IN (SELECT AvailabilityGroupName FROM @tmpAvailabilityGroups)

  IF @ErrorMessage IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The following availability groups do not exist: ' + @ErrorMessage + '.', 10, 1)
  END

  SELECT @ErrorMessage = STRING_AGG(CAST(QUOTENAME(DatabaseName) AS nvarchar(max)), ', ')
                         WITHIN GROUP (ORDER BY DatabaseName ASC)
  FROM @SelectedFileGroups
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName IN (SELECT DatabaseName FROM @tmpDatabases)
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases WHERE Selected = 1)

  IF @ErrorMessage IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The following databases have been selected in the @FileGroups parameter, but not in the @Databases or @AvailabilityGroups parameters: ' + @ErrorMessage + '.', 10, 1)
  END

  SELECT @ErrorMessage = STRING_AGG(CAST(QUOTENAME(DatabaseName) AS nvarchar(max)), ', ')
                         WITHIN GROUP (ORDER BY DatabaseName ASC)
  FROM @SelectedObjects
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName IN (SELECT DatabaseName FROM @tmpDatabases)
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases WHERE Selected = 1)

  IF @ErrorMessage IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The following databases have been selected in the @Objects parameter, but not in the @Databases or @AvailabilityGroups parameters: ' + @ErrorMessage + '.', 10, 1)
  END

  ----------------------------------------------------------------------------------------------------
  --// Check @@SERVERNAME                                                                         //--
  ----------------------------------------------------------------------------------------------------

  IF UPPER(@@SERVERNAME) <> UPPER(@ServerName) AND @IsHadrEnabled = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The @@SERVERNAME does not match SERVERPROPERTY(''ServerName''). See ' + CASE WHEN @IsClustered = 0 THEN 'https://docs.microsoft.com/en-us/sql/database-engine/install-windows/rename-a-computer-that-hosts-a-stand-alone-instance-of-sql-server' WHEN @IsClustered = 1 THEN 'https://docs.microsoft.com/en-us/sql/sql-server/failover-clusters/install/rename-a-sql-server-failover-cluster-instance' END + '.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------
  --// Raise errors                                                                               //--
  ----------------------------------------------------------------------------------------------------

  DECLARE ErrorCursor CURSOR LOCAL FAST_FORWARD FOR SELECT [Message], Severity, [State] FROM @Errors ORDER BY [ID] ASC

  OPEN ErrorCursor

  FETCH ErrorCursor INTO @CurrentMessage, @CurrentSeverity, @CurrentState

  WHILE @@FETCH_STATUS = 0
  BEGIN
    RAISERROR('%s', @CurrentSeverity, @CurrentState, @CurrentMessage) WITH NOWAIT
    RAISERROR(@EmptyLine, 10, 1) WITH NOWAIT

    FETCH NEXT FROM ErrorCursor INTO @CurrentMessage, @CurrentSeverity, @CurrentState
  END

  CLOSE ErrorCursor

  DEALLOCATE ErrorCursor

  IF EXISTS (SELECT * FROM @Errors WHERE Severity >= 16)
  BEGIN
    SET @ReturnCode = 50000
    GOTO Logging
  END

  ----------------------------------------------------------------------------------------------------
  --// Update database order                                                                      //--
  ----------------------------------------------------------------------------------------------------

  IF @DatabaseOrder IN('DATABASE_SIZE_ASC','DATABASE_SIZE_DESC')
  BEGIN
    UPDATE tmpDatabases
    SET DatabaseSize = (SELECT SUM(CAST(size AS bigint)) FROM sys.master_files WHERE [type] = 0 AND database_id = DB_ID(tmpDatabases.DatabaseName))
    FROM @tmpDatabases tmpDatabases
  END

  IF @DatabaseOrder IN('DATABASE_LAST_GOOD_CHECK_ASC','DATABASE_LAST_GOOD_CHECK_DESC')
  BEGIN
    UPDATE tmpDatabases
    SET LastGoodCheckDbTime = NULLIF(CAST(DATABASEPROPERTYEX (DatabaseName,'LastGoodCheckDbTime') AS datetime2),'1900-01-01 00:00:00.000')
    FROM @tmpDatabases tmpDatabases
  END

  IF @DatabaseOrder IN('REPLICA_LAST_GOOD_CHECK_ASC','REPLICA_LAST_GOOD_CHECK_DESC')
  BEGIN
    UPDATE tmpDatabases
    SET LastCommandTime = MaxStartTime
    FROM @tmpDatabases tmpDatabases
    INNER JOIN (SELECT DatabaseName, MAX(StartTime) AS MaxStartTime
                FROM dbo.CommandLog
                WHERE CommandType = 'DBCC_CHECKDB'
                AND ErrorNumber = 0
                GROUP BY DatabaseName) CommandLog
    ON tmpDatabases.DatabaseName = CommandLog.DatabaseName COLLATE DATABASE_DEFAULT
  END

  IF @DatabaseOrder IS NULL
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY StartPosition ASC, DatabaseName ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_NAME_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseName ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_NAME_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseName DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_SIZE_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseSize ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_SIZE_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseSize DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_LAST_GOOD_CHECK_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY LastGoodCheckDbTime ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_LAST_GOOD_CHECK_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY LastGoodCheckDbTime DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'REPLICA_LAST_GOOD_CHECK_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY LastCommandTime ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'REPLICA_LAST_GOOD_CHECK_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY LastCommandTime DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END

  ----------------------------------------------------------------------------------------------------
  --// Update the queue                                                                           //--
  ----------------------------------------------------------------------------------------------------

  IF @DatabasesInParallel = 'Y'
  BEGIN

    BEGIN TRY

      SELECT @QueueID = QueueID
      FROM dbo.[Queue]
      WHERE SchemaName = @SchemaName
      AND ObjectName = @ObjectName
      AND [Parameters] = @ParametersString

      IF @QueueID IS NULL
      BEGIN
        BEGIN TRANSACTION

        SELECT @QueueID = QueueID
        FROM dbo.[Queue] WITH (UPDLOCK, HOLDLOCK)
        WHERE SchemaName = @SchemaName
        AND ObjectName = @ObjectName
        AND [Parameters] = @ParametersString

        IF @QueueID IS NULL
        BEGIN
          INSERT INTO dbo.[Queue] (SchemaName, ObjectName, [Parameters])
          VALUES(@SchemaName, @ObjectName, @ParametersString)

          SET @QueueID = SCOPE_IDENTITY()
        END

        COMMIT TRANSACTION
      END

      BEGIN TRANSACTION

      UPDATE [Queue]
      SET QueueStartTime = SYSDATETIME(),
          SessionID = @@SPID,
          RequestID = (SELECT request_id FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          RequestStartTime = (SELECT start_time FROM sys.dm_exec_requests WHERE session_id = @@SPID)
      FROM dbo.[Queue] [Queue]
      WHERE QueueID = @QueueID
      AND NOT EXISTS (SELECT *
                      FROM sys.dm_exec_requests
                      WHERE session_id = [Queue].SessionID
                      AND request_id = [Queue].RequestID
                      AND start_time = [Queue].RequestStartTime)
      AND NOT EXISTS (SELECT *
                      FROM dbo.QueueDatabase QueueDatabase
                      INNER JOIN sys.dm_exec_requests ON QueueDatabase.SessionID = session_id AND QueueDatabase.RequestID = request_id AND QueueDatabase.RequestStartTime = start_time
                      WHERE QueueDatabase.QueueID = @QueueID)

      IF @@ROWCOUNT = 1
      BEGIN
        INSERT INTO dbo.QueueDatabase (QueueID, DatabaseName)
        SELECT @QueueID AS QueueID,
               DatabaseName
        FROM @tmpDatabases tmpDatabases
        WHERE Selected = 1
        AND NOT EXISTS (SELECT * FROM dbo.QueueDatabase WHERE DatabaseName COLLATE DATABASE_DEFAULT = tmpDatabases.DatabaseName AND QueueID = @QueueID)

        DELETE QueueDatabase
        FROM dbo.QueueDatabase QueueDatabase
        WHERE QueueID = @QueueID
        AND NOT EXISTS (SELECT * FROM @tmpDatabases tmpDatabases WHERE DatabaseName = QueueDatabase.DatabaseName COLLATE DATABASE_DEFAULT AND Selected = 1)

        UPDATE QueueDatabase
        SET DatabaseOrder = tmpDatabases.[Order]
        FROM dbo.QueueDatabase QueueDatabase
        INNER JOIN @tmpDatabases tmpDatabases ON QueueDatabase.DatabaseName COLLATE DATABASE_DEFAULT = tmpDatabases.DatabaseName
        WHERE QueueID = @QueueID
      END

      COMMIT TRANSACTION

      SELECT @QueueStartTime = QueueStartTime
      FROM dbo.[Queue]
      WHERE QueueID = @QueueID

    END TRY

    BEGIN CATCH
      IF XACT_STATE() <> 0
      BEGIN
        ROLLBACK TRANSACTION
      END
      SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar(max)) + ', ' + ISNULL(ERROR_MESSAGE(),'')
      RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
      RAISERROR(@EmptyLine,10,1) WITH NOWAIT
      SET @ReturnCode = ERROR_NUMBER()
      GOTO Logging
    END CATCH

  END

  ----------------------------------------------------------------------------------------------------
  --// Execute commands                                                                           //--
  ----------------------------------------------------------------------------------------------------

  WHILE (1 = 1)
  BEGIN -- Start of database loop

    IF @DatabasesInParallel = 'Y'
    BEGIN
      UPDATE QueueDatabase
      SET DatabaseStartTime = NULL,
          SessionID = NULL,
          RequestID = NULL,
          RequestStartTime = NULL
      FROM dbo.QueueDatabase QueueDatabase
      WHERE QueueID = @QueueID
      AND DatabaseStartTime IS NOT NULL
      AND DatabaseEndTime IS NULL
      AND NOT EXISTS (SELECT * FROM sys.dm_exec_requests WHERE session_id = QueueDatabase.SessionID AND request_id = QueueDatabase.RequestID AND start_time = QueueDatabase.RequestStartTime)

      UPDATE QueueDatabase
      SET DatabaseStartTime = SYSDATETIME(),
          DatabaseEndTime = NULL,
          SessionID = @@SPID,
          RequestID = (SELECT request_id FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          RequestStartTime = (SELECT start_time FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          @CurrentDatabaseName = DatabaseName
      FROM (SELECT TOP 1 DatabaseStartTime,
                         DatabaseEndTime,
                         SessionID,
                         RequestID,
                         RequestStartTime,
                         DatabaseName
            FROM dbo.QueueDatabase
            WHERE QueueID = @QueueID
            AND (DatabaseStartTime < @QueueStartTime OR DatabaseStartTime IS NULL)
            AND NOT (DatabaseStartTime IS NOT NULL AND DatabaseEndTime IS NULL)
            ORDER BY DatabaseOrder ASC
            ) QueueDatabase
    END
    ELSE
    BEGIN
      SELECT TOP 1 @CurrentDBID = ID,
                   @CurrentDatabaseName = DatabaseName
      FROM @tmpDatabases
      WHERE Selected = 1
      AND Completed = 0
      ORDER BY [Order] ASC
    END

    IF @@ROWCOUNT = 0
    BEGIN
      BREAK
    END

    SET @CurrentDatabase_sp_executesql = QUOTENAME(@CurrentDatabaseName) + '.sys.sp_executesql'

    BEGIN
      SET @DatabaseMessage = 'Date and time: ' + CONVERT(nvarchar(max),SYSDATETIME(),120)
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Database: ' + QUOTENAME(@CurrentDatabaseName)
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    SELECT @CurrentUserAccess = user_access_desc,
           @CurrentIsReadOnly = is_read_only,
           @CurrentDatabaseState = state_desc,
           @CurrentInStandby = is_in_standby,
           @CurrentRecoveryModel = recovery_model_desc
    FROM sys.databases
    WHERE [name] = @CurrentDatabaseName

    BEGIN
      SET @DatabaseMessage = 'State: ' + @CurrentDatabaseState
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Standby: ' + CASE WHEN @CurrentInStandby = 1 THEN 'Yes' ELSE 'No' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Updateability: ' + CASE WHEN @CurrentIsReadOnly = 1 THEN 'READ_ONLY' WHEN  @CurrentIsReadOnly = 0 THEN 'READ_WRITE' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'User access: ' + @CurrentUserAccess
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Recovery model: ' + @CurrentRecoveryModel
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    IF @IsHadrEnabled = 1
    BEGIN
      SELECT @CurrentAvailabilityGroupReplicaID = databases.replica_id
      FROM sys.databases databases
      INNER JOIN sys.availability_replicas availability_replicas ON databases.replica_id = availability_replicas.replica_id
      WHERE databases.[name] = @CurrentDatabaseName

      SELECT @CurrentAvailabilityGroupID = group_id,
             @CurrentSecondaryRoleAllowConnections = secondary_role_allow_connections_desc
      FROM sys.availability_replicas
      WHERE replica_id = @CurrentAvailabilityGroupReplicaID

      SELECT @CurrentAvailabilityGroupRole = role_desc
      FROM sys.dm_hadr_availability_replica_states
      WHERE replica_id = @CurrentAvailabilityGroupReplicaID

      SELECT @CurrentAvailabilityGroup = [name],
             @CurrentAvailabilityGroupBackupPreference = UPPER(automated_backup_preference_desc)
      FROM sys.availability_groups
      WHERE group_id = @CurrentAvailabilityGroupID
    END

    IF @IsHadrEnabled = 1 AND @CurrentAvailabilityGroup IS NOT NULL AND @AvailabilityGroupReplicas = 'PREFERRED_BACKUP_REPLICA'
    BEGIN
      SELECT @CurrentIsPreferredBackupReplica = sys.fn_hadr_backup_is_preferred_replica(@CurrentDatabaseName)
    END

    IF @IsHadrEnabled = 1 AND @CurrentAvailabilityGroup IS NOT NULL
    BEGIN
      SELECT @CurrentDistributedAvailabilityGroup = availability_groups.[name],
             @CurrentDistributedAvailabilityGroupReplicaID = availability_replicas.replica_id
      FROM sys.availability_groups availability_groups
      INNER JOIN sys.availability_replicas availability_replicas ON availability_groups.group_id = availability_replicas.group_id
      INNER JOIN sys.availability_groups availability_groups_local ON availability_replicas.replica_server_name = availability_groups_local.[name]
      WHERE availability_groups.is_distributed = 1
      AND availability_groups_local.group_id = @CurrentAvailabilityGroupID

      SELECT @CurrentDistributedAvailabilityGroupRole = dm_hadr_availability_replica_states.role_desc
      FROM sys.dm_hadr_availability_replica_states dm_hadr_availability_replica_states
      WHERE dm_hadr_availability_replica_states.replica_id = @CurrentDistributedAvailabilityGroupReplicaID
    END

    IF @EngineEdition <> 5
    BEGIN
      SELECT @CurrentDatabaseMirroringRole = UPPER(mirroring_role_desc)
      FROM sys.database_mirroring database_mirroring
      INNER JOIN sys.databases databases ON database_mirroring.database_id = databases.database_id
      WHERE databases.[name] = @CurrentDatabaseName
    END

    IF @CurrentAvailabilityGroup IS NOT NULL
    BEGIN
      SET @DatabaseMessage =  'Availability group: ' + ISNULL(@CurrentAvailabilityGroup,'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Availability group role: ' + ISNULL(@CurrentAvailabilityGroupRole,'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      IF @CurrentAvailabilityGroupRole = 'SECONDARY'
      BEGIN
        SET @DatabaseMessage =  'Readable Secondary: ' + ISNULL(@CurrentSecondaryRoleAllowConnections,'N/A')
        RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
      END

      IF @AvailabilityGroupReplicas = 'PREFERRED_BACKUP_REPLICA'
      BEGIN
        SET @DatabaseMessage = 'Availability group backup preference: ' + ISNULL(@CurrentAvailabilityGroupBackupPreference,'N/A')
        RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

        SET @DatabaseMessage = 'Is preferred backup replica: ' + CASE WHEN @CurrentIsPreferredBackupReplica = 1 THEN 'Yes' WHEN @CurrentIsPreferredBackupReplica = 0 THEN 'No' ELSE 'N/A' END
        RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
      END
    END

    IF @CurrentDistributedAvailabilityGroup IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Distributed availability group: ' + ISNULL(@CurrentDistributedAvailabilityGroup,'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Distributed availability group role: ' + ISNULL(@CurrentDistributedAvailabilityGroupRole,'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Replica role in distributed availability group: ' + CASE WHEN @CurrentDistributedAvailabilityGroupRole = 'PRIMARY' AND @CurrentAvailabilityGroupRole = 'PRIMARY' THEN 'Global primary'
                                                                                       WHEN @CurrentDistributedAvailabilityGroupRole = 'SECONDARY' AND @CurrentAvailabilityGroupRole = 'PRIMARY' THEN 'Forwarder'
                                                                                       WHEN @CurrentDistributedAvailabilityGroupRole = 'SECONDARY' AND @CurrentAvailabilityGroupRole = 'SECONDARY' THEN 'Secondary replica in secondary availability group'
                                                                                       WHEN @CurrentDistributedAvailabilityGroupRole = 'PRIMARY' AND @CurrentAvailabilityGroupRole = 'SECONDARY' THEN 'Secondary replica in primary availability group' ELSE 'N/A' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    IF @CurrentDatabaseMirroringRole IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Database mirroring role: ' + @CurrentDatabaseMirroringRole
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    RAISERROR(@EmptyLine,10,1) WITH NOWAIT

    IF @CurrentDatabaseState IN('ONLINE','EMERGENCY')
    AND NOT (@CurrentUserAccess = 'SINGLE_USER')
    AND (@CurrentAvailabilityGroupRole = 'PRIMARY' OR @CurrentAvailabilityGroupRole IS NULL OR @EngineEdition = 3)
    AND ((@AvailabilityGroupReplicas = 'PRIMARY' AND @CurrentAvailabilityGroupRole = 'PRIMARY') OR (@AvailabilityGroupReplicas = 'SECONDARY' AND @CurrentAvailabilityGroupRole = 'SECONDARY') OR (@AvailabilityGroupReplicas = 'PREFERRED_BACKUP_REPLICA' AND @CurrentIsPreferredBackupReplica = 1) OR @AvailabilityGroupReplicas = 'ALL' OR @CurrentAvailabilityGroupRole IS NULL)
    AND NOT (@CurrentIsReadOnly = 1 AND @Updateability = 'READ_WRITE')
    AND NOT (@CurrentIsReadOnly = 0 AND @Updateability = 'READ_ONLY')
    AND NOT (@AmazonRDS = 1 AND @CurrentDatabaseName = 'rdsadmin')
    BEGIN

      -- Check database
      IF EXISTS(SELECT * FROM @SelectedCheckCommands WHERE CheckCommand = 'CHECKDB') AND (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
      BEGIN
        SET @CurrentDatabaseContext = CASE WHEN @EngineEdition = 5 THEN @CurrentDatabaseName ELSE 'master' END

        SET @CurrentCommandType = 'DBCC_CHECKDB'

        SET @CurrentCommand = ''
        IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar(max)) + '; '
        SET @CurrentCommand += 'DBCC CHECKDB (' + QUOTENAME(@CurrentDatabaseName)
        IF @NoIndex = 'Y' SET @CurrentCommand += ', NOINDEX'
        SET @CurrentCommand += ') WITH ALL_ERRORMSGS'
        IF @DataPurity = 'Y' SET @CurrentCommand += ', DATA_PURITY'
        IF @PhysicalOnly = 'Y' SET @CurrentCommand += ', PHYSICAL_ONLY'
        IF @ExtendedLogicalChecks = 'Y' SET @CurrentCommand += ', EXTENDED_LOGICAL_CHECKS'
        IF @NoInformationalMessages = 'Y' SET @CurrentCommand += ', NO_INFOMSGS'
        IF @TabLock = 'Y' SET @CurrentCommand += ', TABLOCK'
        IF @MaxDOP IS NOT NULL SET @CurrentCommand += ', MAXDOP = ' + CAST(@MaxDOP AS nvarchar(max))

        EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseContext, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
        SET @Error = @@ERROR
        IF @Error <> 0 SET @CurrentCommandOutput = @Error
        IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput
      END

      -- Check filegroups
      IF EXISTS(SELECT * FROM @SelectedCheckCommands WHERE CheckCommand = 'CHECKFILEGROUP')
      AND (@CurrentAvailabilityGroupRole = 'PRIMARY' OR (@CurrentAvailabilityGroupRole = 'SECONDARY' AND @CurrentSecondaryRoleAllowConnections = 'ALL') OR @CurrentAvailabilityGroupRole IS NULL)
      AND (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
      BEGIN
        SET @CurrentCommand = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; SELECT data_space_id AS FileGroupID, name AS FileGroupName FROM sys.filegroups filegroups WHERE [type] <> ''FX'' ORDER BY CASE WHEN filegroups.name = ''PRIMARY'' THEN 1 ELSE 0 END DESC, filegroups.name ASC'

        INSERT INTO @tmpFileGroups (FileGroupID, FileGroupName)
        EXECUTE @CurrentDatabase_sp_executesql  @stmt = @CurrentCommand
        SET @Error = @@ERROR
        IF @Error <> 0 SET @ReturnCode = @Error

        IF @FileGroups IS NULL
        BEGIN
          UPDATE tmpFileGroups
          SET tmpFileGroups.Selected = 1
          FROM @tmpFileGroups tmpFileGroups
        END
        ELSE
        BEGIN
          UPDATE tmpFileGroups
          SET tmpFileGroups.Selected = SelectedFileGroups.Selected
          FROM @tmpFileGroups tmpFileGroups
          INNER JOIN @SelectedFileGroups SelectedFileGroups
          ON @CurrentDatabaseName LIKE REPLACE(REPLACE(SelectedFileGroups.DatabaseName,'[','[[]'),'_','[_]') AND tmpFileGroups.FileGroupName LIKE REPLACE(REPLACE(SelectedFileGroups.FileGroupName,'[','[[]'),'_','[_]')
          WHERE SelectedFileGroups.Selected = 1

          UPDATE tmpFileGroups
          SET tmpFileGroups.Selected = SelectedFileGroups.Selected
          FROM @tmpFileGroups tmpFileGroups
          INNER JOIN @SelectedFileGroups SelectedFileGroups
          ON @CurrentDatabaseName LIKE REPLACE(REPLACE(SelectedFileGroups.DatabaseName,'[','[[]'),'_','[_]') AND tmpFileGroups.FileGroupName LIKE REPLACE(REPLACE(SelectedFileGroups.FileGroupName,'[','[[]'),'_','[_]')
          WHERE SelectedFileGroups.Selected = 0

          UPDATE tmpFileGroups
          SET tmpFileGroups.StartPosition = SelectedFileGroups2.StartPosition
          FROM @tmpFileGroups tmpFileGroups
          INNER JOIN (SELECT tmpFileGroups.FileGroupName, MIN(SelectedFileGroups.StartPosition) AS StartPosition
                      FROM @tmpFileGroups tmpFileGroups
                      INNER JOIN @SelectedFileGroups SelectedFileGroups
                      ON @CurrentDatabaseName LIKE REPLACE(REPLACE(SelectedFileGroups.DatabaseName,'[','[[]'),'_','[_]') AND tmpFileGroups.FileGroupName LIKE REPLACE(REPLACE(SelectedFileGroups.FileGroupName,'[','[[]'),'_','[_]')
                      WHERE SelectedFileGroups.Selected = 1
                      GROUP BY tmpFileGroups.FileGroupName) SelectedFileGroups2
          ON tmpFileGroups.FileGroupName = SelectedFileGroups2.FileGroupName
        END;

        WITH tmpFileGroups AS (
        SELECT FileGroupName, [Order], ROW_NUMBER() OVER (ORDER BY StartPosition ASC, FileGroupName ASC) AS RowNumber
        FROM @tmpFileGroups tmpFileGroups
        WHERE Selected = 1
        )
        UPDATE tmpFileGroups
        SET [Order] = RowNumber

        SELECT @ErrorMessage = STRING_AGG(CAST(QUOTENAME(DatabaseName) AS nvarchar(max)) + '.' + QUOTENAME(FileGroupName), ', ')
                               WITHIN GROUP (ORDER BY DatabaseName ASC, FileGroupName ASC)
        FROM @SelectedFileGroups SelectedFileGroups
        WHERE DatabaseName = @CurrentDatabaseName
        AND FileGroupName NOT LIKE '%[%]%'
        AND NOT EXISTS (SELECT * FROM @tmpFileGroups WHERE FileGroupName = SelectedFileGroups.FileGroupName)

        IF @ErrorMessage IS NOT NULL
        BEGIN
          SET @ErrorMessage = 'The following file groups do not exist: ' + @ErrorMessage + '.'
          RAISERROR('%s',10,1,@ErrorMessage) WITH NOWAIT
          RAISERROR(@EmptyLine,10,1) WITH NOWAIT
        END

        WHILE (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
        BEGIN
          SELECT TOP 1 @CurrentFGID = ID,
                       @CurrentFileGroupID = FileGroupID,
                       @CurrentFileGroupName = FileGroupName
          FROM @tmpFileGroups
          WHERE Selected = 1
          AND Completed = 0
          ORDER BY [Order] ASC

          IF @@ROWCOUNT = 0
          BEGIN
            BREAK
          END

          -- Does the filegroup exist?
          SET @CurrentCommand = ''
          IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar(max)) + '; '
          SET @CurrentCommand += 'IF EXISTS(SELECT * FROM sys.filegroups filegroups WHERE [type] <> ''FX'' AND filegroups.data_space_id = @ParamFileGroupID AND filegroups.[name] = @ParamFileGroupName) BEGIN SET @ParamFileGroupExists = 1 END'

          BEGIN TRY
            EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamFileGroupID int, @ParamFileGroupName sysname, @ParamFileGroupExists bit OUTPUT', @ParamFileGroupID = @CurrentFileGroupID, @ParamFileGroupName = @CurrentFileGroupName, @ParamFileGroupExists = @CurrentFileGroupExists OUTPUT

            IF @CurrentFileGroupExists IS NULL SET @CurrentFileGroupExists = 0
          END TRY
          BEGIN CATCH
            SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar(max)) + ', ' + ISNULL(ERROR_MESSAGE(),'') + CASE WHEN ERROR_NUMBER() = 1222 THEN ', ' + ' The file group ' + QUOTENAME(@CurrentFileGroupName) + ' in the database ' + QUOTENAME(@CurrentDatabaseName) + ' is locked. It could not be checked if the filegroup exists.' ELSE '' END
            SET @Severity = CASE WHEN ERROR_NUMBER() IN(1205,1222) THEN @LockMessageSeverity ELSE 16 END
            RAISERROR('%s',@Severity,1,@ErrorMessage) WITH NOWAIT
            RAISERROR(@EmptyLine,10,1) WITH NOWAIT

            IF NOT (ERROR_NUMBER() IN(1205,1222) AND @LockMessageSeverity = 10)
            BEGIN
              SET @ReturnCode = ERROR_NUMBER()
            END
          END CATCH

          IF @CurrentFileGroupExists = 1
          BEGIN
            SET @CurrentDatabaseContext = @CurrentDatabaseName

            SET @CurrentCommandType = 'DBCC_CHECKFILEGROUP'

            SET @CurrentCommand = ''
            IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar(max)) + '; '
            SET @CurrentCommand += 'DBCC CHECKFILEGROUP (' + QUOTENAME(@CurrentFileGroupName)
            IF @NoIndex = 'Y' SET @CurrentCommand += ', NOINDEX'
            SET @CurrentCommand += ') WITH ALL_ERRORMSGS'
            IF @PhysicalOnly = 'Y' SET @CurrentCommand += ', PHYSICAL_ONLY'
            IF @NoInformationalMessages = 'Y' SET @CurrentCommand += ', NO_INFOMSGS'
            IF @TabLock = 'Y' SET @CurrentCommand += ', TABLOCK'
            IF @MaxDOP IS NOT NULL SET @CurrentCommand += ', MAXDOP = ' + CAST(@MaxDOP AS nvarchar(max))

            EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseContext, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
            SET @Error = @@ERROR
            IF @Error <> 0 SET @CurrentCommandOutput = @Error
            IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput
          END

          UPDATE @tmpFileGroups
          SET Completed = 1
          WHERE Selected = 1
          AND Completed = 0
          AND ID = @CurrentFGID

          SET @CurrentFGID = NULL
          SET @CurrentFileGroupID = NULL
          SET @CurrentFileGroupName = NULL
          SET @CurrentFileGroupExists = NULL

          SET @CurrentDatabaseContext = NULL
          SET @CurrentCommand = NULL
          SET @CurrentCommandOutput = NULL
          SET @CurrentCommandType = NULL
        END
      END

      -- Check disk space allocation structures
      IF EXISTS(SELECT * FROM @SelectedCheckCommands WHERE CheckCommand = 'CHECKALLOC') AND (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
      BEGIN
        SET @CurrentDatabaseContext = CASE WHEN @EngineEdition = 5 THEN @CurrentDatabaseName ELSE 'master' END

        SET @CurrentCommandType = 'DBCC_CHECKALLOC'

        SET @CurrentCommand = ''
        IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar(max)) + '; '
        SET @CurrentCommand += 'DBCC CHECKALLOC (' + QUOTENAME(@CurrentDatabaseName)
        IF @NoIndex = 'Y' SET @CurrentCommand += ', NOINDEX'
        SET @CurrentCommand += ') WITH ALL_ERRORMSGS'
        IF @NoInformationalMessages = 'Y' SET @CurrentCommand += ', NO_INFOMSGS'
        IF @TabLock = 'Y' SET @CurrentCommand += ', TABLOCK'

        EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseContext, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
        SET @Error = @@ERROR
        IF @Error <> 0 SET @CurrentCommandOutput = @Error
        IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput
      END

      -- Check objects
      IF EXISTS(SELECT * FROM @SelectedCheckCommands WHERE CheckCommand = 'CHECKTABLE')
      AND (@CurrentAvailabilityGroupRole = 'PRIMARY' OR (@CurrentAvailabilityGroupRole = 'SECONDARY' AND @CurrentSecondaryRoleAllowConnections = 'ALL') OR @CurrentAvailabilityGroupRole IS NULL)
      AND (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
      BEGIN
        SET @CurrentCommand = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; SELECT schemas.[schema_id] AS SchemaID, schemas.[name] AS SchemaName, objects.[object_id] AS ObjectID, objects.[name] AS ObjectName, RTRIM(objects.[type]) AS ObjectType FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.schema_id = schemas.schema_id LEFT OUTER JOIN sys.tables tables ON objects.object_id = tables.object_id WHERE objects.[type] IN(''U'',''V'') AND EXISTS(SELECT * FROM sys.indexes indexes WHERE indexes.object_id = objects.object_id) AND (tables.is_memory_optimized = 0 OR is_memory_optimized IS NULL) ORDER BY schemas.name ASC, objects.name ASC'

        INSERT INTO @tmpObjects (SchemaID, SchemaName, ObjectID, ObjectName, ObjectType)
        EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand
        SET @Error = @@ERROR
        IF @Error <> 0 SET @ReturnCode = @Error

        IF @Objects IS NULL
        BEGIN
          UPDATE tmpObjects
          SET tmpObjects.Selected = 1
          FROM @tmpObjects tmpObjects
        END
        ELSE
        BEGIN
          UPDATE tmpObjects
          SET tmpObjects.Selected = SelectedObjects.Selected
          FROM @tmpObjects tmpObjects
          INNER JOIN @SelectedObjects SelectedObjects
          ON @CurrentDatabaseName LIKE REPLACE(REPLACE(SelectedObjects.DatabaseName,'[','[[]'),'_','[_]') AND tmpObjects.SchemaName LIKE REPLACE(REPLACE(SelectedObjects.SchemaName,'[','[[]'),'_','[_]') AND tmpObjects.ObjectName LIKE REPLACE(REPLACE(SelectedObjects.ObjectName,'[','[[]'),'_','[_]')
          WHERE SelectedObjects.Selected = 1

          UPDATE tmpObjects
          SET tmpObjects.Selected = SelectedObjects.Selected
          FROM @tmpObjects tmpObjects
          INNER JOIN @SelectedObjects SelectedObjects
          ON @CurrentDatabaseName LIKE REPLACE(REPLACE(SelectedObjects.DatabaseName,'[','[[]'),'_','[_]') AND tmpObjects.SchemaName LIKE REPLACE(REPLACE(SelectedObjects.SchemaName,'[','[[]'),'_','[_]') AND tmpObjects.ObjectName LIKE REPLACE(REPLACE(SelectedObjects.ObjectName,'[','[[]'),'_','[_]')
          WHERE SelectedObjects.Selected = 0

          UPDATE tmpObjects
          SET tmpObjects.StartPosition = SelectedObjects2.StartPosition
          FROM @tmpObjects tmpObjects
          INNER JOIN (SELECT tmpObjects.SchemaName, tmpObjects.ObjectName, MIN(SelectedObjects.StartPosition) AS StartPosition
                      FROM @tmpObjects tmpObjects
                      INNER JOIN @SelectedObjects SelectedObjects
                      ON @CurrentDatabaseName LIKE REPLACE(REPLACE(SelectedObjects.DatabaseName,'[','[[]'),'_','[_]') AND tmpObjects.SchemaName LIKE REPLACE(REPLACE(SelectedObjects.SchemaName,'[','[[]'),'_','[_]') AND tmpObjects.ObjectName LIKE REPLACE(REPLACE(SelectedObjects.ObjectName,'[','[[]'),'_','[_]')
                      WHERE SelectedObjects.Selected = 1
                      GROUP BY tmpObjects.SchemaName, tmpObjects.ObjectName) SelectedObjects2
          ON tmpObjects.SchemaName = SelectedObjects2.SchemaName AND tmpObjects.ObjectName = SelectedObjects2.ObjectName
        END;

        WITH tmpObjects AS (
        SELECT SchemaName, ObjectName, [Order], ROW_NUMBER() OVER (ORDER BY StartPosition ASC, SchemaName ASC, ObjectName ASC) AS RowNumber
        FROM @tmpObjects tmpObjects
        WHERE Selected = 1
        )
        UPDATE tmpObjects
        SET [Order] = RowNumber

        SELECT @ErrorMessage = STRING_AGG(CAST(QUOTENAME(DatabaseName) AS nvarchar(max)) + '.' + QUOTENAME(SchemaName) + '.' + QUOTENAME(ObjectName), ', ')
                               WITHIN GROUP (ORDER BY DatabaseName ASC, SchemaName ASC, ObjectName ASC)
        FROM @SelectedObjects SelectedObjects
        WHERE DatabaseName = @CurrentDatabaseName
        AND SchemaName NOT LIKE '%[%]%'
        AND ObjectName NOT LIKE '%[%]%'
        AND NOT EXISTS (SELECT * FROM @tmpObjects WHERE SchemaName = SelectedObjects.SchemaName AND ObjectName = SelectedObjects.ObjectName)

        IF @ErrorMessage IS NOT NULL
        BEGIN
          SET @ErrorMessage = 'The following objects do not exist: ' + @ErrorMessage + '.'
          RAISERROR('%s',10,1,@ErrorMessage) WITH NOWAIT
          RAISERROR(@EmptyLine,10,1) WITH NOWAIT
        END

        WHILE (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
        BEGIN
          SELECT TOP 1 @CurrentOID = ID,
                       @CurrentSchemaID = SchemaID,
                       @CurrentSchemaName = SchemaName,
                       @CurrentObjectID = ObjectID,
                       @CurrentObjectName = ObjectName,
                       @CurrentObjectType = ObjectType
          FROM @tmpObjects
          WHERE Selected = 1
          AND Completed = 0
          ORDER BY [Order] ASC

          IF @@ROWCOUNT = 0
          BEGIN
            BREAK
          END

          -- Does the object exist?
          SET @CurrentCommand = ''
          IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar(max)) + '; '
          SET @CurrentCommand += 'IF EXISTS(SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.schema_id = schemas.schema_id LEFT OUTER JOIN sys.tables tables ON objects.object_id = tables.object_id WHERE objects.[type] IN(''U'',''V'') AND EXISTS(SELECT * FROM sys.indexes indexes WHERE indexes.object_id = objects.object_id) AND (tables.is_memory_optimized = 0 OR is_memory_optimized IS NULL) AND schemas.[schema_id] = @ParamSchemaID AND schemas.[name] = @ParamSchemaName AND objects.[object_id] = @ParamObjectID AND objects.[name] = @ParamObjectName AND objects.[type] = @ParamObjectType) BEGIN SET @ParamObjectExists = 1 END'

          BEGIN TRY
            EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamSchemaID int, @ParamSchemaName sysname, @ParamObjectID int, @ParamObjectName sysname, @ParamObjectType sysname, @ParamObjectExists bit OUTPUT', @ParamSchemaID = @CurrentSchemaID, @ParamSchemaName = @CurrentSchemaName, @ParamObjectID = @CurrentObjectID, @ParamObjectName = @CurrentObjectName, @ParamObjectType = @CurrentObjectType, @ParamObjectExists = @CurrentObjectExists OUTPUT

            IF @CurrentObjectExists IS NULL SET @CurrentObjectExists = 0
          END TRY
          BEGIN CATCH
            SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar(max)) + ', ' + ISNULL(ERROR_MESSAGE(),'') + CASE WHEN ERROR_NUMBER() = 1222 THEN ', ' + 'The object ' + QUOTENAME(@CurrentDatabaseName) + '.' + QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName) + ' is locked. It could not be checked if the object exists.' ELSE '' END
            SET @Severity = CASE WHEN ERROR_NUMBER() IN(1205,1222) THEN @LockMessageSeverity ELSE 16 END
            RAISERROR('%s',@Severity,1,@ErrorMessage) WITH NOWAIT
            RAISERROR(@EmptyLine,10,1) WITH NOWAIT

            IF NOT (ERROR_NUMBER() IN(1205,1222) AND @LockMessageSeverity = 10)
            BEGIN
              SET @ReturnCode = ERROR_NUMBER()
            END
          END CATCH

          IF @CurrentObjectExists = 1
          BEGIN
            SET @CurrentDatabaseContext = @CurrentDatabaseName

            SET @CurrentCommandType = 'DBCC_CHECKTABLE'

            SET @CurrentCommand = ''
            IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar(max)) + '; '
            SET @CurrentCommand += 'DBCC CHECKTABLE (N''' + REPLACE(QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName),'''','''''') + ''''
            IF @NoIndex = 'Y' SET @CurrentCommand += ', NOINDEX'
            SET @CurrentCommand += ') WITH ALL_ERRORMSGS'
            IF @DataPurity = 'Y' SET @CurrentCommand += ', DATA_PURITY'
            IF @PhysicalOnly = 'Y' SET @CurrentCommand += ', PHYSICAL_ONLY'
            IF @ExtendedLogicalChecks = 'Y' SET @CurrentCommand += ', EXTENDED_LOGICAL_CHECKS'
            IF @NoInformationalMessages = 'Y' SET @CurrentCommand += ', NO_INFOMSGS'
            IF @TabLock = 'Y' SET @CurrentCommand += ', TABLOCK'
            IF @MaxDOP IS NOT NULL SET @CurrentCommand += ', MAXDOP = ' + CAST(@MaxDOP AS nvarchar(max))

            EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseContext, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @SchemaName = @CurrentSchemaName, @ObjectName = @CurrentObjectName, @ObjectType = @CurrentObjectType, @LogToTable = @LogToTable, @Execute = @Execute
            SET @Error = @@ERROR
            IF @Error <> 0 SET @CurrentCommandOutput = @Error
            IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput
          END

          UPDATE @tmpObjects
          SET Completed = 1
          WHERE Selected = 1
          AND Completed = 0
          AND ID = @CurrentOID

          SET @CurrentOID = NULL
          SET @CurrentSchemaID = NULL
          SET @CurrentSchemaName = NULL
          SET @CurrentObjectID = NULL
          SET @CurrentObjectName = NULL
          SET @CurrentObjectType = NULL
          SET @CurrentObjectExists = NULL

          SET @CurrentDatabaseContext = NULL
          SET @CurrentCommand = NULL
          SET @CurrentCommandOutput = NULL
          SET @CurrentCommandType = NULL
        END
      END

      -- Check catalog
      IF EXISTS(SELECT * FROM @SelectedCheckCommands WHERE CheckCommand = 'CHECKCATALOG') AND (@CurrentAvailabilityGroupRole = 'PRIMARY' OR @CurrentAvailabilityGroupRole IS NULL) AND (@CurrentDistributedAvailabilityGroupRole = 'PRIMARY' OR @CurrentDistributedAvailabilityGroupRole IS NULL) AND (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
      BEGIN
        SET @CurrentDatabaseContext = CASE WHEN @EngineEdition = 5 THEN @CurrentDatabaseName ELSE 'master' END

        SET @CurrentCommandType = 'DBCC_CHECKCATALOG'

        SET @CurrentCommand = ''
        IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar(max)) + '; '
        SET @CurrentCommand += 'DBCC CHECKCATALOG (' + QUOTENAME(@CurrentDatabaseName)
        SET @CurrentCommand += ')'
        IF @NoInformationalMessages = 'Y' SET @CurrentCommand += ' WITH NO_INFOMSGS'

        EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseContext, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
        SET @Error = @@ERROR
        IF @Error <> 0 SET @CurrentCommandOutput = @Error
        IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput
      END

    END

    IF @CurrentDatabaseState = 'SUSPECT'
    BEGIN
      SET @ErrorMessage = 'The database ' + QUOTENAME(@CurrentDatabaseName) + ' is in a SUSPECT state.'
      RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
      SET @Error = @@ERROR
      SET @ReturnCode = @Error
      RAISERROR(@EmptyLine,10,1) WITH NOWAIT
    END

    -- Update that the database is completed
    IF @DatabasesInParallel = 'Y'
    BEGIN
      UPDATE dbo.QueueDatabase
      SET DatabaseEndTime = SYSDATETIME()
      WHERE QueueID = @QueueID
      AND DatabaseName = @CurrentDatabaseName
    END
    ELSE
    BEGIN
      UPDATE @tmpDatabases
      SET Completed = 1
      WHERE Selected = 1
      AND Completed = 0
      AND ID = @CurrentDBID
    END

    -- Clear variables
    SET @CurrentDBID = NULL
    SET @CurrentDatabaseName = NULL

    SET @CurrentDatabase_sp_executesql = NULL

    SET @CurrentUserAccess = NULL
    SET @CurrentIsReadOnly = NULL
    SET @CurrentDatabaseState = NULL
    SET @CurrentInStandby = NULL
    SET @CurrentRecoveryModel = NULL

    SET @CurrentAvailabilityGroupReplicaID = NULL
    SET @CurrentAvailabilityGroupID = NULL
    SET @CurrentAvailabilityGroup = NULL
    SET @CurrentAvailabilityGroupRole = NULL
    SET @CurrentAvailabilityGroupBackupPreference = NULL
    SET @CurrentSecondaryRoleAllowConnections = NULL
    SET @CurrentIsPreferredBackupReplica = NULL
    SET @CurrentDistributedAvailabilityGroup = NULL
    SET @CurrentDistributedAvailabilityGroupReplicaID = NULL
    SET @CurrentDistributedAvailabilityGroupRole = NULL
    SET @CurrentDatabaseMirroringRole = NULL

    SET @CurrentDatabaseContext = NULL
    SET @CurrentCommand = NULL
    SET @CurrentCommandOutput = NULL
    SET @CurrentCommandType = NULL

    DELETE FROM @tmpFileGroups
    DELETE FROM @tmpObjects

  END -- End of database loop

  ----------------------------------------------------------------------------------------------------
  --// Log completing information                                                                 //--
  ----------------------------------------------------------------------------------------------------

  Logging:
  SET @EndMessage = 'Date and time: ' + CONVERT(nvarchar(max),SYSDATETIME(),120)
  RAISERROR('%s',10,1,@EndMessage) WITH NOWAIT

  RAISERROR(@EmptyLine,10,1) WITH NOWAIT

  IF @ReturnCode <> 0
  BEGIN
    RETURN @ReturnCode
  END

  ----------------------------------------------------------------------------------------------------

END

GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[IndexOptimize]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[IndexOptimize] AS'
END
GO

ALTER PROCEDURE [dbo].[IndexOptimize]

@Databases nvarchar(max) = NULL,
@FragmentationLow nvarchar(max) = NULL,
@FragmentationMedium nvarchar(max) = 'INDEX_REORGANIZE,INDEX_REBUILD_ONLINE,INDEX_REBUILD_OFFLINE',
@FragmentationHigh nvarchar(max) = 'INDEX_REBUILD_ONLINE,INDEX_REBUILD_OFFLINE',
@FragmentationLevel1 int = 5,
@FragmentationLevel2 int = 30,
@MinNumberOfPages int = 1000,
@MaxNumberOfPages int = NULL,
@SortInTempdb nvarchar(max) = 'N',
@MaxDOP int = NULL,
@FillFactor int = NULL,
@PadIndex nvarchar(max) = NULL,
@DataCompression nvarchar(max) = NULL,
@WaitAtLowPriorityMaxDuration int = NULL,
@WaitAtLowPriorityAbortAfterWait nvarchar(max) = NULL,
@Resumable nvarchar(max) = 'N',
@LOBCompaction nvarchar(max) = 'Y',
@UpdateStatistics nvarchar(max) = NULL,
@OnlyModifiedStatistics nvarchar(max) = 'N',
@StatisticsModificationLevel int = NULL,
@StatisticsSample int = NULL,
@StatisticsPersistSample nvarchar(max) = NULL,
@StatisticsResample nvarchar(max) = 'N',
@PartitionLevel nvarchar(max) = 'Y',
@MSShippedObjects nvarchar(max) = 'N',
@Indexes nvarchar(max) = NULL,
@TimeLimit int = NULL,
@Delay int = NULL,
@AvailabilityGroups nvarchar(max) = NULL,
@LockTimeout int = NULL,
@LockMessageSeverity int = 16,
@StringDelimiter nvarchar(max) = ',',
@DatabaseOrder nvarchar(max) = NULL,
@DatabasesInParallel nvarchar(max) = 'N',
@ExecuteAsUser nvarchar(max) = NULL,
@LogToTable nvarchar(max) = 'N',
@Execute nvarchar(max) = 'Y'

AS

BEGIN

  ----------------------------------------------------------------------------------------------------
  --// Source:  https://ola.hallengren.com                                                        //--
  --// License: https://ola.hallengren.com/license.html                                           //--
  --// GitHub:  https://github.com/olahallengren/sql-server-maintenance-solution                  //--
  --// Version: 2026-07-21 14:38:46                                                               //--
  ----------------------------------------------------------------------------------------------------

  SET NOCOUNT ON

  SET ARITHABORT ON

  SET NUMERIC_ROUNDABORT OFF

  DECLARE @StartMessage nvarchar(max)
  DECLARE @EndMessage nvarchar(max)
  DECLARE @DatabaseMessage nvarchar(max)
  DECLARE @ErrorMessage nvarchar(max)
  DECLARE @Severity int

  DECLARE @StartTime datetime2 = SYSDATETIME()
  DECLARE @SchemaName nvarchar(max) = OBJECT_SCHEMA_NAME(@@PROCID)
  DECLARE @ObjectName nvarchar(max) = OBJECT_NAME(@@PROCID)
  DECLARE @VersionTimestamp nvarchar(max) = SUBSTRING(OBJECT_DEFINITION(@@PROCID),CHARINDEX('--// Version: ',OBJECT_DEFINITION(@@PROCID)) + LEN('--// Version: ') + 1, 19)

  DECLARE @Parameters TABLE (ID int IDENTITY PRIMARY KEY,
                             [Name] nvarchar(max) NOT NULL,
                             ValueNvarchar nvarchar(max),
                             ValueInt int,
                             ValueDatetime datetime2)

  DECLARE @ParametersString nvarchar(max)
  DECLARE @CurrentParameterName nvarchar(max)
  DECLARE @CurrentParameterValueNvarchar nvarchar(max)
  DECLARE @CurrentParameterValueInt int
  DECLARE @CurrentParameterValueDatetime datetime2
  DECLARE @CurrentParameterDelimiter nvarchar(max)
  DECLARE @CurrentParameterMessage nvarchar(max)

  DECLARE @HostPlatform nvarchar(max)
  DECLARE @ContainedAvailabilityGroupListenerConnection bit = 0

  DECLARE @QueueID int
  DECLARE @QueueStartTime datetime2

  DECLARE @CurrentDBID int
  DECLARE @CurrentDatabaseName nvarchar(max)

  DECLARE @CurrentDatabase_sp_executesql nvarchar(max)

  DECLARE @CurrentExecuteAsUserExists bit
  DECLARE @CurrentUserAccess nvarchar(max)
  DECLARE @CurrentIsReadOnly bit
  DECLARE @CurrentDatabaseState nvarchar(max)
  DECLARE @CurrentInStandby bit
  DECLARE @CurrentRecoveryModel nvarchar(max)
  DECLARE @CurrentDatabaseHasReadOnlyFileGroup bit

  DECLARE @CurrentAvailabilityGroupReplicaID uniqueidentifier
  DECLARE @CurrentAvailabilityGroupID uniqueidentifier
  DECLARE @CurrentAvailabilityGroup nvarchar(max)
  DECLARE @CurrentAvailabilityGroupRole nvarchar(max)
  DECLARE @CurrentDistributedAvailabilityGroup nvarchar(max)
  DECLARE @CurrentDistributedAvailabilityGroupReplicaID uniqueidentifier
  DECLARE @CurrentDistributedAvailabilityGroupRole nvarchar(max)
  DECLARE @CurrentDatabaseMirroringRole nvarchar(max)

  DECLARE @CurrentDatabaseContext nvarchar(max)
  DECLARE @CurrentCommand nvarchar(max)
  DECLARE @CurrentCommandOutput int
  DECLARE @CurrentCommandType nvarchar(max)
  DECLARE @CurrentComment nvarchar(max)
  DECLARE @CurrentExtendedInfo xml

  DECLARE @Errors TABLE (ID int IDENTITY PRIMARY KEY,
                         [Message] nvarchar(max) NOT NULL,
                         Severity int NOT NULL,
                         [State] int)

  DECLARE @CurrentMessage nvarchar(max)
  DECLARE @CurrentSeverity int
  DECLARE @CurrentState int

  DECLARE @CurrentIxID int
  DECLARE @CurrentIxOrder int
  DECLARE @CurrentSchemaID int
  DECLARE @CurrentSchemaName nvarchar(max)
  DECLARE @CurrentObjectID int
  DECLARE @CurrentObjectName nvarchar(max)
  DECLARE @CurrentObjectType nvarchar(max)
  DECLARE @CurrentIsMemoryOptimized bit
  DECLARE @CurrentIndexID int
  DECLARE @CurrentIndexName nvarchar(max)
  DECLARE @CurrentIndexType int
  DECLARE @CurrentStatisticsID int
  DECLARE @CurrentStatisticsName nvarchar(max)
  DECLARE @CurrentPartitionID bigint
  DECLARE @CurrentPartitionNumber int
  DECLARE @CurrentPartitionCount int
  DECLARE @CurrentInRowDataPageCount bigint
  DECLARE @CurrentIsPartition bit
  DECLARE @CurrentIndexExists bit
  DECLARE @CurrentStatisticsExists bit
  DECLARE @CurrentIsImageText bit
  DECLARE @CurrentIsFileStream bit
  DECLARE @CurrentHasClusteredColumnstore bit
  DECLARE @CurrentIsColumnstoreOrdered bit
  DECLARE @CurrentIsComputed bit
  DECLARE @CurrentIsClusteredIndexComputed bit
  DECLARE @CurrentIsTimestamp bit
  DECLARE @CurrentAllowPageLocks bit
  DECLARE @CurrentHasFilter bit
  DECLARE @CurrentNoRecompute bit
  DECLARE @CurrentIsIncremental bit
  DECLARE @CurrentObjectHasRows bit
  DECLARE @CurrentRowCount bigint
  DECLARE @CurrentModificationCounter bigint
  DECLARE @CurrentOnReadOnlyFileGroup bit
  DECLARE @CurrentResumableIndexOperation bit
  DECLARE @CurrentFragmentationLevel float
  DECLARE @CurrentPageCount bigint
  DECLARE @CurrentFragmentationGroup nvarchar(max)
  DECLARE @CurrentAction nvarchar(max)
  DECLARE @CurrentMaxDOP int
  DECLARE @CurrentUpdateStatistics nvarchar(max)
  DECLARE @CurrentStatisticsSample int
  DECLARE @CurrentStatisticsPersistSample nvarchar(max)
  DECLARE @CurrentStatisticsResample nvarchar(max)
  DECLARE @CurrentDelay datetime

  DECLARE @tmpDatabases TABLE (ID int IDENTITY,
                               DatabaseName nvarchar(128),
                               DatabaseType nvarchar(1),
                               AvailabilityGroup bit,
                               StartPosition int,
                               DatabaseSize bigint,
                               [Order] int DEFAULT 0,
                               Selected bit DEFAULT 0,
                               Completed bit DEFAULT 0,
                               PRIMARY KEY (Selected, Completed, [Order], ID))

  DECLARE @tmpAvailabilityGroups TABLE (ID int IDENTITY PRIMARY KEY,
                                        AvailabilityGroupName nvarchar(128),
                                        StartPosition int,
                                        Selected bit DEFAULT 0)

  DECLARE @tmpDatabasesAvailabilityGroups TABLE (DatabaseName nvarchar(128),
                                                 AvailabilityGroupName nvarchar(128))

  DECLARE @tmpIndexesStatistics TABLE (ID int IDENTITY,
                                       SchemaID int,
                                       SchemaName nvarchar(128),
                                       ObjectID int,
                                       ObjectName nvarchar(128),
                                       ObjectType nvarchar(2),
                                       IsMemoryOptimized bit,
                                       IndexID int,
                                       IndexName nvarchar(128),
                                       IndexType int,
                                       AllowPageLocks bit,
                                       HasFilter bit,
                                       IsImageText bit,
                                       IsFileStream bit,
                                       HasClusteredColumnstore bit,
                                       IsColumnstoreOrdered bit,
                                       IsComputed bit,
                                       IsClusteredIndexComputed bit,
                                       IsTimestamp bit,
                                       OnReadOnlyFileGroup bit,
                                       ResumableIndexOperation bit,
                                       StatisticsID int,
                                       StatisticsName nvarchar(128),
                                       [NoRecompute] bit,
                                       IsIncremental bit,
                                       PartitionID bigint,
                                       PartitionNumber int,
                                       PartitionCount int,
                                       InRowDataPageCount bigint,
                                       StartPosition int,
                                       [Order] int DEFAULT 0,
                                       Selected bit DEFAULT 0,
                                       Completed bit DEFAULT 0,
                                       PRIMARY KEY (Selected, Completed, [Order], ID),
                                       INDEX IX_ObjectID_StatisticsID_PartitionNumber NONCLUSTERED (ObjectID, StatisticsID, PartitionNumber))

  DROP TABLE IF EXISTS #SelectedIndexes

  CREATE TABLE #SelectedIndexes (DatabaseName nvarchar(max) COLLATE DATABASE_DEFAULT,
                                 SchemaName nvarchar(max) COLLATE DATABASE_DEFAULT,
                                 ObjectName nvarchar(max) COLLATE DATABASE_DEFAULT,
                                 IndexName nvarchar(max) COLLATE DATABASE_DEFAULT,
                                 StartPosition int,
                                 Selected bit)

  DROP TABLE IF EXISTS #Objects

  CREATE TABLE #Objects (ObjectID int NOT NULL,
                         SchemaID int,
                         SchemaName nvarchar(128) COLLATE DATABASE_DEFAULT,
                         ObjectName nvarchar(128) COLLATE DATABASE_DEFAULT,
                         ObjectType nvarchar(2) COLLATE DATABASE_DEFAULT,
                         IsMemoryOptimized bit,
                         HasClusteredColumnstore bit,
                         IsClusteredIndexComputed bit,
                         IsClusteredIndexDisabled bit,
                         PRIMARY KEY (ObjectID))

  DROP TABLE IF EXISTS #Indexes

  CREATE TABLE #Indexes (ObjectID int NOT NULL,
                         IndexID int NOT NULL,
                         IndexName nvarchar(128) COLLATE DATABASE_DEFAULT,
                         IndexType int,
                         DataSpaceID int,
                         AllowPageLocks bit,
                         HasFilter bit,
                         IsImageText bit,
                         IsFileStream bit,
                         IsColumnstoreOrdered bit,
                         IsComputed bit,
                         IsTimestamp bit,
                         PRIMARY KEY (ObjectID, IndexID))

  DROP TABLE IF EXISTS #Stats

  CREATE TABLE #Stats (ObjectID int NOT NULL,
                       StatisticsID int NOT NULL,
                       StatisticsName nvarchar(128) COLLATE DATABASE_DEFAULT,
                       [NoRecompute] bit,
                       IsIncremental bit,
                       IsIndex bit,
                       PRIMARY KEY (ObjectID, StatisticsID))

  DROP TABLE IF EXISTS #ExistingObjects

  CREATE TABLE #ExistingObjects (SchemaName nvarchar(max) COLLATE DATABASE_DEFAULT,
                                 ObjectName nvarchar(max) COLLATE DATABASE_DEFAULT)

  DROP TABLE IF EXISTS #ExistingIndexes

  CREATE TABLE #ExistingIndexes (SchemaName nvarchar(max) COLLATE DATABASE_DEFAULT,
                                 ObjectName nvarchar(max) COLLATE DATABASE_DEFAULT,
                                 IndexName nvarchar(max) COLLATE DATABASE_DEFAULT)

  DECLARE @tmpResumableOperations TABLE (ObjectID int NOT NULL,
                                         IndexID int NOT NULL,
                                         PartitionNumber int)

  DECLARE @SelectedDatabases TABLE (DatabaseName nvarchar(max),
                                    DatabaseType nvarchar(1),
                                    AvailabilityGroup bit,
                                    StartPosition int,
                                    Selected bit)

  DECLARE @SelectedAvailabilityGroups TABLE (AvailabilityGroupName nvarchar(max),
                                             StartPosition int,
                                             Selected bit)

  DECLARE @SelectedIndexes TABLE (DatabaseName nvarchar(max),
                                  SchemaName nvarchar(max),
                                  ObjectName nvarchar(max),
                                  IndexName nvarchar(max),
                                  StartPosition int,
                                  Selected bit)

  DECLARE @IncrementalStatsProperties TABLE (ObjectID int,
                                             StatisticsID int,
                                             PartitionNumber int,
                                             [Rows] bigint,
                                             ModificationCounter bigint,
                                             PRIMARY KEY (ObjectID, StatisticsID, PartitionNumber))

  DECLARE @Actions TABLE ([Action] nvarchar(max))

  INSERT INTO @Actions([Action]) VALUES('INDEX_REBUILD_ONLINE')
  INSERT INTO @Actions([Action]) VALUES('INDEX_REBUILD_OFFLINE')
  INSERT INTO @Actions([Action]) VALUES('INDEX_REORGANIZE')

  DECLARE @ActionsPreferred TABLE (FragmentationGroup nvarchar(max),
                                   [Priority] int,
                                   [Action] nvarchar(max))

  DECLARE @CurrentActionsAllowed TABLE ([Action] nvarchar(max))

  DECLARE @CurrentAlterIndexWithClauseArguments TABLE (ID int IDENTITY,
                                                       Argument nvarchar(max))

  DECLARE @CurrentUpdateStatisticsWithClauseArguments TABLE (ID int IDENTITY,
                                                             Argument nvarchar(max))

  DECLARE @Error int = 0
  DECLARE @ReturnCode int = 0

  DECLARE @EmptyLine nvarchar(max) = CHAR(9)

  DECLARE @ProductVersion nvarchar(max) = CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))
  DECLARE @ProductUpdateType nvarchar(max) = CAST(SERVERPROPERTY('ProductUpdateType') AS nvarchar(max))
  DECLARE @EngineEdition int = CAST(SERVERPROPERTY('EngineEdition') AS int)
  DECLARE @Edition nvarchar(max) = CAST(SERVERPROPERTY('Edition') AS nvarchar(max))
  DECLARE @IsHadrEnabled bit = CAST(SERVERPROPERTY('IsHadrEnabled') AS bit)
  DECLARE @ServerName nvarchar(max) = CAST(SERVERPROPERTY('ServerName') AS nvarchar(max))

  DECLARE @Collation nvarchar(128) = CAST(DATABASEPROPERTYEX(DB_NAME(),'Collation') AS nvarchar(128))

  DECLARE @Version numeric(18,10) = CAST(PARSENAME(@ProductVersion,4) + '.' + PARSENAME(@ProductVersion,3) + PARSENAME(@ProductVersion,2) AS numeric(18,10))

  IF @EngineEdition = 8 AND @ProductVersion = '12.0.2000.8' AND @ProductUpdateType = 'CU'
  BEGIN
    SET @Version = 16.01000
  END

  IF @EngineEdition <> 5
  BEGIN
    SELECT @HostPlatform = host_platform
    FROM sys.dm_os_host_info
  END

  IF @EngineEdition <> 5
  BEGIN
    IF EXISTS (SELECT * FROM sys.databases WHERE name = 'msdb' AND database_id <> 4)
    AND EXISTS (SELECT * FROM sys.dm_exec_connections dm_exec_connections INNER JOIN sys.availability_group_listener_ip_addresses availability_group_listener_ip_addresses ON dm_exec_connections.local_net_address = availability_group_listener_ip_addresses.ip_address WHERE dm_exec_connections.session_id = @@SPID)
    BEGIN
      SET @ContainedAvailabilityGroupListenerConnection = 1
    END
  END

  DECLARE @AmazonRDS bit = CASE WHEN @EngineEdition IN (5, 8) THEN 0 WHEN EXISTS (SELECT * FROM sys.databases WHERE [name] = 'rdsadmin') AND SUSER_SNAME(0x01) = 'rdsa' THEN 1 ELSE 0 END

  ----------------------------------------------------------------------------------------------------
  --// Log initial information                                                                    //--
  ----------------------------------------------------------------------------------------------------

  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@Databases', @Databases)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@FragmentationLow', @FragmentationLow)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@FragmentationMedium', @FragmentationMedium)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@FragmentationHigh', @FragmentationHigh)
  INSERT INTO @Parameters ([Name], ValueInt) VALUES('@FragmentationLevel1', @FragmentationLevel1)
  INSERT INTO @Parameters ([Name], ValueInt) VALUES('@FragmentationLevel2', @FragmentationLevel2)
  INSERT INTO @Parameters ([Name], ValueInt) VALUES('@MinNumberOfPages', @MinNumberOfPages)
  INSERT INTO @Parameters ([Name], ValueInt) VALUES('@MaxNumberOfPages', @MaxNumberOfPages)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@SortInTempdb', @SortInTempdb)
  INSERT INTO @Parameters ([Name], ValueInt) VALUES('@MaxDOP', @MaxDOP)
  INSERT INTO @Parameters ([Name], ValueInt) VALUES('@FillFactor', @FillFactor)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@PadIndex', @PadIndex)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@DataCompression', @DataCompression)
  INSERT INTO @Parameters ([Name], ValueInt) VALUES('@WaitAtLowPriorityMaxDuration', @WaitAtLowPriorityMaxDuration)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@WaitAtLowPriorityAbortAfterWait', @WaitAtLowPriorityAbortAfterWait)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@Resumable', @Resumable)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@LOBCompaction', @LOBCompaction)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@UpdateStatistics', @UpdateStatistics)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@OnlyModifiedStatistics', @OnlyModifiedStatistics)
  INSERT INTO @Parameters ([Name], ValueInt) VALUES('@StatisticsModificationLevel', @StatisticsModificationLevel)
  INSERT INTO @Parameters ([Name], ValueInt) VALUES('@StatisticsSample', @StatisticsSample)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@StatisticsPersistSample', @StatisticsPersistSample)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@StatisticsResample', @StatisticsResample)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@PartitionLevel', @PartitionLevel)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@MSShippedObjects', @MSShippedObjects)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@Indexes', @Indexes)
  INSERT INTO @Parameters ([Name], ValueInt) VALUES('@TimeLimit', @TimeLimit)
  INSERT INTO @Parameters ([Name], ValueInt) VALUES('@Delay', @Delay)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@AvailabilityGroups', @AvailabilityGroups)
  INSERT INTO @Parameters ([Name], ValueInt) VALUES('@LockTimeout', @LockTimeout)
  INSERT INTO @Parameters ([Name], ValueInt) VALUES('@LockMessageSeverity', @LockMessageSeverity)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@StringDelimiter', @StringDelimiter)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@DatabaseOrder', @DatabaseOrder)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@DatabasesInParallel', @DatabasesInParallel)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@ExecuteAsUser', @ExecuteAsUser)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@LogToTable', @LogToTable)
  INSERT INTO @Parameters ([Name], ValueNvarchar) VALUES('@Execute', @Execute)

  SELECT @ParametersString = STRING_AGG(CAST([Name] + ' = ' + CASE WHEN ValueNvarchar IS NOT NULL THEN '''' + REPLACE(ValueNvarchar,'''','''''') + '''' WHEN ValueInt IS NOT NULL THEN CAST(ValueInt AS nvarchar(max)) WHEN ValueDatetime IS NOT NULL THEN '''' + CONVERT(nvarchar(max), ValueDatetime, 21) + '''' ELSE 'NULL' END AS nvarchar(max)), ', ') WITHIN GROUP (ORDER BY [ID] ASC)
  FROM @Parameters

  SET @StartMessage = 'Date and time: ' + CONVERT(nvarchar(max),@StartTime,120)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Server: ' + @ServerName
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Version: ' + @ProductVersion
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Edition: ' + @Edition
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  IF @EngineEdition = 8
  BEGIN
    SET @StartMessage = 'Update type: ' + @ProductUpdateType
    RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT
  END

  IF @EngineEdition <> 5
  BEGIN
    SET @StartMessage = 'Platform: ' + ISNULL(@HostPlatform, 'N/A')
    RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT
  END

  IF @EngineEdition <> 5
  BEGIN
    SET @StartMessage = 'Contained availability group connection: ' + CASE WHEN @ContainedAvailabilityGroupListenerConnection = 1 THEN 'Yes' WHEN @ContainedAvailabilityGroupListenerConnection = 0 THEN 'No' ELSE 'N/A' END
    RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT
  END

  SET @StartMessage = 'Database: ' + QUOTENAME(DB_NAME())
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Procedure: ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@ObjectName)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Version: ' + @VersionTimestamp
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Source: https://ola.hallengren.com'
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT
  RAISERROR(@EmptyLine,10,1) WITH NOWAIT

  SET @StartMessage = 'Command:'
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'EXECUTE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@ObjectName)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  DECLARE ParameterCursor CURSOR LOCAL FAST_FORWARD FOR SELECT [Name], ValueNvarchar, ValueInt, ValueDatetime, CASE WHEN [ID] = MAX([ID]) OVER() THEN '' ELSE ',' END FROM @Parameters ORDER BY [ID] ASC

  OPEN ParameterCursor

  FETCH ParameterCursor INTO @CurrentParameterName, @CurrentParameterValueNvarchar, @CurrentParameterValueInt, @CurrentParameterValueDatetime, @CurrentParameterDelimiter

  WHILE @@FETCH_STATUS = 0
  BEGIN
    SET @CurrentParameterMessage = @CurrentParameterName + ' = ' + CASE WHEN @CurrentParameterValueNvarchar IS NOT NULL THEN '''' + REPLACE(@CurrentParameterValueNvarchar,'''','''''') + '''' WHEN @CurrentParameterValueInt IS NOT NULL THEN CAST(@CurrentParameterValueInt AS nvarchar(max)) WHEN @CurrentParameterValueDatetime IS NOT NULL THEN '''' + CONVERT(nvarchar(max), @CurrentParameterValueDatetime, 21) + '''' ELSE 'NULL' END + @CurrentParameterDelimiter

    RAISERROR('%s',10,1,@CurrentParameterMessage) WITH NOWAIT

    FETCH NEXT FROM ParameterCursor INTO @CurrentParameterName, @CurrentParameterValueNvarchar, @CurrentParameterValueInt, @CurrentParameterValueDatetime, @CurrentParameterDelimiter
  END

  CLOSE ParameterCursor

  DEALLOCATE ParameterCursor

  RAISERROR(@EmptyLine,10,1) WITH NOWAIT

  ----------------------------------------------------------------------------------------------------
  --// Check core requirements                                                                    //--
  ----------------------------------------------------------------------------------------------------

  IF NOT (SELECT uses_ansi_nulls FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('ANSI_NULLS has to be set to ON for the stored procedure.', 16, 1)
  END

  IF NOT (SELECT uses_quoted_identifier FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('QUOTED_IDENTIFIER has to be set to ON for the stored procedure.', 16, 1)
  END

  IF NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'P' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandExecute')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The stored procedure CommandExecute is missing. Download https://ola.hallengren.com/scripts/CommandExecute.sql.', 16, 1)
  END

  IF EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'P' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandExecute' AND OBJECT_DEFINITION(objects.[object_id]) NOT LIKE '%@DatabaseContext%')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The stored procedure CommandExecute needs to be updated. Download https://ola.hallengren.com/scripts/CommandExecute.sql.', 16, 1)
  END

  IF @LogToTable = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandLog')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The table CommandLog is missing. Download https://ola.hallengren.com/scripts/CommandLog.sql.', 16, 1)
  END

  IF @DatabasesInParallel = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'Queue')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The table Queue is missing. Download https://ola.hallengren.com/scripts/Queue.sql.', 16, 1)
  END

  IF @DatabasesInParallel = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'QueueDatabase')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The table QueueDatabase is missing. Download https://ola.hallengren.com/scripts/QueueDatabase.sql.', 16, 1)
  END

  IF @@TRANCOUNT <> 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The transaction count is not 0.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------
  --// Select databases                                                                           //--
  ----------------------------------------------------------------------------------------------------

  SET @Databases = REPLACE(@Databases, CHAR(10), '')
  SET @Databases = REPLACE(@Databases, CHAR(13), '')

  WHILE CHARINDEX(@StringDelimiter + ' ', @Databases) > 0 SET @Databases = REPLACE(@Databases, @StringDelimiter + ' ', @StringDelimiter)
  WHILE CHARINDEX(' ' + @StringDelimiter, @Databases) > 0 SET @Databases = REPLACE(@Databases, ' ' + @StringDelimiter, @StringDelimiter)

  SET @Databases = LTRIM(RTRIM(@Databases));

  WITH Databases1 (StartPosition, EndPosition, DatabaseItem) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, 1), 0), LEN(@Databases) + 1) AS EndPosition,
         SUBSTRING(@Databases, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, 1), 0), LEN(@Databases) + 1) - 1) AS DatabaseItem
  WHERE @Databases IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, EndPosition + 1), 0), LEN(@Databases) + 1) AS EndPosition,
         SUBSTRING(@Databases, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Databases, EndPosition + 1), 0), LEN(@Databases) + 1) - EndPosition - 1) AS DatabaseItem
  FROM Databases1
  WHERE EndPosition < LEN(@Databases) + 1
  ),
  Databases2 (DatabaseItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN DatabaseItem LIKE '-%' THEN RIGHT(DatabaseItem,LEN(DatabaseItem) - 1) ELSE DatabaseItem END AS DatabaseItem,
         StartPosition,
         CASE WHEN DatabaseItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
  FROM Databases1
  ),
  Databases3 (DatabaseItem, DatabaseType, AvailabilityGroup, StartPosition, Selected) AS
  (
  SELECT CASE WHEN DatabaseItem IN('ALL_DATABASES','SYSTEM_DATABASES','USER_DATABASES','AVAILABILITY_GROUP_DATABASES') THEN '%' ELSE DatabaseItem END AS DatabaseItem,
         CASE WHEN DatabaseItem = 'SYSTEM_DATABASES' THEN 'S' WHEN DatabaseItem = 'USER_DATABASES' THEN 'U' ELSE NULL END AS DatabaseType,
         CASE WHEN DatabaseItem = 'AVAILABILITY_GROUP_DATABASES' THEN 1 ELSE NULL END AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases2
  ),
  Databases4 (DatabaseName, DatabaseType, AvailabilityGroup, StartPosition, Selected) AS
  (
  SELECT CASE WHEN LEFT(DatabaseItem,1) = '[' AND RIGHT(DatabaseItem,1) = ']' THEN PARSENAME(DatabaseItem,1) ELSE DatabaseItem END AS DatabaseItem,
         DatabaseType,
         AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases3
  )
  INSERT INTO @SelectedDatabases (DatabaseName, DatabaseType, AvailabilityGroup, StartPosition, Selected)
  SELECT DatabaseName,
         DatabaseType,
         AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases4
  OPTION (MAXRECURSION 0)

  IF @IsHadrEnabled = 1
  BEGIN
    INSERT INTO @tmpAvailabilityGroups (AvailabilityGroupName)
    SELECT name AS AvailabilityGroupName
    FROM sys.availability_groups

    INSERT INTO @tmpDatabasesAvailabilityGroups (DatabaseName, AvailabilityGroupName)
    SELECT databases.name,
           availability_groups.name
    FROM sys.databases databases
    INNER JOIN sys.availability_replicas availability_replicas ON databases.replica_id = availability_replicas.replica_id
    INNER JOIN sys.availability_groups availability_groups ON availability_replicas.group_id = availability_groups.group_id
  END

  INSERT INTO @tmpDatabases (DatabaseName, DatabaseType, AvailabilityGroup)
  SELECT [name] AS DatabaseName,
         CASE WHEN name IN('master','msdb','model') OR is_distributor = 1 THEN 'S' ELSE 'U' END AS DatabaseType,
         NULL AS AvailabilityGroup
  FROM sys.databases
  WHERE [name] <> 'tempdb'
  AND source_database_id IS NULL
  ORDER BY [name] ASC

  UPDATE tmpDatabases
  SET AvailabilityGroup = CASE WHEN EXISTS (SELECT * FROM @tmpDatabasesAvailabilityGroups WHERE DatabaseName = tmpDatabases.DatabaseName) THEN 1 ELSE 0 END
  FROM @tmpDatabases tmpDatabases

  UPDATE tmpDatabases
  SET tmpDatabases.Selected = SelectedDatabases.Selected
  FROM @tmpDatabases tmpDatabases
  INNER JOIN @SelectedDatabases SelectedDatabases
  ON tmpDatabases.DatabaseName LIKE REPLACE(REPLACE(SelectedDatabases.DatabaseName,'[','[[]'),'_','[_]')
  AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
  AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
  WHERE SelectedDatabases.Selected = 1

  UPDATE tmpDatabases
  SET tmpDatabases.Selected = SelectedDatabases.Selected
  FROM @tmpDatabases tmpDatabases
  INNER JOIN @SelectedDatabases SelectedDatabases
  ON tmpDatabases.DatabaseName LIKE REPLACE(REPLACE(SelectedDatabases.DatabaseName,'[','[[]'),'_','[_]')
  AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
  AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
  WHERE SelectedDatabases.Selected = 0

  UPDATE tmpDatabases
  SET tmpDatabases.StartPosition = SelectedDatabases2.StartPosition
  FROM @tmpDatabases tmpDatabases
  INNER JOIN (SELECT tmpDatabases.DatabaseName, MIN(SelectedDatabases.StartPosition) AS StartPosition
              FROM @tmpDatabases tmpDatabases
              INNER JOIN @SelectedDatabases SelectedDatabases
              ON tmpDatabases.DatabaseName LIKE REPLACE(REPLACE(SelectedDatabases.DatabaseName,'[','[[]'),'_','[_]')
              AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
              AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
              WHERE SelectedDatabases.Selected = 1
              GROUP BY tmpDatabases.DatabaseName) SelectedDatabases2
  ON tmpDatabases.DatabaseName = SelectedDatabases2.DatabaseName

  IF @Databases IS NOT NULL AND (NOT EXISTS(SELECT * FROM @SelectedDatabases) OR EXISTS(SELECT * FROM @SelectedDatabases WHERE DatabaseName IS NULL OR DATALENGTH(DatabaseName) = 0))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @Databases is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------
  --// Select availability groups                                                                 //--
  ----------------------------------------------------------------------------------------------------

  IF @AvailabilityGroups IS NOT NULL AND @IsHadrEnabled = 1
  BEGIN

    SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, CHAR(10), '')
    SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, CHAR(13), '')

    WHILE CHARINDEX(@StringDelimiter + ' ', @AvailabilityGroups) > 0 SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, @StringDelimiter + ' ', @StringDelimiter)
    WHILE CHARINDEX(' ' + @StringDelimiter, @AvailabilityGroups) > 0 SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, ' ' + @StringDelimiter, @StringDelimiter)

    SET @AvailabilityGroups = LTRIM(RTRIM(@AvailabilityGroups));

    WITH AvailabilityGroups1 (StartPosition, EndPosition, AvailabilityGroupItem) AS
    (
    SELECT 1 AS StartPosition,
           ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, 1), 0), LEN(@AvailabilityGroups) + 1) AS EndPosition,
           SUBSTRING(@AvailabilityGroups, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, 1), 0), LEN(@AvailabilityGroups) + 1) - 1) AS AvailabilityGroupItem
    WHERE @AvailabilityGroups IS NOT NULL
    UNION ALL
    SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
           ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, EndPosition + 1), 0), LEN(@AvailabilityGroups) + 1) AS EndPosition,
           SUBSTRING(@AvailabilityGroups, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @AvailabilityGroups, EndPosition + 1), 0), LEN(@AvailabilityGroups) + 1) - EndPosition - 1) AS AvailabilityGroupItem
    FROM AvailabilityGroups1
    WHERE EndPosition < LEN(@AvailabilityGroups) + 1
    ),
    AvailabilityGroups2 (AvailabilityGroupItem, StartPosition, Selected) AS
    (
    SELECT CASE WHEN AvailabilityGroupItem LIKE '-%' THEN RIGHT(AvailabilityGroupItem,LEN(AvailabilityGroupItem) - 1) ELSE AvailabilityGroupItem END AS AvailabilityGroupItem,
           StartPosition,
           CASE WHEN AvailabilityGroupItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
    FROM AvailabilityGroups1
    ),
    AvailabilityGroups3 (AvailabilityGroupItem, StartPosition, Selected) AS
    (
    SELECT CASE WHEN AvailabilityGroupItem = 'ALL_AVAILABILITY_GROUPS' THEN '%' ELSE AvailabilityGroupItem END AS AvailabilityGroupItem,
           StartPosition,
           Selected
    FROM AvailabilityGroups2
    ),
    AvailabilityGroups4 (AvailabilityGroupName, StartPosition, Selected) AS
    (
    SELECT CASE WHEN LEFT(AvailabilityGroupItem,1) = '[' AND RIGHT(AvailabilityGroupItem,1) = ']' THEN PARSENAME(AvailabilityGroupItem,1) ELSE AvailabilityGroupItem END AS AvailabilityGroupItem,
           StartPosition,
           Selected
    FROM AvailabilityGroups3
    )
    INSERT INTO @SelectedAvailabilityGroups (AvailabilityGroupName, StartPosition, Selected)
    SELECT AvailabilityGroupName, StartPosition, Selected
    FROM AvailabilityGroups4
    OPTION (MAXRECURSION 0)

    UPDATE tmpAvailabilityGroups
    SET tmpAvailabilityGroups.Selected = SelectedAvailabilityGroups.Selected
    FROM @tmpAvailabilityGroups tmpAvailabilityGroups
    INNER JOIN @SelectedAvailabilityGroups SelectedAvailabilityGroups
    ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'[','[[]'),'_','[_]')
    WHERE SelectedAvailabilityGroups.Selected = 1

    UPDATE tmpAvailabilityGroups
    SET tmpAvailabilityGroups.Selected = SelectedAvailabilityGroups.Selected
    FROM @tmpAvailabilityGroups tmpAvailabilityGroups
    INNER JOIN @SelectedAvailabilityGroups SelectedAvailabilityGroups
    ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'[','[[]'),'_','[_]')
    WHERE SelectedAvailabilityGroups.Selected = 0

    UPDATE tmpAvailabilityGroups
    SET tmpAvailabilityGroups.StartPosition = SelectedAvailabilityGroups2.StartPosition
    FROM @tmpAvailabilityGroups tmpAvailabilityGroups
    INNER JOIN (SELECT tmpAvailabilityGroups.AvailabilityGroupName, MIN(SelectedAvailabilityGroups.StartPosition) AS StartPosition
                FROM @tmpAvailabilityGroups tmpAvailabilityGroups
                INNER JOIN @SelectedAvailabilityGroups SelectedAvailabilityGroups
                ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'[','[[]'),'_','[_]')
                WHERE SelectedAvailabilityGroups.Selected = 1
                GROUP BY tmpAvailabilityGroups.AvailabilityGroupName) SelectedAvailabilityGroups2
    ON tmpAvailabilityGroups.AvailabilityGroupName = SelectedAvailabilityGroups2.AvailabilityGroupName

    UPDATE tmpDatabases
    SET tmpDatabases.StartPosition = tmpAvailabilityGroups.StartPosition,
        tmpDatabases.Selected = 1
    FROM @tmpDatabases tmpDatabases
    INNER JOIN @tmpDatabasesAvailabilityGroups tmpDatabasesAvailabilityGroups ON tmpDatabases.DatabaseName = tmpDatabasesAvailabilityGroups.DatabaseName
    INNER JOIN @tmpAvailabilityGroups tmpAvailabilityGroups ON tmpDatabasesAvailabilityGroups.AvailabilityGroupName = tmpAvailabilityGroups.AvailabilityGroupName
    WHERE tmpAvailabilityGroups.Selected = 1

  END

  IF @AvailabilityGroups IS NOT NULL AND (NOT EXISTS(SELECT * FROM @SelectedAvailabilityGroups) OR EXISTS(SELECT * FROM @SelectedAvailabilityGroups WHERE AvailabilityGroupName IS NULL OR AvailabilityGroupName = '') OR @IsHadrEnabled = 0)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @AvailabilityGroups is not supported.', 16, 1)
  END

  IF (@Databases IS NULL AND @AvailabilityGroups IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('You need to specify one of the parameters @Databases and @AvailabilityGroups.', 16, 2)
  END

  IF (@Databases IS NOT NULL AND @AvailabilityGroups IS NOT NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('You can only specify one of the parameters @Databases and @AvailabilityGroups.', 16, 3)
  END

  ----------------------------------------------------------------------------------------------------
  --// Select indexes                                                                             //--
  ----------------------------------------------------------------------------------------------------

  SET @Indexes = REPLACE(@Indexes, CHAR(10), '')
  SET @Indexes = REPLACE(@Indexes, CHAR(13), '')

  WHILE CHARINDEX(@StringDelimiter + ' ', @Indexes) > 0 SET @Indexes = REPLACE(@Indexes, @StringDelimiter + ' ', @StringDelimiter)
  WHILE CHARINDEX(' ' + @StringDelimiter, @Indexes) > 0 SET @Indexes = REPLACE(@Indexes, ' ' + @StringDelimiter, @StringDelimiter)

  SET @Indexes = LTRIM(RTRIM(@Indexes));

  WITH Indexes1 (StartPosition, EndPosition, IndexItem) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Indexes, 1), 0), LEN(@Indexes) + 1) AS EndPosition,
         SUBSTRING(@Indexes, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Indexes, 1), 0), LEN(@Indexes) + 1) - 1) AS IndexItem
  WHERE @Indexes IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Indexes, EndPosition + 1), 0), LEN(@Indexes) + 1) AS EndPosition,
         SUBSTRING(@Indexes, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @Indexes, EndPosition + 1), 0), LEN(@Indexes) + 1) - EndPosition - 1) AS IndexItem
  FROM Indexes1
  WHERE EndPosition < LEN(@Indexes) + 1
  ),
  Indexes2 (IndexItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN IndexItem LIKE '-%' THEN RIGHT(IndexItem,LEN(IndexItem) - 1) ELSE IndexItem END AS IndexItem,
         StartPosition,
         CASE WHEN IndexItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
  FROM Indexes1
  ),
  Indexes3 (IndexItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN IndexItem = 'ALL_INDEXES' THEN '%.%.%.%' ELSE IndexItem END AS IndexItem,
         StartPosition,
         Selected
  FROM Indexes2
  ),
  Indexes4 (DatabaseName, SchemaName, ObjectName, IndexName, StartPosition, Selected) AS
  (
  SELECT CASE WHEN PARSENAME(IndexItem,4) IS NULL THEN PARSENAME(IndexItem,3) ELSE PARSENAME(IndexItem,4) END AS DatabaseName,
         CASE WHEN PARSENAME(IndexItem,4) IS NULL THEN PARSENAME(IndexItem,2) ELSE PARSENAME(IndexItem,3) END AS SchemaName,
         CASE WHEN PARSENAME(IndexItem,4) IS NULL THEN PARSENAME(IndexItem,1) ELSE PARSENAME(IndexItem,2) END AS ObjectName,
         CASE WHEN PARSENAME(IndexItem,4) IS NULL THEN '%' ELSE PARSENAME(IndexItem,1) END AS IndexName,
         StartPosition,
         Selected
  FROM Indexes3
  )
  INSERT INTO @SelectedIndexes (DatabaseName, SchemaName, ObjectName, IndexName, StartPosition, Selected)
  SELECT DatabaseName, SchemaName, ObjectName, IndexName, StartPosition, Selected
  FROM Indexes4
  OPTION (MAXRECURSION 0)

  INSERT INTO #SelectedIndexes (DatabaseName, SchemaName, ObjectName, IndexName, StartPosition, Selected)
  SELECT DatabaseName, SchemaName, ObjectName, IndexName, StartPosition, Selected
  FROM @SelectedIndexes

  ----------------------------------------------------------------------------------------------------
  --// Select actions                                                                             //--
  ----------------------------------------------------------------------------------------------------

  SET @FragmentationLow = REPLACE(@FragmentationLow, @StringDelimiter + ' ', @StringDelimiter);

  WITH FragmentationLow (StartPosition, EndPosition, [Action]) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationLow, 1), 0), LEN(@FragmentationLow) + 1) AS EndPosition,
         SUBSTRING(@FragmentationLow, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationLow, 1), 0), LEN(@FragmentationLow) + 1) - 1) AS [Action]
  WHERE @FragmentationLow IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationLow, EndPosition + 1), 0), LEN(@FragmentationLow) + 1) AS EndPosition,
         SUBSTRING(@FragmentationLow, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationLow, EndPosition + 1), 0), LEN(@FragmentationLow) + 1) - EndPosition - 1) AS [Action]
  FROM FragmentationLow
  WHERE EndPosition < LEN(@FragmentationLow) + 1
  )
  INSERT INTO @ActionsPreferred(FragmentationGroup, [Priority], [Action])
  SELECT 'Low' AS FragmentationGroup,
         ROW_NUMBER() OVER(ORDER BY StartPosition ASC) AS [Priority],
         [Action]
  FROM FragmentationLow
  OPTION (MAXRECURSION 0)

  SET @FragmentationMedium = REPLACE(@FragmentationMedium, @StringDelimiter + ' ', @StringDelimiter);

  WITH FragmentationMedium (StartPosition, EndPosition, [Action]) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationMedium, 1), 0), LEN(@FragmentationMedium) + 1) AS EndPosition,
         SUBSTRING(@FragmentationMedium, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationMedium, 1), 0), LEN(@FragmentationMedium) + 1) - 1) AS [Action]
  WHERE @FragmentationMedium IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationMedium, EndPosition + 1), 0), LEN(@FragmentationMedium) + 1) AS EndPosition,
         SUBSTRING(@FragmentationMedium, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationMedium, EndPosition + 1), 0), LEN(@FragmentationMedium) + 1) - EndPosition - 1) AS [Action]
  FROM FragmentationMedium
  WHERE EndPosition < LEN(@FragmentationMedium) + 1
  )
  INSERT INTO @ActionsPreferred(FragmentationGroup, [Priority], [Action])
  SELECT 'Medium' AS FragmentationGroup,
         ROW_NUMBER() OVER(ORDER BY StartPosition ASC) AS [Priority],
         [Action]
  FROM FragmentationMedium
  OPTION (MAXRECURSION 0)

  SET @FragmentationHigh = REPLACE(@FragmentationHigh, @StringDelimiter + ' ', @StringDelimiter);

  WITH FragmentationHigh (StartPosition, EndPosition, [Action]) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationHigh, 1), 0), LEN(@FragmentationHigh) + 1) AS EndPosition,
         SUBSTRING(@FragmentationHigh, 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationHigh, 1), 0), LEN(@FragmentationHigh) + 1) - 1) AS [Action]
  WHERE @FragmentationHigh IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationHigh, EndPosition + 1), 0), LEN(@FragmentationHigh) + 1) AS EndPosition,
         SUBSTRING(@FragmentationHigh, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@StringDelimiter, @FragmentationHigh, EndPosition + 1), 0), LEN(@FragmentationHigh) + 1) - EndPosition - 1) AS [Action]
  FROM FragmentationHigh
  WHERE EndPosition < LEN(@FragmentationHigh) + 1
  )
  INSERT INTO @ActionsPreferred(FragmentationGroup, [Priority], [Action])
  SELECT 'High' AS FragmentationGroup,
         ROW_NUMBER() OVER(ORDER BY StartPosition ASC) AS [Priority],
         [Action]
  FROM FragmentationHigh
  OPTION (MAXRECURSION 0)

  ----------------------------------------------------------------------------------------------------
  --// Check input parameters                                                                     //--
  ----------------------------------------------------------------------------------------------------

  IF EXISTS (SELECT [Action] FROM @ActionsPreferred WHERE FragmentationGroup = 'Low' AND [Action] NOT IN(SELECT * FROM @Actions))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @FragmentationLow is not supported.', 16, 1)
  END

  IF EXISTS (SELECT * FROM @ActionsPreferred WHERE FragmentationGroup = 'Low' GROUP BY [Action] HAVING COUNT(*) > 1)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @FragmentationLow is not supported.', 16, 2)
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS (SELECT [Action] FROM @ActionsPreferred WHERE FragmentationGroup = 'Medium' AND [Action] NOT IN(SELECT * FROM @Actions))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @FragmentationMedium is not supported.', 16, 1)
  END

  IF EXISTS (SELECT * FROM @ActionsPreferred WHERE FragmentationGroup = 'Medium' GROUP BY [Action] HAVING COUNT(*) > 1)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @FragmentationMedium is not supported.', 16, 2)
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS (SELECT [Action] FROM @ActionsPreferred WHERE FragmentationGroup = 'High' AND [Action] NOT IN(SELECT * FROM @Actions))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @FragmentationHigh is not supported.', 16, 1)
  END

  IF EXISTS (SELECT * FROM @ActionsPreferred WHERE FragmentationGroup = 'High' GROUP BY [Action] HAVING COUNT(*) > 1)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @FragmentationHigh is not supported.', 16, 2)
  END

  ----------------------------------------------------------------------------------------------------

  IF @FragmentationLevel1 <= 0 OR @FragmentationLevel1 >= 100 OR @FragmentationLevel1 IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @FragmentationLevel1 is not supported.', 16, 1)
  END

  IF @FragmentationLevel1 >= @FragmentationLevel2
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @FragmentationLevel1 is not supported.', 16, 2)
  END

  ----------------------------------------------------------------------------------------------------

  IF @FragmentationLevel2 <= 0 OR @FragmentationLevel2 >= 100 OR @FragmentationLevel2 IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @FragmentationLevel2 is not supported.', 16, 1)
  END

  IF @FragmentationLevel2 <= @FragmentationLevel1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @FragmentationLevel2 is not supported.', 16, 2)
  END

  ----------------------------------------------------------------------------------------------------

  IF @MinNumberOfPages < 0 OR @MinNumberOfPages IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @MinNumberOfPages is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @MaxNumberOfPages < 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @MaxNumberOfPages is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @SortInTempdb NOT IN('Y','N') OR @SortInTempdb IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @SortInTempdb is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @MaxDOP < 0 OR @MaxDOP > 64
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @MaxDOP is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @FillFactor <= 0 OR @FillFactor > 100
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @FillFactor is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @PadIndex NOT IN('Y','N')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @PadIndex is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @DataCompression NOT IN('NONE', 'PAGE', 'ROW')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @DataCompression is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @WaitAtLowPriorityMaxDuration < 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @WaitAtLowPriorityMaxDuration is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @WaitAtLowPriorityAbortAfterWait NOT IN('NONE','SELF','BLOCKERS')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @WaitAtLowPriorityAbortAfterWait is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF (@WaitAtLowPriorityAbortAfterWait IS NOT NULL AND @WaitAtLowPriorityMaxDuration IS NULL) OR (@WaitAtLowPriorityAbortAfterWait IS NULL AND @WaitAtLowPriorityMaxDuration IS NOT NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The parameters @WaitAtLowPriorityMaxDuration and @WaitAtLowPriorityAbortAfterWait can only be used together.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @Resumable NOT IN('Y','N') OR @Resumable IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @Resumable is not supported.', 16, 1)
  END

  IF @Resumable = 'Y' AND @SortInTempdb = 'Y'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('You can only specify one of the parameters @Resumable and @SortInTempdb.', 16, 2)
  END

  ----------------------------------------------------------------------------------------------------

  IF @LOBCompaction NOT IN('Y','N') OR @LOBCompaction IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @LOBCompaction is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @UpdateStatistics NOT IN('ALL','COLUMNS','INDEX')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @UpdateStatistics is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @OnlyModifiedStatistics NOT IN('Y','N') OR @OnlyModifiedStatistics IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @OnlyModifiedStatistics is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @StatisticsModificationLevel <= 0 OR @StatisticsModificationLevel > 100
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @StatisticsModificationLevel is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @OnlyModifiedStatistics = 'Y' AND @StatisticsModificationLevel IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('You can only specify one of the parameters @OnlyModifiedStatistics and @StatisticsModificationLevel.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @StatisticsSample <= 0 OR @StatisticsSample  > 100
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @StatisticsSample is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @StatisticsPersistSample NOT IN('Y','N')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @StatisticsPersistSample is not supported.', 16, 1)
  END

  IF @StatisticsPersistSample IS NOT NULL AND @StatisticsSample IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The parameter @StatisticsPersistSample can only be used together with @StatisticsSample.', 16, 2)
  END

  IF @StatisticsPersistSample IS NOT NULL AND @StatisticsResample = 'Y'
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The parameters @StatisticsPersistSample and @StatisticsResample cannot be used together.', 16, 3)
  END

  IF @StatisticsPersistSample IS NOT NULL AND NOT (@Version >= 14.03006 OR @EngineEdition = 5 OR (@EngineEdition = 8 AND @ProductUpdateType = 'Continuous'))
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @StatisticsPersistSample is not supported.', 16, 4)
  END

  ----------------------------------------------------------------------------------------------------

  IF @StatisticsResample NOT IN('Y','N') OR @StatisticsResample IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @StatisticsResample is not supported.', 16, 1)
  END

  IF @StatisticsResample = 'Y' AND @StatisticsSample IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @StatisticsResample is not supported.', 16, 2)
  END

  ----------------------------------------------------------------------------------------------------

  IF @PartitionLevel NOT IN('Y','N') OR @PartitionLevel IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @PartitionLevel is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @MSShippedObjects NOT IN('Y','N') OR @MSShippedObjects IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @MSShippedObjects is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS(SELECT * FROM @SelectedIndexes WHERE DatabaseName IS NULL OR SchemaName IS NULL OR ObjectName IS NULL OR IndexName IS NULL)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @Indexes is not supported.', 16, 1)
  END

  IF @Indexes IS NOT NULL AND NOT EXISTS(SELECT * FROM @SelectedIndexes)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @Indexes is not supported.', 16, 2)
  END

  ----------------------------------------------------------------------------------------------------

  IF @TimeLimit < 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @TimeLimit is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @Delay < 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @Delay is not supported.', 16, 1)
  END

  IF @Delay >= 86400
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @Delay is not supported.', 16, 2)
  END

  ----------------------------------------------------------------------------------------------------

  IF @LockTimeout < 0
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @LockTimeout is not supported.', 16, 1)
  END

  IF @LockTimeout > 86400
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @LockTimeout is not supported.', 16, 2)
  END

  ----------------------------------------------------------------------------------------------------

  IF @LockMessageSeverity NOT IN(10, 16) OR @LockMessageSeverity IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @LockMessageSeverity is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @StringDelimiter IS NULL OR LEN(@StringDelimiter) <> 1
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @StringDelimiter is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @DatabaseOrder NOT IN('DATABASE_NAME_ASC','DATABASE_NAME_DESC','DATABASE_SIZE_ASC','DATABASE_SIZE_DESC')
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @DatabaseOrder is not supported.', 16, 1)
  END

  IF @DatabaseOrder IS NOT NULL AND @EngineEdition = 5
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @DatabaseOrder is not supported.', 16, 2)
  END

  ----------------------------------------------------------------------------------------------------

  IF @DatabasesInParallel NOT IN('Y','N') OR @DatabasesInParallel IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @DatabasesInParallel is not supported.', 16, 1)
  END

  IF @DatabasesInParallel = 'Y' AND @EngineEdition = 5
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @DatabasesInParallel is not supported.', 16, 2)
  END

  ----------------------------------------------------------------------------------------------------

  IF LEN(@ExecuteAsUser) > 128
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @ExecuteAsUser is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @LogToTable NOT IN('Y','N') OR @LogToTable IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @LogToTable is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF @Execute NOT IN('Y','N') OR @Execute IS NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The value for the parameter @Execute is not supported.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------

  IF EXISTS(SELECT * FROM @Errors)
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The documentation is available at https://ola.hallengren.com/sql-server-index-and-statistics-maintenance.html.', 16, 1)
  END

  ----------------------------------------------------------------------------------------------------
  --// Check that selected databases and availability groups exist                                //--
  ----------------------------------------------------------------------------------------------------

  SELECT @ErrorMessage = STRING_AGG(CAST(QUOTENAME(DatabaseName) AS nvarchar(max)), ', ')
                         WITHIN GROUP (ORDER BY DatabaseName ASC)
  FROM @SelectedDatabases
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases)

  IF @ErrorMessage IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The following databases in the @Databases parameter do not exist: ' + @ErrorMessage + '.', 10, 1)
  END

  SELECT @ErrorMessage = STRING_AGG(CAST(QUOTENAME(DatabaseName) AS nvarchar(max)), ', ')
                         WITHIN GROUP (ORDER BY DatabaseName ASC)
  FROM @SelectedIndexes
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases)

  IF @ErrorMessage IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The following databases in the @Indexes parameter do not exist: ' + @ErrorMessage + '.', 10, 1)
  END

  SELECT @ErrorMessage = STRING_AGG(CAST(QUOTENAME(AvailabilityGroupName) AS nvarchar(max)), ', ')
                         WITHIN GROUP (ORDER BY AvailabilityGroupName ASC)
  FROM @SelectedAvailabilityGroups
  WHERE AvailabilityGroupName NOT LIKE '%[%]%'
  AND AvailabilityGroupName NOT IN (SELECT AvailabilityGroupName FROM @tmpAvailabilityGroups)

  IF @ErrorMessage IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The following availability groups do not exist: ' + @ErrorMessage + '.', 10, 1)
  END

  SELECT @ErrorMessage = STRING_AGG(CAST(QUOTENAME(DatabaseName) AS nvarchar(max)), ', ')
                         WITHIN GROUP (ORDER BY DatabaseName ASC)
  FROM @SelectedIndexes
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName IN (SELECT DatabaseName FROM @tmpDatabases)
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases WHERE Selected = 1)

  IF @ErrorMessage IS NOT NULL
  BEGIN
    INSERT INTO @Errors ([Message], Severity, [State])
    VALUES('The following databases have been selected in the @Indexes parameter, but not in the @Databases or @AvailabilityGroups parameters: ' + @ErrorMessage + '.', 10, 1)
  END

  ----------------------------------------------------------------------------------------------------
  --// Raise errors                                                                               //--
  ----------------------------------------------------------------------------------------------------

  DECLARE ErrorCursor CURSOR LOCAL FAST_FORWARD FOR SELECT [Message], Severity, [State] FROM @Errors ORDER BY [ID] ASC

  OPEN ErrorCursor

  FETCH ErrorCursor INTO @CurrentMessage, @CurrentSeverity, @CurrentState

  WHILE @@FETCH_STATUS = 0
  BEGIN
    RAISERROR('%s', @CurrentSeverity, @CurrentState, @CurrentMessage) WITH NOWAIT
    RAISERROR(@EmptyLine, 10, 1) WITH NOWAIT

    FETCH NEXT FROM ErrorCursor INTO @CurrentMessage, @CurrentSeverity, @CurrentState
  END

  CLOSE ErrorCursor

  DEALLOCATE ErrorCursor

  IF EXISTS (SELECT * FROM @Errors WHERE Severity >= 16)
  BEGIN
    SET @ReturnCode = 50000
    GOTO Logging
  END

  ----------------------------------------------------------------------------------------------------
  --// Update database order                                                                      //--
  ----------------------------------------------------------------------------------------------------

  IF @DatabaseOrder IN('DATABASE_SIZE_ASC','DATABASE_SIZE_DESC')
  BEGIN
    UPDATE tmpDatabases
    SET DatabaseSize = (SELECT SUM(CAST(size AS bigint)) FROM sys.master_files WHERE [type] = 0 AND database_id = DB_ID(tmpDatabases.DatabaseName))
    FROM @tmpDatabases tmpDatabases
  END

  IF @DatabaseOrder IS NULL
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY StartPosition ASC, DatabaseName ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_NAME_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseName ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_NAME_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseName DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_SIZE_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseSize ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_SIZE_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseSize DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END

  ----------------------------------------------------------------------------------------------------
  --// Update the queue                                                                           //--
  ----------------------------------------------------------------------------------------------------

  IF @DatabasesInParallel = 'Y'
  BEGIN

    BEGIN TRY

      SELECT @QueueID = QueueID
      FROM dbo.[Queue]
      WHERE SchemaName = @SchemaName
      AND ObjectName = @ObjectName
      AND [Parameters] = @ParametersString

      IF @QueueID IS NULL
      BEGIN
        BEGIN TRANSACTION

        SELECT @QueueID = QueueID
        FROM dbo.[Queue] WITH (UPDLOCK, HOLDLOCK)
        WHERE SchemaName = @SchemaName
        AND ObjectName = @ObjectName
        AND [Parameters] = @ParametersString

        IF @QueueID IS NULL
        BEGIN
          INSERT INTO dbo.[Queue] (SchemaName, ObjectName, [Parameters])
          VALUES(@SchemaName, @ObjectName, @ParametersString)

          SET @QueueID = SCOPE_IDENTITY()
        END

        COMMIT TRANSACTION
      END

      BEGIN TRANSACTION

      UPDATE [Queue]
      SET QueueStartTime = SYSDATETIME(),
          SessionID = @@SPID,
          RequestID = (SELECT request_id FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          RequestStartTime = (SELECT start_time FROM sys.dm_exec_requests WHERE session_id = @@SPID)
      FROM dbo.[Queue] [Queue]
      WHERE QueueID = @QueueID
      AND NOT EXISTS (SELECT *
                      FROM sys.dm_exec_requests
                      WHERE session_id = [Queue].SessionID
                      AND request_id = [Queue].RequestID
                      AND start_time = [Queue].RequestStartTime)
      AND NOT EXISTS (SELECT *
                      FROM dbo.QueueDatabase QueueDatabase
                      INNER JOIN sys.dm_exec_requests ON QueueDatabase.SessionID = session_id AND QueueDatabase.RequestID = request_id AND QueueDatabase.RequestStartTime = start_time
                      WHERE QueueDatabase.QueueID = @QueueID)

      IF @@ROWCOUNT = 1
      BEGIN
        INSERT INTO dbo.QueueDatabase (QueueID, DatabaseName)
        SELECT @QueueID AS QueueID,
               DatabaseName
        FROM @tmpDatabases tmpDatabases
        WHERE Selected = 1
        AND NOT EXISTS (SELECT * FROM dbo.QueueDatabase WHERE DatabaseName COLLATE DATABASE_DEFAULT = tmpDatabases.DatabaseName AND QueueID = @QueueID)

        DELETE QueueDatabase
        FROM dbo.QueueDatabase QueueDatabase
        WHERE QueueID = @QueueID
        AND NOT EXISTS (SELECT * FROM @tmpDatabases tmpDatabases WHERE DatabaseName = QueueDatabase.DatabaseName COLLATE DATABASE_DEFAULT AND Selected = 1)

        UPDATE QueueDatabase
        SET DatabaseOrder = tmpDatabases.[Order]
        FROM dbo.QueueDatabase QueueDatabase
        INNER JOIN @tmpDatabases tmpDatabases ON QueueDatabase.DatabaseName COLLATE DATABASE_DEFAULT = tmpDatabases.DatabaseName
        WHERE QueueID = @QueueID
      END

      COMMIT TRANSACTION

      SELECT @QueueStartTime = QueueStartTime
      FROM dbo.[Queue]
      WHERE QueueID = @QueueID

    END TRY

    BEGIN CATCH
      IF XACT_STATE() <> 0
      BEGIN
        ROLLBACK TRANSACTION
      END
      SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar(max)) + ', ' + ISNULL(ERROR_MESSAGE(),'')
      RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
      RAISERROR(@EmptyLine,10,1) WITH NOWAIT
      SET @ReturnCode = ERROR_NUMBER()
      GOTO Logging
    END CATCH

  END

  ----------------------------------------------------------------------------------------------------
  --// Execute commands                                                                           //--
  ----------------------------------------------------------------------------------------------------

  WHILE (1 = 1)
  BEGIN -- Start of database loop

    IF @DatabasesInParallel = 'Y'
    BEGIN
      UPDATE QueueDatabase
      SET DatabaseStartTime = NULL,
          SessionID = NULL,
          RequestID = NULL,
          RequestStartTime = NULL
      FROM dbo.QueueDatabase QueueDatabase
      WHERE QueueID = @QueueID
      AND DatabaseStartTime IS NOT NULL
      AND DatabaseEndTime IS NULL
      AND NOT EXISTS (SELECT * FROM sys.dm_exec_requests WHERE session_id = QueueDatabase.SessionID AND request_id = QueueDatabase.RequestID AND start_time = QueueDatabase.RequestStartTime)

      UPDATE QueueDatabase
      SET DatabaseStartTime = SYSDATETIME(),
          DatabaseEndTime = NULL,
          SessionID = @@SPID,
          RequestID = (SELECT request_id FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          RequestStartTime = (SELECT start_time FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          @CurrentDatabaseName = DatabaseName
      FROM (SELECT TOP 1 DatabaseStartTime,
                         DatabaseEndTime,
                         SessionID,
                         RequestID,
                         RequestStartTime,
                         DatabaseName
            FROM dbo.QueueDatabase
            WHERE QueueID = @QueueID
            AND (DatabaseStartTime < @QueueStartTime OR DatabaseStartTime IS NULL)
            AND NOT (DatabaseStartTime IS NOT NULL AND DatabaseEndTime IS NULL)
            ORDER BY DatabaseOrder ASC
            ) QueueDatabase
    END
    ELSE
    BEGIN
      SELECT TOP 1 @CurrentDBID = ID,
                   @CurrentDatabaseName = DatabaseName
      FROM @tmpDatabases
      WHERE Selected = 1
      AND Completed = 0
      ORDER BY [Order] ASC
    END

    IF @@ROWCOUNT = 0
    BEGIN
      BREAK
    END

    SET @CurrentDatabase_sp_executesql = QUOTENAME(@CurrentDatabaseName) + '.sys.sp_executesql'

    BEGIN
      SET @DatabaseMessage = 'Date and time: ' + CONVERT(nvarchar(max),SYSDATETIME(),120)
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Database: ' + QUOTENAME(@CurrentDatabaseName)
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    SELECT @CurrentUserAccess = user_access_desc,
           @CurrentIsReadOnly = is_read_only,
           @CurrentDatabaseState = state_desc,
           @CurrentInStandby = is_in_standby,
           @CurrentRecoveryModel = recovery_model_desc
    FROM sys.databases
    WHERE [name] = @CurrentDatabaseName

    BEGIN
      SET @DatabaseMessage = 'State: ' + @CurrentDatabaseState
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Standby: ' + CASE WHEN @CurrentInStandby = 1 THEN 'Yes' ELSE 'No' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Updateability: ' + CASE WHEN @CurrentIsReadOnly = 1 THEN 'READ_ONLY' WHEN  @CurrentIsReadOnly = 0 THEN 'READ_WRITE' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'User access: ' + @CurrentUserAccess
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Recovery model: ' + @CurrentRecoveryModel
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    IF @IsHadrEnabled = 1
    BEGIN
      SELECT @CurrentAvailabilityGroupReplicaID = databases.replica_id
      FROM sys.databases databases
      INNER JOIN sys.availability_replicas availability_replicas ON databases.replica_id = availability_replicas.replica_id
      WHERE databases.[name] = @CurrentDatabaseName

      SELECT @CurrentAvailabilityGroupID = group_id
      FROM sys.availability_replicas
      WHERE replica_id = @CurrentAvailabilityGroupReplicaID

      SELECT @CurrentAvailabilityGroupRole = role_desc
      FROM sys.dm_hadr_availability_replica_states
      WHERE replica_id = @CurrentAvailabilityGroupReplicaID

      SELECT @CurrentAvailabilityGroup = [name]
      FROM sys.availability_groups
      WHERE group_id = @CurrentAvailabilityGroupID
    END

    IF @IsHadrEnabled = 1 AND @CurrentAvailabilityGroup IS NOT NULL
    BEGIN
      SELECT @CurrentDistributedAvailabilityGroup = availability_groups.[name],
             @CurrentDistributedAvailabilityGroupReplicaID = availability_replicas.replica_id
      FROM sys.availability_groups availability_groups
      INNER JOIN sys.availability_replicas availability_replicas ON availability_groups.group_id = availability_replicas.group_id
      INNER JOIN sys.availability_groups availability_groups_local ON availability_replicas.replica_server_name = availability_groups_local.[name]
      WHERE availability_groups.is_distributed = 1
      AND availability_groups_local.group_id = @CurrentAvailabilityGroupID

      SELECT @CurrentDistributedAvailabilityGroupRole = dm_hadr_availability_replica_states.role_desc
      FROM sys.dm_hadr_availability_replica_states dm_hadr_availability_replica_states
      WHERE dm_hadr_availability_replica_states.replica_id = @CurrentDistributedAvailabilityGroupReplicaID
    END

    IF @EngineEdition <> 5
    BEGIN
      SELECT @CurrentDatabaseMirroringRole = UPPER(mirroring_role_desc)
      FROM sys.database_mirroring database_mirroring
      INNER JOIN sys.databases databases ON database_mirroring.database_id = databases.database_id
      WHERE databases.[name] = @CurrentDatabaseName
    END

    IF @CurrentAvailabilityGroup IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Availability group: ' + ISNULL(@CurrentAvailabilityGroup,'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Availability group role: ' + ISNULL(@CurrentAvailabilityGroupRole,'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    IF @CurrentDistributedAvailabilityGroup IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Distributed availability group: ' + ISNULL(@CurrentDistributedAvailabilityGroup,'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Distributed availability group role: ' + ISNULL(@CurrentDistributedAvailabilityGroupRole,'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Replica role in distributed availability group: ' + CASE WHEN @CurrentDistributedAvailabilityGroupRole = 'PRIMARY' AND @CurrentAvailabilityGroupRole = 'PRIMARY' THEN 'Global primary'
                                                                                       WHEN @CurrentDistributedAvailabilityGroupRole = 'SECONDARY' AND @CurrentAvailabilityGroupRole = 'PRIMARY' THEN 'Forwarder'
                                                                                       WHEN @CurrentDistributedAvailabilityGroupRole = 'SECONDARY' AND @CurrentAvailabilityGroupRole = 'SECONDARY' THEN 'Secondary replica in secondary availability group'
                                                                                       WHEN @CurrentDistributedAvailabilityGroupRole = 'PRIMARY' AND @CurrentAvailabilityGroupRole = 'SECONDARY' THEN 'Secondary replica in primary availability group' ELSE 'N/A' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    IF @CurrentDatabaseMirroringRole IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Database mirroring role: ' + @CurrentDatabaseMirroringRole
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    RAISERROR(@EmptyLine,10,1) WITH NOWAIT

    IF @ExecuteAsUser IS NOT NULL
    AND @CurrentDatabaseState = 'ONLINE'
    AND NOT (@CurrentUserAccess = 'SINGLE_USER')
    AND NOT (@CurrentAvailabilityGroup IS NOT NULL AND (@CurrentAvailabilityGroupRole <> 'PRIMARY' OR @CurrentAvailabilityGroupRole IS NULL))
    AND NOT (@AmazonRDS = 1 AND @CurrentDatabaseName = 'rdsadmin')
    BEGIN
      SET @CurrentCommand = ''
      SET @CurrentCommand += 'IF EXISTS(SELECT * FROM sys.database_principals database_principals WHERE database_principals.[name] = @ParamExecuteAsUser) BEGIN SET @ParamExecuteAsUserExists = 1 END ELSE BEGIN SET @ParamExecuteAsUserExists = 0 END'

      EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamExecuteAsUser sysname, @ParamExecuteAsUserExists bit OUTPUT', @ParamExecuteAsUser = @ExecuteAsUser, @ParamExecuteAsUserExists = @CurrentExecuteAsUserExists OUTPUT
    END

    IF @CurrentExecuteAsUserExists = 0
    BEGIN
      SET @DatabaseMessage = 'The user ' + QUOTENAME(@ExecuteAsUser) + ' does not exist in the database ' + QUOTENAME(@CurrentDatabaseName) + '.'
      RAISERROR('%s',16,1,@DatabaseMessage) WITH NOWAIT
      SET @Error = @@ERROR
      SET @ReturnCode = @Error
      RAISERROR(@EmptyLine,10,1) WITH NOWAIT
    END

    IF @CurrentDatabaseState = 'ONLINE'
    AND NOT (@CurrentUserAccess = 'SINGLE_USER')
    AND NOT (@CurrentAvailabilityGroup IS NOT NULL AND (@CurrentAvailabilityGroupRole <> 'PRIMARY' OR @CurrentAvailabilityGroupRole IS NULL))
    AND NOT (@CurrentDistributedAvailabilityGroup IS NOT NULL AND (@CurrentDistributedAvailabilityGroupRole <> 'PRIMARY' OR @CurrentDistributedAvailabilityGroupRole IS NULL))
    AND NOT (@AmazonRDS = 1 AND @CurrentDatabaseName = 'rdsadmin')
    AND NOT (@CurrentIsReadOnly = 1)
    AND (@CurrentExecuteAsUserExists = 1 OR @CurrentExecuteAsUserExists IS NULL)
    BEGIN

      IF (EXISTS(SELECT * FROM @ActionsPreferred) OR @UpdateStatistics IS NOT NULL) AND (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
      BEGIN
        -- Select objects
        SET @CurrentCommand = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
                            + ' SELECT objects.[object_id] AS ObjectID'
                            + ', objects.[schema_id] AS SchemaID'
                            + ', schemas.[name] AS SchemaName'
                            + ', objects.[name] AS ObjectName'
                            + ', RTRIM(objects.[type]) AS ObjectType'
                            + ', ISNULL(tables.is_memory_optimized, 0) AS IsMemoryOptimized'
                            + ', ' + CASE WHEN @EngineEdition IN (3, 5, 8) AND EXISTS(SELECT * FROM @ActionsPreferred WHERE [Action] = 'INDEX_REBUILD_ONLINE') THEN 'CASE WHEN EXISTS(SELECT * FROM sys.indexes indexes WHERE indexes.[object_id] = objects.object_id AND [type] = 5) THEN 1 ELSE 0 END' ELSE 'NULL' END + ' AS HasClusteredColumnstore'
                            + ', ' + CASE WHEN @EngineEdition IN (3, 5, 8) AND EXISTS(SELECT * FROM @ActionsPreferred WHERE [Action] = 'INDEX_REBUILD_ONLINE') AND @Resumable = 'Y' THEN 'CASE WHEN EXISTS(SELECT * FROM sys.index_columns index_columns INNER JOIN sys.columns columns ON index_columns.object_id = columns.object_id AND index_columns.column_id = columns.column_id INNER JOIN sys.indexes indexes2 ON index_columns.object_id = indexes2.object_id AND index_columns.index_id = indexes2.index_id WHERE (index_columns.key_ordinal > 0 OR index_columns.partition_ordinal > 0) AND columns.is_computed = 1 AND indexes2.[type] = 1 AND index_columns.object_id = objects.object_id) THEN 1 ELSE 0 END' ELSE 'NULL' END + ' AS IsClusteredIndexComputed'
                            + ', CASE WHEN EXISTS(SELECT * FROM sys.indexes indexes WHERE indexes.[object_id] = objects.object_id AND [type] = 1 AND is_disabled = 1) THEN 1 ELSE 0 END AS IsClusteredIndexDisabled'
                            + ' FROM sys.objects objects'
                            + ' INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id]'
                            + ' LEFT OUTER JOIN sys.tables tables ON objects.[object_id] = tables.[object_id]'
                            + ' WHERE objects.[type] IN(''U'',''V'')'
                            + ' AND (tables.is_external = 0 OR tables.is_external IS NULL)'
                            + CASE WHEN @MSShippedObjects = 'N' THEN ' AND objects.is_ms_shipped = 0' ELSE '' END
        IF @Indexes IS NOT NULL AND EXISTS(SELECT * FROM @SelectedIndexes WHERE Selected = 1) AND NOT EXISTS(SELECT * FROM @SelectedIndexes WHERE Selected = 1 AND DatabaseName = '%' AND SchemaName = '%' AND ObjectName = '%')
        BEGIN
          SET @CurrentCommand += ' AND EXISTS(SELECT * FROM #SelectedIndexes SelectedIndexes WHERE @ParamDatabaseName LIKE REPLACE(REPLACE(SelectedIndexes.DatabaseName,''['',''[[]''),''_'',''[_]'') COLLATE ' + @Collation + ' AND schemas.[name] LIKE REPLACE(REPLACE(SelectedIndexes.SchemaName,''['',''[[]''),''_'',''[_]'') COLLATE ' + @Collation + ' AND objects.[name] LIKE REPLACE(REPLACE(SelectedIndexes.ObjectName,''['',''[[]''),''_'',''[_]'') COLLATE ' + @Collation + ' AND SelectedIndexes.Selected = 1)'
        END
        IF @Indexes IS NOT NULL AND EXISTS(SELECT * FROM @SelectedIndexes WHERE Selected = 0 AND IndexName = '%')
        BEGIN
          SET @CurrentCommand += ' AND NOT EXISTS(SELECT * FROM #SelectedIndexes SelectedIndexes WHERE @ParamDatabaseName LIKE REPLACE(REPLACE(SelectedIndexes.DatabaseName,''['',''[[]''),''_'',''[_]'') COLLATE ' + @Collation + ' AND schemas.[name] LIKE REPLACE(REPLACE(SelectedIndexes.SchemaName,''['',''[[]''),''_'',''[_]'') COLLATE ' + @Collation + ' AND objects.[name] LIKE REPLACE(REPLACE(SelectedIndexes.ObjectName,''['',''[[]''),''_'',''[_]'') COLLATE ' + @Collation + ' AND SelectedIndexes.IndexName = ''%'' AND SelectedIndexes.Selected = 0)'
        END

        INSERT INTO #Objects (ObjectID, SchemaID, SchemaName, ObjectName, ObjectType, IsMemoryOptimized, HasClusteredColumnstore, IsClusteredIndexComputed, IsClusteredIndexDisabled)
        EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamDatabaseName nvarchar(max)', @ParamDatabaseName = @CurrentDatabaseName
        SET @Error = @@ERROR
        IF @Error <> 0
        BEGIN
          SET @ReturnCode = @Error
        END

        -- Select indexes
        SET @CurrentCommand = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
                            + ' SELECT indexes.[object_id] AS ObjectID'
                            + ', indexes.index_id AS IndexID'
                            + ', indexes.[name] AS IndexName'
                            + ', indexes.[type] AS IndexType'
                            + ', indexes.data_space_id AS DataSpaceID'
                            + ', indexes.allow_page_locks AS AllowPageLocks'
                            + ', indexes.has_filter AS HasFilter'
                            + ', ' + CASE WHEN @EngineEdition IN (3, 5, 8) AND EXISTS(SELECT * FROM @ActionsPreferred WHERE [Action] = 'INDEX_REBUILD_ONLINE') THEN 'CASE WHEN indexes.[type] = 1 AND EXISTS(SELECT * FROM sys.columns columns INNER JOIN sys.types types ON columns.system_type_id = types.user_type_id WHERE columns.[object_id] = indexes.object_id AND types.name IN(''image'',''text'',''ntext'')) THEN 1 ELSE 0 END' ELSE 'NULL' END + ' AS IsImageText'
                            + ', ' + CASE WHEN @EngineEdition IN (3, 5, 8) AND EXISTS(SELECT * FROM @ActionsPreferred WHERE [Action] = 'INDEX_REBUILD_ONLINE') THEN 'CASE WHEN indexes.[type] = 1 AND EXISTS(SELECT * FROM sys.columns columns WHERE columns.[object_id] = indexes.object_id AND columns.is_filestream = 1) THEN 1 ELSE 0 END' ELSE 'NULL' END + ' AS IsFileStream'
                            + ', ' + CASE WHEN @EngineEdition IN (3, 5, 8) AND EXISTS(SELECT * FROM @ActionsPreferred WHERE [Action] = 'INDEX_REBUILD_ONLINE') AND (@Version >= 16 OR @EngineEdition = 5 OR (@EngineEdition = 8 AND @ProductUpdateType = 'Continuous')) THEN 'CASE WHEN EXISTS(SELECT * FROM sys.index_columns index_columns WHERE index_columns.[object_id] = indexes.[object_id] AND index_columns.index_id = indexes.index_id AND index_columns.column_store_order_ordinal = 1) THEN 1 ELSE 0 END' ELSE 'NULL' END + ' AS IsColumnstoreOrdered'
                            + ', ' + CASE WHEN @EngineEdition IN (3, 5, 8) AND EXISTS(SELECT * FROM @ActionsPreferred WHERE [Action] = 'INDEX_REBUILD_ONLINE') AND @Resumable = 'Y' THEN 'CASE WHEN EXISTS(SELECT * FROM sys.index_columns index_columns INNER JOIN sys.columns columns ON index_columns.object_id = columns.object_id AND index_columns.column_id = columns.column_id WHERE (index_columns.key_ordinal > 0 OR index_columns.partition_ordinal > 0 OR index_columns.is_included_column = 1) AND columns.is_computed = 1 AND index_columns.object_id = indexes.object_id AND index_columns.index_id = indexes.index_id) THEN 1 ELSE 0 END' ELSE 'NULL' END + ' AS IsComputed'
                            + ', ' + CASE WHEN @EngineEdition IN (3, 5, 8) AND EXISTS(SELECT * FROM @ActionsPreferred WHERE [Action] = 'INDEX_REBUILD_ONLINE') AND @Resumable = 'Y' THEN 'CASE WHEN EXISTS(SELECT * FROM sys.index_columns index_columns INNER JOIN sys.columns columns ON index_columns.[object_id] = columns.[object_id] AND index_columns.column_id = columns.column_id INNER JOIN sys.types types ON columns.system_type_id = types.system_type_id WHERE (index_columns.key_ordinal > 0 OR index_columns.partition_ordinal > 0) AND index_columns.[object_id] = indexes.[object_id] AND index_columns.index_id = indexes.index_id AND types.[name] = ''timestamp'') THEN 1 ELSE 0 END' ELSE 'NULL' END + ' AS IsTimestamp'
                            + ' FROM sys.indexes indexes'
                            + ' INNER JOIN #Objects Objects ON indexes.[object_id] = Objects.ObjectID'
                            + ' AND indexes.[type] IN(1,2,3,4,5,6,7)'
                            + ' AND indexes.is_disabled = 0'
                            + ' AND indexes.is_hypothetical = 0'

        INSERT INTO #Indexes (ObjectID, IndexID, IndexName, IndexType, DataSpaceID, AllowPageLocks, HasFilter, IsImageText, IsFileStream, IsColumnstoreOrdered, IsComputed, IsTimestamp)
        EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand
        SET @Error = @@ERROR
        IF @Error <> 0
        BEGIN
          SET @ReturnCode = @Error
        END

        -- Select statistics
        SET @CurrentCommand = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
                            + ' SELECT stats.[object_id] AS ObjectID'
                            + ', stats.stats_id AS StatisticsID'
                            + ', stats.name AS StatisticsName'
                            + ', stats.no_recompute AS NoRecompute'
                            + ', stats.is_incremental AS IsIncremental'
                            + ', CASE WHEN EXISTS(SELECT * FROM sys.indexes indexes WHERE indexes.[object_id] = stats.[object_id] AND indexes.index_id = stats.stats_id) THEN 1 ELSE 0 END AS IsIndex'
                            + ' FROM sys.stats stats'
                            + ' INNER JOIN #Objects Objects ON stats.[object_id] = Objects.ObjectID'

        INSERT INTO #Stats (ObjectID, StatisticsID, StatisticsName, [NoRecompute], IsIncremental, IsIndex)
        EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand
        SET @Error = @@ERROR
        IF @Error <> 0
        BEGIN
          SET @ReturnCode = @Error
        END

        -- Select paused resumable index operations
        SET @CurrentCommand = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
                            + ' SELECT index_resumable_operations.object_id AS ObjectID'
                            + ', index_resumable_operations.index_id AS IndexID'
                            + ', ' + CASE WHEN @PartitionLevel = 'Y' THEN 'index_resumable_operations.partition_number AS PartitionNumber' WHEN @PartitionLevel = 'N' THEN 'NULL AS PartitionNumber' END
                            + ' FROM sys.index_resumable_operations index_resumable_operations'
                            + ' WHERE index_resumable_operations.state_desc = ''PAUSED'''

        INSERT INTO @tmpResumableOperations (ObjectID, IndexID, PartitionNumber)
        EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand
        SET @Error = @@ERROR
        IF @Error <> 0
        BEGIN
          SET @ReturnCode = @Error
        END

        IF EXISTS(SELECT * FROM @ActionsPreferred) OR @UpdateStatistics IN('ALL','INDEX')
        BEGIN
          -- Check if there are read-only filegroups in the database
          SET @CurrentCommand = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
                              + ' SELECT @ParamDatabaseHasReadOnlyFileGroup = CASE WHEN EXISTS(SELECT * FROM sys.filegroups filegroups WHERE filegroups.is_read_only = 1) THEN 1 ELSE 0 END'
          EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamDatabaseHasReadOnlyFileGroup bit OUTPUT', @ParamDatabaseHasReadOnlyFileGroup = @CurrentDatabaseHasReadOnlyFileGroup OUTPUT
          SET @Error = @@ERROR
          IF @Error <> 0
          BEGIN
            SET @ReturnCode = @Error
          END

          -- Select clustered, nonclustered and hash indexes
          SET @CurrentCommand = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
                              + ' SELECT Objects.SchemaID AS SchemaID'
                              + ', Objects.SchemaName AS SchemaName'
                              + ', Objects.ObjectID AS ObjectID'
                              + ', Objects.ObjectName AS ObjectName'
                              + ', Objects.ObjectType AS ObjectType'
                              + ', Objects.IsMemoryOptimized AS IsMemoryOptimized'
                              + ', Indexes.IndexID AS IndexID'
                              + ', Indexes.IndexName AS IndexName'
                              + ', Indexes.IndexType AS IndexType'
                              + ', Indexes.AllowPageLocks AS AllowPageLocks'
                              + ', Indexes.HasFilter AS HasFilter'
                              + ', Indexes.IsImageText AS IsImageText'
                              + ', Indexes.IsFileStream AS IsFileStream'
                              + ', Objects.HasClusteredColumnstore AS HasClusteredColumnstore'
                              + ', Indexes.IsColumnstoreOrdered AS IsColumnstoreOrdered'
                              + ', Indexes.IsComputed AS IsComputed'
                              + ', Objects.IsClusteredIndexComputed AS IsClusteredIndexComputed'
                              + ', Indexes.IsTimestamp AS IsTimestamp'
                              + CASE WHEN @CurrentDatabaseHasReadOnlyFileGroup = 1 THEN ', CASE WHEN EXISTS (SELECT * FROM sys.indexes indexes2 INNER JOIN sys.destination_data_spaces destination_data_spaces ON Indexes.DataSpaceID = destination_data_spaces.partition_scheme_id INNER JOIN sys.filegroups filegroups ON destination_data_spaces.data_space_id = filegroups.data_space_id WHERE filegroups.is_read_only = 1 AND indexes2.[object_id] = Indexes.ObjectID AND indexes2.[index_id] = Indexes.IndexID' + CASE WHEN @PartitionLevel = 'Y' THEN ' AND destination_data_spaces.destination_id = partitions.partition_number' ELSE '' END + ') THEN 1'
                              + ' WHEN EXISTS (SELECT * FROM sys.indexes indexes2 INNER JOIN sys.filegroups filegroups ON Indexes.DataSpaceID = filegroups.data_space_id WHERE filegroups.is_read_only = 1 AND Indexes.ObjectID = indexes2.[object_id] AND Indexes.IndexID = indexes2.index_id) THEN 1'
                              + ' WHEN Indexes.IndexType = 1 AND EXISTS (SELECT * FROM sys.tables tables INNER JOIN sys.filegroups filegroups ON tables.lob_data_space_id = filegroups.data_space_id WHERE filegroups.is_read_only = 1 AND tables.[object_id] = Objects.ObjectID) THEN 1 ELSE 0 END AS OnReadOnlyFileGroup' ELSE ', 0 AS OnReadOnlyFileGroup' END
                              + ', 0 AS ResumableIndexOperation'
                              + CASE WHEN @UpdateStatistics IN('ALL','INDEX') THEN ', Stats.StatisticsID AS StatisticsID' ELSE ', NULL AS StatisticsID' END
                              + CASE WHEN @UpdateStatistics IN('ALL','INDEX') THEN ', Stats.StatisticsName AS StatisticsName' ELSE ', NULL AS StatisticsName' END
                              + CASE WHEN @UpdateStatistics IN('ALL','INDEX') THEN ', Stats.[NoRecompute] AS NoRecompute' ELSE ', NULL AS NoRecompute' END
                              + CASE WHEN @UpdateStatistics IN('ALL','INDEX') THEN ', Stats.IsIncremental AS IsIncremental' ELSE ', NULL AS IsIncremental' END
                              + ', ' + CASE WHEN @PartitionLevel = 'Y' THEN 'partitions.partition_id AS PartitionID' WHEN @PartitionLevel = 'N' THEN 'NULL AS PartitionID' END
                              + ', ' + CASE WHEN @PartitionLevel = 'Y' THEN 'partitions.partition_number AS PartitionNumber' WHEN @PartitionLevel = 'N' THEN 'NULL AS PartitionNumber' END
                              + ', ' + CASE WHEN @PartitionLevel = 'Y' AND (@MinNumberOfPages > 0 OR @MaxNumberOfPages IS NOT NULL) THEN 'dm_db_partition_stats.in_row_data_page_count AS InRowDataPageCount' ELSE 'NULL AS InRowDataPageCount' END
                              + ' FROM #Indexes Indexes'
                              + ' INNER JOIN #Objects Objects ON Indexes.ObjectID = Objects.ObjectID'
                              + CASE WHEN @UpdateStatistics IN('ALL','INDEX') THEN ' INNER JOIN #Stats Stats ON Indexes.ObjectID = Stats.ObjectID AND Indexes.IndexID = Stats.StatisticsID' ELSE '' END
          IF @PartitionLevel = 'Y'
          BEGIN
            SET @CurrentCommand += ' INNER JOIN sys.partitions partitions ON Indexes.ObjectID = partitions.[object_id] AND Indexes.IndexID = partitions.index_id'
          END
          IF @PartitionLevel = 'Y' AND (@MinNumberOfPages > 0 OR @MaxNumberOfPages IS NOT NULL)
          BEGIN
            SET @CurrentCommand += ' INNER JOIN sys.dm_db_partition_stats dm_db_partition_stats ON partitions.object_id = dm_db_partition_stats.object_id AND partitions.index_id = dm_db_partition_stats.index_id AND partitions.partition_number = dm_db_partition_stats.partition_number'
          END
          SET @CurrentCommand += ' WHERE Objects.ObjectType IN(''U'',''V'')'
                               + ' AND Indexes.IndexType IN(1,2,7)'
                               + CASE WHEN @PartitionLevel = 'Y' AND (@UpdateStatistics = 'COLUMNS' OR @UpdateStatistics IS NULL) AND @MinNumberOfPages > 0 THEN ' AND dm_db_partition_stats.in_row_data_page_count >= @ParamMinNumberOfPages' ELSE '' END
                               + CASE WHEN @PartitionLevel = 'Y' AND (@UpdateStatistics = 'COLUMNS' OR @UpdateStatistics IS NULL) AND @MaxNumberOfPages IS NOT NULL THEN ' AND dm_db_partition_stats.in_row_data_page_count <= @ParamMaxNumberOfPages' ELSE '' END

          INSERT INTO @tmpIndexesStatistics (SchemaID, SchemaName, ObjectID, ObjectName, ObjectType, IsMemoryOptimized, IndexID, IndexName, IndexType, AllowPageLocks, HasFilter, IsImageText, IsFileStream, HasClusteredColumnstore, IsColumnstoreOrdered, IsComputed, IsClusteredIndexComputed, IsTimestamp, OnReadOnlyFileGroup, ResumableIndexOperation, StatisticsID, StatisticsName, [NoRecompute], IsIncremental, PartitionID, PartitionNumber, InRowDataPageCount)
          EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamMinNumberOfPages int, @ParamMaxNumberOfPages int', @ParamMinNumberOfPages = @MinNumberOfPages, @ParamMaxNumberOfPages = @MaxNumberOfPages
          SET @Error = @@ERROR
          IF @Error <> 0
          BEGIN
            SET @ReturnCode = @Error
          END

          -- Select XML and spatial indexes
          SET @CurrentCommand = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
                              + ' SELECT Objects.SchemaID AS SchemaID'
                              + ', Objects.SchemaName AS SchemaName'
                              + ', Objects.ObjectID AS ObjectID'
                              + ', Objects.ObjectName AS ObjectName'
                              + ', Objects.ObjectType AS ObjectType'
                              + ', Objects.IsMemoryOptimized AS IsMemoryOptimized'
                              + ', Indexes.IndexID AS IndexID'
                              + ', Indexes.IndexName AS IndexName'
                              + ', Indexes.IndexType AS IndexType'
                              + ', Indexes.AllowPageLocks AS AllowPageLocks'
                              + ', Indexes.HasFilter AS HasFilter'
                              + ', Indexes.IsImageText AS IsImageText'
                              + ', Indexes.IsFileStream AS IsFileStream'
                              + ', Objects.HasClusteredColumnstore AS HasClusteredColumnstore'
                              + ', Indexes.IsColumnstoreOrdered AS IsColumnstoreOrdered'
                              + ', Indexes.IsComputed AS IsComputed'
                              + ', Objects.IsClusteredIndexComputed AS IsClusteredIndexComputed'
                              + ', Indexes.IsTimestamp AS IsTimestamp'
                              + CASE WHEN @CurrentDatabaseHasReadOnlyFileGroup = 1 THEN ', CASE WHEN EXISTS (SELECT * FROM sys.indexes indexes2 INNER JOIN sys.destination_data_spaces destination_data_spaces ON Indexes.DataSpaceID = destination_data_spaces.partition_scheme_id INNER JOIN sys.filegroups filegroups ON destination_data_spaces.data_space_id = filegroups.data_space_id WHERE filegroups.is_read_only = 1 AND indexes2.[object_id] = Indexes.ObjectID AND indexes2.[index_id] = Indexes.IndexID) THEN 1'
                              + ' WHEN EXISTS (SELECT * FROM sys.indexes indexes2 INNER JOIN sys.filegroups filegroups ON Indexes.DataSpaceID = filegroups.data_space_id WHERE filegroups.is_read_only = 1 AND Indexes.ObjectID = indexes2.[object_id] AND Indexes.IndexID = indexes2.index_id) THEN 1 ELSE 0 END AS OnReadOnlyFileGroup' ELSE ', 0 AS OnReadOnlyFileGroup' END
                              + ', 0 AS ResumableIndexOperation'
                              + ' FROM #Indexes Indexes'
                              + ' INNER JOIN #Objects Objects ON Indexes.ObjectID = Objects.ObjectID'
                              + ' WHERE Objects.ObjectType = ''U'''
                              + ' AND Indexes.IndexType IN(3,4)'

          INSERT INTO @tmpIndexesStatistics (SchemaID, SchemaName, ObjectID, ObjectName, ObjectType, IsMemoryOptimized, IndexID, IndexName, IndexType, AllowPageLocks, HasFilter, IsImageText, IsFileStream, HasClusteredColumnstore, IsColumnstoreOrdered, IsComputed, IsClusteredIndexComputed, IsTimestamp, OnReadOnlyFileGroup, ResumableIndexOperation)
          EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand
          SET @Error = @@ERROR
          IF @Error <> 0
          BEGIN
            SET @ReturnCode = @Error
          END

          -- Select columnstore indexes
          SET @CurrentCommand = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
                              + ' SELECT Objects.SchemaID AS SchemaID'
                              + ', Objects.SchemaName AS SchemaName'
                              + ', Objects.ObjectID AS ObjectID'
                              + ', Objects.ObjectName AS ObjectName'
                              + ', Objects.ObjectType AS ObjectType'
                              + ', Objects.IsMemoryOptimized AS IsMemoryOptimized'
                              + ', Indexes.IndexID AS IndexID'
                              + ', Indexes.IndexName AS IndexName'
                              + ', Indexes.IndexType AS IndexType'
                              + ', Indexes.AllowPageLocks AS AllowPageLocks'
                              + ', Indexes.HasFilter AS HasFilter'
                              + ', Indexes.IsImageText AS IsImageText'
                              + ', Indexes.IsFileStream AS IsFileStream'
                              + ', Objects.HasClusteredColumnstore AS HasClusteredColumnstore'
                              + ', Indexes.IsColumnstoreOrdered AS IsColumnstoreOrdered'
                              + ', Indexes.IsComputed AS IsComputed'
                              + ', Objects.IsClusteredIndexComputed AS IsClusteredIndexComputed'
                              + ', Indexes.IsTimestamp AS IsTimestamp'
                              + CASE WHEN @CurrentDatabaseHasReadOnlyFileGroup = 1 THEN ', CASE WHEN EXISTS (SELECT * FROM sys.indexes indexes2 INNER JOIN sys.destination_data_spaces destination_data_spaces ON Indexes.DataSpaceID = destination_data_spaces.partition_scheme_id INNER JOIN sys.filegroups filegroups ON destination_data_spaces.data_space_id = filegroups.data_space_id WHERE filegroups.is_read_only = 1 AND indexes2.[object_id] = Indexes.ObjectID AND indexes2.[index_id] = Indexes.IndexID' + CASE WHEN @PartitionLevel = 'Y' THEN ' AND destination_data_spaces.destination_id = partitions.partition_number' ELSE '' END + ') THEN 1'
                              + ' WHEN EXISTS (SELECT * FROM sys.indexes indexes2 INNER JOIN sys.filegroups filegroups ON Indexes.DataSpaceID = filegroups.data_space_id WHERE filegroups.is_read_only = 1 AND Indexes.ObjectID = indexes2.[object_id] AND Indexes.IndexID = indexes2.index_id) THEN 1'
                              + ' WHEN Indexes.IndexType = 1 AND EXISTS (SELECT * FROM sys.tables tables INNER JOIN sys.filegroups filegroups ON tables.lob_data_space_id = filegroups.data_space_id WHERE filegroups.is_read_only = 1 AND tables.[object_id] = Objects.ObjectID) THEN 1 ELSE 0 END AS OnReadOnlyFileGroup' ELSE ', 0 AS OnReadOnlyFileGroup' END
                              + ', 0 AS ResumableIndexOperation'
                              + ', NULL AS StatisticsID'
                              + ', NULL AS StatisticsName'
                              + ', NULL AS NoRecompute'
                              + ', NULL AS IsIncremental'
                              + ', ' + CASE WHEN @PartitionLevel = 'Y' THEN 'partitions.partition_id AS PartitionID' WHEN @PartitionLevel = 'N' THEN 'NULL AS PartitionID' END
                              + ', ' + CASE WHEN @PartitionLevel = 'Y' THEN 'partitions.partition_number AS PartitionNumber' WHEN @PartitionLevel = 'N' THEN 'NULL AS PartitionNumber' END
                              + ', ' + CASE WHEN @PartitionLevel = 'Y' AND (@MinNumberOfPages > 0 OR @MaxNumberOfPages IS NOT NULL) THEN 'dm_db_partition_stats.in_row_data_page_count AS InRowDataPageCount' ELSE 'NULL AS InRowDataPageCount' END
                              + ' FROM #Indexes Indexes'
                              + ' INNER JOIN #Objects Objects ON Indexes.ObjectID = Objects.ObjectID'
          IF @PartitionLevel = 'Y'
          BEGIN
            SET @CurrentCommand += ' INNER JOIN sys.partitions partitions ON Indexes.ObjectID = partitions.[object_id] AND Indexes.IndexID = partitions.index_id'
          END
          IF @PartitionLevel = 'Y' AND (@MinNumberOfPages > 0 OR @MaxNumberOfPages IS NOT NULL)
          BEGIN
            SET @CurrentCommand += ' INNER JOIN sys.dm_db_partition_stats dm_db_partition_stats ON partitions.object_id = dm_db_partition_stats.object_id AND partitions.index_id = dm_db_partition_stats.index_id AND partitions.partition_number = dm_db_partition_stats.partition_number'
          END
          SET @CurrentCommand += ' WHERE Objects.ObjectType = ''U'''
                               + ' AND Indexes.IndexType IN(5,6)'
                               + CASE WHEN @PartitionLevel = 'Y' AND (@UpdateStatistics = 'COLUMNS' OR @UpdateStatistics IS NULL) AND @MinNumberOfPages > 0 THEN ' AND dm_db_partition_stats.in_row_data_page_count >= @ParamMinNumberOfPages' ELSE '' END
                               + CASE WHEN @PartitionLevel = 'Y' AND (@UpdateStatistics = 'COLUMNS' OR @UpdateStatistics IS NULL) AND @MaxNumberOfPages IS NOT NULL THEN ' AND dm_db_partition_stats.in_row_data_page_count <= @ParamMaxNumberOfPages' ELSE '' END

          INSERT INTO @tmpIndexesStatistics (SchemaID, SchemaName, ObjectID, ObjectName, ObjectType, IsMemoryOptimized, IndexID, IndexName, IndexType, AllowPageLocks, HasFilter, IsImageText, IsFileStream, HasClusteredColumnstore, IsColumnstoreOrdered, IsComputed, IsClusteredIndexComputed, IsTimestamp, OnReadOnlyFileGroup, ResumableIndexOperation, StatisticsID, StatisticsName, [NoRecompute], IsIncremental, PartitionID, PartitionNumber, InRowDataPageCount)
          EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamMinNumberOfPages int, @ParamMaxNumberOfPages int', @ParamMinNumberOfPages = @MinNumberOfPages, @ParamMaxNumberOfPages = @MaxNumberOfPages
          SET @Error = @@ERROR
          IF @Error <> 0
          BEGIN
            SET @ReturnCode = @Error
          END

        END

        IF @UpdateStatistics IN('ALL','COLUMNS')
        BEGIN
          -- Select non-incremental column level statistics
          SET @CurrentCommand = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
                              + ' SELECT Objects.SchemaID AS SchemaID'
                              + ', Objects.SchemaName AS SchemaName'
                              + ', Objects.ObjectID AS ObjectID'
                              + ', Objects.ObjectName AS ObjectName'
                              + ', Objects.ObjectType AS ObjectType'
                              + ', Objects.IsMemoryOptimized AS IsMemoryOptimized'
                              + ', Stats.StatisticsID AS StatisticsID'
                              + ', Stats.StatisticsName AS StatisticsName'
                              + ', Stats.[NoRecompute] AS NoRecompute'
                              + ', Stats.IsIncremental AS IsIncremental'
                              + ', NULL AS PartitionNumber'
                              + ' FROM #Stats Stats'
                              + ' INNER JOIN #Objects Objects ON Stats.ObjectID = Objects.ObjectID'
                              + ' WHERE Stats.IsIndex = 0'
                              + ' AND Stats.IsIncremental = 0'
                              + ' AND Objects.IsClusteredIndexDisabled = 0'

          INSERT INTO @tmpIndexesStatistics (SchemaID, SchemaName, ObjectID, ObjectName, ObjectType, IsMemoryOptimized, StatisticsID, StatisticsName, [NoRecompute], IsIncremental, PartitionNumber)
          EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand
          SET @Error = @@ERROR
          IF @Error <> 0
          BEGIN
            SET @ReturnCode = @Error
          END

          -- Select incremental column level statistics
          SET @CurrentCommand = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
                              + ' SELECT Objects.SchemaID AS SchemaID'
                              + ', Objects.SchemaName AS SchemaName'
                              + ', Objects.ObjectID AS ObjectID'
                              + ', Objects.ObjectName AS ObjectName'
                              + ', Objects.ObjectType AS ObjectType'
                              + ', Objects.IsMemoryOptimized AS IsMemoryOptimized'
                              + ', Stats.StatisticsID AS StatisticsID'
                              + ', Stats.StatisticsName AS StatisticsName'
                              + ', Stats.[NoRecompute] AS NoRecompute'
                              + ', Stats.IsIncremental AS IsIncremental'
                              + ', ' + CASE WHEN @PartitionLevel = 'Y' THEN 'partitions.partition_number' ELSE 'NULL' END + ' AS PartitionNumber'
                              + ' FROM #Stats Stats'
                              + ' INNER JOIN #Objects Objects ON Stats.ObjectID = Objects.ObjectID'
          IF @PartitionLevel = 'Y'
          BEGIN
            SET @CurrentCommand += ' INNER JOIN sys.partitions partitions ON partitions.[object_id] = Stats.ObjectID AND partitions.index_id IN (0, 1)'
          END
          SET @CurrentCommand += ' WHERE Objects.IsMemoryOptimized = 0'
                               + ' AND Stats.IsIndex = 0'
                               + ' AND Stats.IsIncremental = 1'
                               + ' AND Objects.IsClusteredIndexDisabled = 0'

          INSERT INTO @tmpIndexesStatistics (SchemaID, SchemaName, ObjectID, ObjectName, ObjectType, IsMemoryOptimized, StatisticsID, StatisticsName, [NoRecompute], IsIncremental, PartitionNumber)
          EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand
          SET @Error = @@ERROR
          IF @Error <> 0
          BEGIN
            SET @ReturnCode = @Error
          END
        END

        UPDATE tmpIndexesStatistics
        SET tmpIndexesStatistics.ResumableIndexOperation = 1
        FROM @tmpIndexesStatistics tmpIndexesStatistics
        INNER JOIN @tmpResumableOperations tmpResumableOperations ON tmpIndexesStatistics.ObjectID = tmpResumableOperations.ObjectID AND tmpIndexesStatistics.IndexID = tmpResumableOperations.IndexID AND (tmpIndexesStatistics.PartitionNumber = tmpResumableOperations.PartitionNumber OR tmpResumableOperations.PartitionNumber IS NULL)
        OPTION (RECOMPILE)

        IF @PartitionLevel = 'Y'
        BEGIN
          UPDATE tmpIndexesStatistics
          SET tmpIndexesStatistics.PartitionCount = PartitionCounts.PartitionCount
          FROM @tmpIndexesStatistics tmpIndexesStatistics
          INNER JOIN (SELECT ObjectID, IndexID, COUNT(*) AS PartitionCount FROM @tmpIndexesStatistics WHERE IndexID IS NOT NULL GROUP BY ObjectID, IndexID) PartitionCounts ON tmpIndexesStatistics.ObjectID = PartitionCounts.ObjectID AND tmpIndexesStatistics.IndexID = PartitionCounts.IndexID
          OPTION (RECOMPILE)
        END

        IF @Indexes IS NULL
        BEGIN
          UPDATE tmpIndexesStatistics
          SET tmpIndexesStatistics.Selected = 1
          FROM @tmpIndexesStatistics tmpIndexesStatistics
        END
        ELSE
        BEGIN
          UPDATE tmpIndexesStatistics
          SET tmpIndexesStatistics.Selected = SelectedIndexes.Selected
          FROM @tmpIndexesStatistics tmpIndexesStatistics
          INNER JOIN @SelectedIndexes SelectedIndexes
          ON @CurrentDatabaseName LIKE REPLACE(REPLACE(SelectedIndexes.DatabaseName,'[','[[]'),'_','[_]') AND tmpIndexesStatistics.SchemaName LIKE REPLACE(REPLACE(SelectedIndexes.SchemaName,'[','[[]'),'_','[_]') AND tmpIndexesStatistics.ObjectName LIKE REPLACE(REPLACE(SelectedIndexes.ObjectName,'[','[[]'),'_','[_]') AND COALESCE(tmpIndexesStatistics.IndexName,tmpIndexesStatistics.StatisticsName) LIKE REPLACE(REPLACE(SelectedIndexes.IndexName,'[','[[]'),'_','[_]')
          WHERE SelectedIndexes.Selected = 1

          UPDATE tmpIndexesStatistics
          SET tmpIndexesStatistics.Selected = SelectedIndexes.Selected
          FROM @tmpIndexesStatistics tmpIndexesStatistics
          INNER JOIN @SelectedIndexes SelectedIndexes
          ON @CurrentDatabaseName LIKE REPLACE(REPLACE(SelectedIndexes.DatabaseName,'[','[[]'),'_','[_]') AND tmpIndexesStatistics.SchemaName LIKE REPLACE(REPLACE(SelectedIndexes.SchemaName,'[','[[]'),'_','[_]') AND tmpIndexesStatistics.ObjectName LIKE REPLACE(REPLACE(SelectedIndexes.ObjectName,'[','[[]'),'_','[_]') AND COALESCE(tmpIndexesStatistics.IndexName,tmpIndexesStatistics.StatisticsName) LIKE REPLACE(REPLACE(SelectedIndexes.IndexName,'[','[[]'),'_','[_]')
          WHERE SelectedIndexes.Selected = 0

          UPDATE tmpIndexesStatistics
          SET tmpIndexesStatistics.StartPosition = SelectedIndexes2.StartPosition
          FROM @tmpIndexesStatistics tmpIndexesStatistics
          INNER JOIN (SELECT tmpIndexesStatistics.SchemaName, tmpIndexesStatistics.ObjectName, tmpIndexesStatistics.IndexName, tmpIndexesStatistics.StatisticsName, MIN(SelectedIndexes.StartPosition) AS StartPosition
                      FROM @tmpIndexesStatistics tmpIndexesStatistics
                      INNER JOIN @SelectedIndexes SelectedIndexes
                      ON @CurrentDatabaseName LIKE REPLACE(REPLACE(SelectedIndexes.DatabaseName,'[','[[]'),'_','[_]') AND tmpIndexesStatistics.SchemaName LIKE REPLACE(REPLACE(SelectedIndexes.SchemaName,'[','[[]'),'_','[_]') AND tmpIndexesStatistics.ObjectName LIKE REPLACE(REPLACE(SelectedIndexes.ObjectName,'[','[[]'),'_','[_]') AND COALESCE(tmpIndexesStatistics.IndexName,tmpIndexesStatistics.StatisticsName) LIKE REPLACE(REPLACE(SelectedIndexes.IndexName,'[','[[]'),'_','[_]')
                      WHERE SelectedIndexes.Selected = 1
                      GROUP BY tmpIndexesStatistics.SchemaName, tmpIndexesStatistics.ObjectName, tmpIndexesStatistics.IndexName, tmpIndexesStatistics.StatisticsName) SelectedIndexes2
          ON tmpIndexesStatistics.SchemaName = SelectedIndexes2.SchemaName
          AND tmpIndexesStatistics.ObjectName = SelectedIndexes2.ObjectName
          AND (tmpIndexesStatistics.IndexName = SelectedIndexes2.IndexName OR tmpIndexesStatistics.IndexName IS NULL)
          AND (tmpIndexesStatistics.StatisticsName = SelectedIndexes2.StatisticsName OR tmpIndexesStatistics.StatisticsName IS NULL)
        END;

        WITH tmpIndexesStatistics AS (
        SELECT SchemaName, ObjectName, [Order], ROW_NUMBER() OVER (ORDER BY ISNULL(ResumableIndexOperation,0) DESC, StartPosition ASC, SchemaName ASC, ObjectName ASC, CASE WHEN IndexType IS NULL THEN 1 ELSE 0 END ASC, IndexType ASC, IndexName ASC, StatisticsName ASC, PartitionNumber ASC) AS RowNumber
        FROM @tmpIndexesStatistics tmpIndexesStatistics
        WHERE Selected = 1
        )
        UPDATE tmpIndexesStatistics
        SET [Order] = RowNumber

        SET @CurrentCommand = 'SELECT schemas.[name] AS SchemaName, objects.[name] AS ObjectName'
                            + ' FROM sys.objects objects'
                            + ' INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id]'
                            + ' WHERE objects.[type] IN(''U'',''V'')'
                            + ' AND EXISTS(SELECT * FROM #SelectedIndexes SelectedIndexes'
                            + ' WHERE SelectedIndexes.DatabaseName = @ParamDatabaseName'
                            + ' AND SelectedIndexes.SchemaName NOT LIKE ''%[%]%'''
                            + ' AND SelectedIndexes.ObjectName NOT LIKE ''%[%]%'''
                            + ' AND schemas.[name] = SelectedIndexes.SchemaName COLLATE ' + @Collation
                            + ' AND objects.[name] = SelectedIndexes.ObjectName COLLATE ' + @Collation + ')'

        INSERT INTO #ExistingObjects (SchemaName, ObjectName)
        EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamDatabaseName nvarchar(max)', @ParamDatabaseName = @CurrentDatabaseName

        SET @CurrentCommand = 'SELECT schemas.[name] AS SchemaName, objects.[name] AS ObjectName, [Names].[name] AS IndexName'
                            + ' FROM sys.objects objects'
                            + ' INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id]'
                            + ' CROSS APPLY (SELECT indexes.[name] FROM sys.indexes indexes WHERE indexes.[object_id] = objects.[object_id] AND indexes.[type] <> 0'
                            + ' UNION SELECT stats.[name] FROM sys.stats stats WHERE stats.[object_id] = objects.[object_id]) [Names]'
                            + ' WHERE objects.[type] IN(''U'',''V'')'
                            + ' AND EXISTS(SELECT * FROM #SelectedIndexes SelectedIndexes'
                            + ' WHERE SelectedIndexes.DatabaseName = @ParamDatabaseName'
                            + ' AND SelectedIndexes.SchemaName NOT LIKE ''%[%]%'''
                            + ' AND SelectedIndexes.ObjectName NOT LIKE ''%[%]%'''
                            + ' AND SelectedIndexes.IndexName NOT LIKE ''%[%]%'''
                            + ' AND schemas.[name] = SelectedIndexes.SchemaName COLLATE ' + @Collation
                            + ' AND objects.[name] = SelectedIndexes.ObjectName COLLATE ' + @Collation
                            + ' AND [Names].[name] = SelectedIndexes.IndexName COLLATE ' + @Collation + ')'

        INSERT INTO #ExistingIndexes (SchemaName, ObjectName, IndexName)
        EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamDatabaseName nvarchar(max)', @ParamDatabaseName = @CurrentDatabaseName

        SELECT @ErrorMessage = STRING_AGG(CAST(QUOTENAME(DatabaseName) AS nvarchar(max)) + '.' + QUOTENAME(SchemaName) + '.' + QUOTENAME(ObjectName), ', ')
                               WITHIN GROUP (ORDER BY DatabaseName ASC, SchemaName ASC, ObjectName ASC)
        FROM @SelectedIndexes SelectedIndexes
        WHERE DatabaseName = @CurrentDatabaseName
        AND SchemaName NOT LIKE '%[%]%'
        AND ObjectName NOT LIKE '%[%]%'
        AND IndexName LIKE '%[%]%'
        AND NOT EXISTS (SELECT * FROM #ExistingObjects WHERE SchemaName = SelectedIndexes.SchemaName AND ObjectName = SelectedIndexes.ObjectName)

        IF @ErrorMessage IS NOT NULL
        BEGIN
          SET @ErrorMessage = 'The following objects in the @Indexes parameter do not exist: ' + @ErrorMessage + '.'
          RAISERROR('%s',10,1,@ErrorMessage) WITH NOWAIT
          RAISERROR(@EmptyLine,10,1) WITH NOWAIT
        END

        SELECT @ErrorMessage = STRING_AGG(CAST(QUOTENAME(DatabaseName) AS nvarchar(max)) + '.' + QUOTENAME(SchemaName) + '.' + QUOTENAME(ObjectName) + '.' + QUOTENAME(IndexName), ', ')
                               WITHIN GROUP (ORDER BY DatabaseName ASC, SchemaName ASC, ObjectName ASC, IndexName ASC)
        FROM @SelectedIndexes SelectedIndexes
        WHERE DatabaseName = @CurrentDatabaseName
        AND SchemaName NOT LIKE '%[%]%'
        AND ObjectName NOT LIKE '%[%]%'
        AND IndexName NOT LIKE '%[%]%'
        AND NOT EXISTS (SELECT * FROM #ExistingIndexes WHERE SchemaName = SelectedIndexes.SchemaName AND ObjectName = SelectedIndexes.ObjectName AND IndexName = SelectedIndexes.IndexName)

        IF @ErrorMessage IS NOT NULL
        BEGIN
          SET @ErrorMessage = 'The following indexes in the @Indexes parameter do not exist: ' + @ErrorMessage + '.'
          RAISERROR('%s',10,1,@ErrorMessage) WITH NOWAIT
          RAISERROR(@EmptyLine,10,1) WITH NOWAIT
        END
      END

      WHILE (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
      BEGIN
        SELECT TOP 1 @CurrentIxID = ID,
                     @CurrentIxOrder = [Order],
                     @CurrentSchemaID = SchemaID,
                     @CurrentSchemaName = SchemaName,
                     @CurrentObjectID = ObjectID,
                     @CurrentObjectName = ObjectName,
                     @CurrentObjectType = ObjectType,
                     @CurrentIsMemoryOptimized = IsMemoryOptimized,
                     @CurrentIndexID = IndexID,
                     @CurrentIndexName = IndexName,
                     @CurrentIndexType = IndexType,
                     @CurrentAllowPageLocks = AllowPageLocks,
                     @CurrentHasFilter = HasFilter,
                     @CurrentIsImageText = IsImageText,
                     @CurrentIsFileStream = IsFileStream,
                     @CurrentHasClusteredColumnstore = HasClusteredColumnstore,
                     @CurrentIsColumnstoreOrdered = IsColumnstoreOrdered,
                     @CurrentIsComputed = IsComputed,
                     @CurrentIsClusteredIndexComputed = IsClusteredIndexComputed,
                     @CurrentIsTimestamp = IsTimestamp,
                     @CurrentOnReadOnlyFileGroup = OnReadOnlyFileGroup,
                     @CurrentResumableIndexOperation = ResumableIndexOperation,
                     @CurrentStatisticsID = StatisticsID,
                     @CurrentStatisticsName = StatisticsName,
                     @CurrentNoRecompute = [NoRecompute],
                     @CurrentIsIncremental = IsIncremental,
                     @CurrentPartitionID = PartitionID,
                     @CurrentPartitionNumber = PartitionNumber,
                     @CurrentPartitionCount = PartitionCount,
                     @CurrentInRowDataPageCount = InRowDataPageCount
        FROM @tmpIndexesStatistics
        WHERE Selected = 1
        AND Completed = 0
        ORDER BY [Order] ASC

        IF @@ROWCOUNT = 0
        BEGIN
          BREAK
        END

        -- Is the index a partition?
        IF @CurrentPartitionNumber IS NULL OR @CurrentPartitionCount = 1 BEGIN SET @CurrentIsPartition = 0 END ELSE BEGIN SET @CurrentIsPartition = 1 END

        IF ((@CurrentInRowDataPageCount >= @MinNumberOfPages OR @MinNumberOfPages = 0) AND (@CurrentInRowDataPageCount <= @MaxNumberOfPages OR @MaxNumberOfPages IS NULL)) OR @CurrentInRowDataPageCount IS NULL
        BEGIN
          -- Does the index exist?
          IF @CurrentIndexID IS NOT NULL AND EXISTS(SELECT * FROM @ActionsPreferred)
          BEGIN
            SET @CurrentCommand = ''

            IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar(max)) + '; '

            IF @CurrentIsPartition = 0 SET @CurrentCommand += 'IF EXISTS(SELECT * FROM sys.indexes indexes INNER JOIN sys.objects objects ON indexes.[object_id] = objects.[object_id] INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] IN(''U'',''V'') AND indexes.[type] IN(1,2,3,4,5,6,7) AND indexes.is_disabled = 0 AND indexes.is_hypothetical = 0 AND schemas.[schema_id] = @ParamSchemaID AND schemas.[name] = @ParamSchemaName AND objects.[object_id] = @ParamObjectID AND objects.[name] = @ParamObjectName AND objects.[type] = @ParamObjectType AND indexes.index_id = @ParamIndexID AND indexes.[name] = @ParamIndexName AND indexes.[type] = @ParamIndexType) BEGIN SET @ParamIndexExists = 1 END'
            IF @CurrentIsPartition = 1 SET @CurrentCommand += 'IF EXISTS(SELECT * FROM sys.indexes indexes INNER JOIN sys.objects objects ON indexes.[object_id] = objects.[object_id] INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] INNER JOIN sys.partitions partitions ON indexes.[object_id] = partitions.[object_id] AND indexes.index_id = partitions.index_id WHERE objects.[type] IN(''U'',''V'') AND indexes.[type] IN(1,2,3,4,5,6,7) AND indexes.is_disabled = 0 AND indexes.is_hypothetical = 0 AND schemas.[schema_id] = @ParamSchemaID AND schemas.[name] = @ParamSchemaName AND objects.[object_id] = @ParamObjectID AND objects.[name] = @ParamObjectName AND objects.[type] = @ParamObjectType AND indexes.index_id = @ParamIndexID AND indexes.[name] = @ParamIndexName AND indexes.[type] = @ParamIndexType AND partitions.partition_id = @ParamPartitionID AND partitions.partition_number = @ParamPartitionNumber) BEGIN SET @ParamIndexExists = 1 END'

            BEGIN TRY
              EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamSchemaID int, @ParamSchemaName sysname, @ParamObjectID int, @ParamObjectName sysname, @ParamObjectType sysname, @ParamIndexID int, @ParamIndexName sysname, @ParamIndexType int, @ParamPartitionID bigint, @ParamPartitionNumber int, @ParamIndexExists bit OUTPUT', @ParamSchemaID = @CurrentSchemaID, @ParamSchemaName = @CurrentSchemaName, @ParamObjectID = @CurrentObjectID, @ParamObjectName = @CurrentObjectName, @ParamObjectType = @CurrentObjectType, @ParamIndexID = @CurrentIndexID, @ParamIndexName = @CurrentIndexName, @ParamIndexType = @CurrentIndexType, @ParamPartitionID = @CurrentPartitionID, @ParamPartitionNumber = @CurrentPartitionNumber, @ParamIndexExists = @CurrentIndexExists OUTPUT

              IF @CurrentIndexExists IS NULL
              BEGIN
                SET @CurrentIndexExists = 0
                GOTO NoAction
              END
            END TRY
            BEGIN CATCH
              SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar(max)) + ', ' + ISNULL(ERROR_MESSAGE(),'') + CASE WHEN ERROR_NUMBER() = 1222 THEN ' The index ' + QUOTENAME(@CurrentIndexName) + ' on the object ' + QUOTENAME(@CurrentDatabaseName) + '.' + QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName) + ' is locked. It could not be checked if the index exists.' ELSE '' END
              SET @Severity = CASE WHEN ERROR_NUMBER() IN(1205,1222) THEN @LockMessageSeverity ELSE 16 END
              RAISERROR('%s',@Severity,1,@ErrorMessage) WITH NOWAIT
              RAISERROR(@EmptyLine,10,1) WITH NOWAIT

              IF NOT (ERROR_NUMBER() IN(1205,1222) AND @LockMessageSeverity = 10)
              BEGIN
                SET @ReturnCode = ERROR_NUMBER()
              END

              GOTO NoAction
            END CATCH
          END

          -- Is the index fragmented?
          IF @CurrentIndexID IS NOT NULL
          AND @CurrentOnReadOnlyFileGroup = 0
          AND EXISTS(SELECT * FROM @ActionsPreferred)
          AND (EXISTS(SELECT [Priority], [Action], COUNT(*) FROM @ActionsPreferred GROUP BY [Priority], [Action] HAVING COUNT(*) <> 3) OR @MinNumberOfPages > 0 OR @MaxNumberOfPages IS NOT NULL)
          BEGIN
            SET @CurrentCommand = ''

            IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar(max)) + '; '

            IF @CurrentPartitionNumber IS NULL
            BEGIN
              SET @CurrentCommand += 'SELECT @ParamFragmentationLevel = MAX(avg_fragmentation_in_percent), @ParamPageCount = SUM(page_count) FROM sys.dm_db_index_physical_stats(DB_ID(@ParamDatabaseName), @ParamObjectID, @ParamIndexID, NULL, ''LIMITED'') WHERE alloc_unit_type_desc = ''IN_ROW_DATA'' AND index_level = 0'
            END
            ELSE
            BEGIN
              SET @CurrentCommand += 'SELECT @ParamFragmentationLevel = avg_fragmentation_in_percent, @ParamPageCount = page_count FROM sys.dm_db_index_physical_stats(DB_ID(@ParamDatabaseName), @ParamObjectID, @ParamIndexID, @ParamPartitionNumber, ''LIMITED'') WHERE alloc_unit_type_desc = ''IN_ROW_DATA'' AND index_level = 0'
            END

            BEGIN TRY
              EXECUTE sp_executesql @stmt = @CurrentCommand, @params = N'@ParamDatabaseName nvarchar(max), @ParamObjectID int, @ParamIndexID int, @ParamPartitionNumber int, @ParamFragmentationLevel float OUTPUT, @ParamPageCount bigint OUTPUT', @ParamDatabaseName = @CurrentDatabaseName, @ParamObjectID = @CurrentObjectID, @ParamIndexID = @CurrentIndexID, @ParamPartitionNumber = @CurrentPartitionNumber, @ParamFragmentationLevel = @CurrentFragmentationLevel OUTPUT, @ParamPageCount = @CurrentPageCount OUTPUT
            END TRY
            BEGIN CATCH
              SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar(max)) + ', ' + ISNULL(ERROR_MESSAGE(),'') + CASE WHEN ERROR_NUMBER() = 1222 THEN ' The index ' + QUOTENAME(@CurrentIndexName) + ' on the object ' + QUOTENAME(@CurrentDatabaseName) + '.' + QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName) + ' is locked. The page_count and avg_fragmentation_in_percent could not be checked.' ELSE '' END
              SET @Severity = CASE WHEN ERROR_NUMBER() IN(1205,1222) THEN @LockMessageSeverity ELSE 16 END
              RAISERROR('%s',@Severity,1,@ErrorMessage) WITH NOWAIT
              RAISERROR(@EmptyLine,10,1) WITH NOWAIT

              IF NOT (ERROR_NUMBER() IN(1205,1222) AND @LockMessageSeverity = 10)
              BEGIN
                SET @ReturnCode = ERROR_NUMBER()
              END

              GOTO NoAction
            END CATCH
          END

          -- Select fragmentation group
          IF @CurrentIndexID IS NOT NULL AND @CurrentOnReadOnlyFileGroup = 0 AND EXISTS(SELECT * FROM @ActionsPreferred)
          BEGIN
            SET @CurrentFragmentationGroup = CASE
            WHEN @CurrentFragmentationLevel >= @FragmentationLevel2 THEN 'High'
            WHEN @CurrentFragmentationLevel >= @FragmentationLevel1 AND @CurrentFragmentationLevel < @FragmentationLevel2 THEN 'Medium'
            WHEN @CurrentFragmentationLevel < @FragmentationLevel1 THEN 'Low'
            END
          END

          -- Which actions are allowed?
          IF @CurrentIndexID IS NOT NULL AND EXISTS(SELECT * FROM @ActionsPreferred)
          BEGIN
            IF NOT (@CurrentOnReadOnlyFileGroup = 1)
            AND NOT (@CurrentIsMemoryOptimized = 1)
            AND NOT (@CurrentAllowPageLocks = 0)
            BEGIN
              INSERT INTO @CurrentActionsAllowed ([Action])
              VALUES ('INDEX_REORGANIZE')
            END
            IF NOT (@CurrentOnReadOnlyFileGroup = 1)
            AND NOT (@CurrentIsMemoryOptimized = 1)
            BEGIN
              INSERT INTO @CurrentActionsAllowed ([Action])
              VALUES ('INDEX_REBUILD_OFFLINE')
            END
            IF @EngineEdition IN (3, 5, 8)
            AND NOT (@CurrentOnReadOnlyFileGroup = 1)
            AND NOT (@CurrentIsMemoryOptimized = 1)
            AND NOT (@CurrentIndexType = 1 AND @CurrentIsImageText = 1 AND @CurrentIsImageText IS NOT NULL)
            AND NOT (@CurrentIndexType = 1 AND @CurrentIsFileStream = 1 AND @CurrentIsFileStream IS NOT NULL)
            AND NOT (@CurrentIndexType = 3)
            AND NOT (@CurrentIndexType = 4)
            AND NOT (@CurrentIndexType = 5 AND NOT (@Version >= 15 OR @EngineEdition = 5 OR (@EngineEdition = 8 AND @ProductUpdateType = 'Continuous')))
            AND NOT (@CurrentIndexType = 2 AND @CurrentHasClusteredColumnstore = 1 AND @CurrentHasClusteredColumnstore IS NOT NULL AND NOT (@Version >= 15 OR @EngineEdition = 5 OR (@EngineEdition = 8 AND @ProductUpdateType = 'Continuous')))
            AND NOT (@CurrentIndexType = 5 AND @CurrentIsColumnstoreOrdered = 1 AND @CurrentIsColumnstoreOrdered IS NOT NULL AND NOT (@Version >= 17 OR @EngineEdition = 5 OR (@EngineEdition = 8 AND @ProductUpdateType = 'Continuous')))
            BEGIN
              INSERT INTO @CurrentActionsAllowed ([Action])
              VALUES ('INDEX_REBUILD_ONLINE')
            END
          END

          -- Decide action
          IF @CurrentIndexID IS NOT NULL
          AND EXISTS(SELECT * FROM @ActionsPreferred)
          AND (@CurrentPageCount >= @MinNumberOfPages OR @MinNumberOfPages = 0)
          AND (@CurrentPageCount <= @MaxNumberOfPages OR @MaxNumberOfPages IS NULL)
          AND @CurrentResumableIndexOperation = 0
          BEGIN
            IF EXISTS(SELECT [Priority], [Action], COUNT(*) FROM @ActionsPreferred GROUP BY [Priority], [Action] HAVING COUNT(*) <> 3)
            BEGIN
              SELECT @CurrentAction = [Action]
              FROM @ActionsPreferred
              WHERE FragmentationGroup = @CurrentFragmentationGroup
              AND [Priority] = (SELECT MIN([Priority])
                                FROM @ActionsPreferred
                                WHERE FragmentationGroup = @CurrentFragmentationGroup
                                AND [Action] IN (SELECT [Action] FROM @CurrentActionsAllowed))
            END
            ELSE
            BEGIN
              SELECT @CurrentAction = [Action]
              FROM @ActionsPreferred
              WHERE [Priority] = (SELECT MIN([Priority])
                                  FROM @ActionsPreferred
                                  WHERE [Action] IN (SELECT [Action] FROM @CurrentActionsAllowed))
            END
          END

          IF @CurrentResumableIndexOperation = 1
          BEGIN
            SET @CurrentAction = 'INDEX_REBUILD_ONLINE'
          END

          SET @CurrentMaxDOP = @MaxDOP

          -- Workaround for limitation in SQL Server, http://support.microsoft.com/kb/2292737
          IF @CurrentAction = 'INDEX_REBUILD_ONLINE' AND @CurrentIndexType IN (1, 2) AND @CurrentAllowPageLocks = 0
          BEGIN
            SET @CurrentMaxDOP = 1
          END
        END

        -- Create index comment
        IF @CurrentAction IS NOT NULL
        BEGIN
          SET @CurrentComment = 'ObjectType: ' + CASE WHEN @CurrentObjectType = 'U' THEN 'Table' WHEN @CurrentObjectType = 'V' THEN 'View' ELSE 'N/A' END + ', '
          SET @CurrentComment += 'IndexType: ' + CASE WHEN @CurrentIndexType = 1 THEN 'Clustered' WHEN @CurrentIndexType = 2 THEN 'NonClustered' WHEN @CurrentIndexType = 3 THEN 'XML' WHEN @CurrentIndexType = 4 THEN 'Spatial' WHEN @CurrentIndexType = 5 THEN 'Clustered Columnstore' WHEN @CurrentIndexType = 6 THEN 'NonClustered Columnstore' WHEN @CurrentIndexType = 7 THEN 'NonClustered Hash' ELSE 'N/A' END + ', '
          IF @CurrentIsImageText IS NOT NULL SET @CurrentComment += 'ImageText: ' + CASE WHEN @CurrentIsImageText = 1 THEN 'Yes' WHEN @CurrentIsImageText = 0 THEN 'No' ELSE 'N/A' END + ', '
          IF @CurrentIsFileStream IS NOT NULL SET @CurrentComment += 'FileStream: ' + CASE WHEN @CurrentIsFileStream = 1 THEN 'Yes' WHEN @CurrentIsFileStream = 0 THEN 'No' ELSE 'N/A' END + ', '
          IF @CurrentHasClusteredColumnstore IS NOT NULL AND @CurrentIndexType NOT IN(5, 6) SET @CurrentComment += 'HasClusteredColumnstore: ' + CASE WHEN @CurrentHasClusteredColumnstore = 1 THEN 'Yes' WHEN @CurrentHasClusteredColumnstore = 0 THEN 'No' ELSE 'N/A' END + ', '
          IF @CurrentIsColumnstoreOrdered IS NOT NULL AND @CurrentIndexType = 5 SET @CurrentComment += 'IsColumnstoreOrdered: ' + CASE WHEN @CurrentIsColumnstoreOrdered = 1 THEN 'Yes' WHEN @CurrentIsColumnstoreOrdered = 0 THEN 'No' ELSE 'N/A' END + ', '
          IF @CurrentIsComputed IS NOT NULL SET @CurrentComment += 'Computed: ' + CASE WHEN @CurrentIsComputed = 1 THEN 'Yes' WHEN @CurrentIsComputed = 0 THEN 'No' ELSE 'N/A' END + ', '
          IF @CurrentIsClusteredIndexComputed IS NOT NULL AND @CurrentIndexType = 2 SET @CurrentComment += 'ClusteredIndexComputed: ' + CASE WHEN @CurrentIsClusteredIndexComputed = 1 THEN 'Yes' WHEN @CurrentIsClusteredIndexComputed = 0 THEN 'No' ELSE 'N/A' END + ', '
          IF @CurrentIsTimestamp IS NOT NULL SET @CurrentComment += 'Timestamp: ' + CASE WHEN @CurrentIsTimestamp = 1 THEN 'Yes' WHEN @CurrentIsTimestamp = 0 THEN 'No' ELSE 'N/A' END + ', '
          IF @Resumable = 'Y' SET @CurrentComment += 'HasFilter: ' + CASE WHEN @CurrentHasFilter = 1 THEN 'Yes' WHEN @CurrentHasFilter = 0 THEN 'No' ELSE 'N/A' END + ', '
          SET @CurrentComment += 'AllowPageLocks: ' + CASE WHEN @CurrentAllowPageLocks = 1 THEN 'Yes' WHEN @CurrentAllowPageLocks = 0 THEN 'No' ELSE 'N/A' END + ', '
          SET @CurrentComment += 'PageCount: ' + ISNULL(CAST(@CurrentPageCount AS nvarchar(max)),'N/A') + ', '
          SET @CurrentComment += 'Fragmentation: ' + ISNULL(CAST(@CurrentFragmentationLevel AS nvarchar(max)),'N/A')
        END

        IF @CurrentAction IS NOT NULL AND (@CurrentPageCount IS NOT NULL OR @CurrentFragmentationLevel IS NOT NULL)
        BEGIN
        SET @CurrentExtendedInfo = (SELECT *
                                    FROM (SELECT CAST(@CurrentPageCount AS nvarchar(max)) AS [PageCount],
                                                 CAST(@CurrentFragmentationLevel AS nvarchar(max)) AS Fragmentation
                                    ) ExtendedInfo FOR XML RAW('ExtendedInfo'), ELEMENTS)
        END

        IF @CurrentAction IS NOT NULL AND (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
        BEGIN
          SET @CurrentDatabaseContext = @CurrentDatabaseName

          SET @CurrentCommandType = 'ALTER_INDEX'

          SET @CurrentCommand = ''
          IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar(max)) + '; '
          SET @CurrentCommand += 'ALTER INDEX ' + QUOTENAME(@CurrentIndexName) + ' ON ' + QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName)
          IF @CurrentResumableIndexOperation = 1 SET @CurrentCommand += ' RESUME'
          IF @CurrentAction IN('INDEX_REBUILD_ONLINE','INDEX_REBUILD_OFFLINE') AND @CurrentResumableIndexOperation = 0 SET @CurrentCommand += ' REBUILD'
          IF @CurrentAction IN('INDEX_REORGANIZE') AND @CurrentResumableIndexOperation = 0 SET @CurrentCommand += ' REORGANIZE'
          IF @CurrentIsPartition = 1 AND @CurrentResumableIndexOperation = 0 SET @CurrentCommand += ' PARTITION = ' + CAST(@CurrentPartitionNumber AS nvarchar(max))

          IF @CurrentAction IN('INDEX_REBUILD_ONLINE','INDEX_REBUILD_OFFLINE') AND @SortInTempdb = 'Y' AND @CurrentIndexType IN(1,2,3,4) AND @CurrentResumableIndexOperation = 0
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            VALUES('SORT_IN_TEMPDB = ON')
          END

          IF @CurrentAction IN('INDEX_REBUILD_ONLINE','INDEX_REBUILD_OFFLINE') AND @SortInTempdb = 'N' AND @CurrentIndexType IN(1,2,3,4) AND @CurrentResumableIndexOperation = 0
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            VALUES('SORT_IN_TEMPDB = OFF')
          END

          IF @CurrentAction = 'INDEX_REBUILD_ONLINE' AND @CurrentResumableIndexOperation = 0
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            VALUES('ONLINE = ON' + CASE WHEN @WaitAtLowPriorityMaxDuration IS NOT NULL THEN ' (WAIT_AT_LOW_PRIORITY (MAX_DURATION = ' + CAST(@WaitAtLowPriorityMaxDuration AS nvarchar(max)) + ', ABORT_AFTER_WAIT = ' + UPPER(@WaitAtLowPriorityAbortAfterWait) + '))' ELSE '' END)
          END

          IF @CurrentAction = 'INDEX_REBUILD_ONLINE' AND @CurrentResumableIndexOperation = 1 AND @WaitAtLowPriorityMaxDuration IS NOT NULL
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            VALUES('WAIT_AT_LOW_PRIORITY (MAX_DURATION = ' + CAST(@WaitAtLowPriorityMaxDuration AS nvarchar(max)) + ', ABORT_AFTER_WAIT = ' + UPPER(@WaitAtLowPriorityAbortAfterWait) + ')')
          END

          IF @CurrentAction = 'INDEX_REBUILD_OFFLINE' AND @CurrentResumableIndexOperation = 0
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            VALUES('ONLINE = OFF')
          END

          IF @CurrentAction IN('INDEX_REBUILD_ONLINE','INDEX_REBUILD_OFFLINE') AND @CurrentMaxDOP IS NOT NULL
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            VALUES('MAXDOP = ' + CAST(@CurrentMaxDOP AS nvarchar(max)))
          END

          IF @CurrentAction IN('INDEX_REBUILD_ONLINE','INDEX_REBUILD_OFFLINE') AND @FillFactor IS NOT NULL AND @CurrentIsPartition = 0 AND @CurrentIndexType IN(1,2,3,4) AND @CurrentResumableIndexOperation = 0
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            VALUES('FILLFACTOR = ' + CAST(@FillFactor AS nvarchar(max)))
          END

          IF @CurrentAction IN('INDEX_REBUILD_ONLINE','INDEX_REBUILD_OFFLINE') AND @PadIndex IS NOT NULL AND @CurrentIsPartition = 0 AND @CurrentIndexType IN(1,2,3,4) AND @CurrentResumableIndexOperation = 0
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            VALUES('PAD_INDEX = ' + CASE WHEN @PadIndex = 'Y' THEN 'ON' WHEN @PadIndex = 'N' THEN 'OFF' END)
          END

          IF @CurrentAction IN('INDEX_REBUILD_ONLINE','INDEX_REBUILD_OFFLINE') AND @DataCompression IS NOT NULL AND @CurrentIndexType IN(1,2,4) AND @CurrentResumableIndexOperation = 0
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            VALUES('DATA_COMPRESSION = ' + @DataCompression)
          END

          IF @CurrentAction = 'INDEX_REBUILD_ONLINE' AND @CurrentResumableIndexOperation = 0
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            VALUES(CASE WHEN @Resumable = 'Y' AND @CurrentIndexType IN(1,2) AND (@CurrentIsComputed = 0 OR @CurrentIsComputed IS NULL) AND (@CurrentIsClusteredIndexComputed = 0 OR @CurrentIsClusteredIndexComputed IS NULL) AND (@CurrentIsTimestamp = 0 OR @CurrentIsTimestamp IS NULL) AND @CurrentHasFilter = 0 AND (@CurrentHasClusteredColumnstore = 0 OR @CurrentHasClusteredColumnstore IS NULL) THEN 'RESUMABLE = ON' ELSE 'RESUMABLE = OFF' END)
          END

          IF @CurrentAction = 'INDEX_REBUILD_ONLINE' AND ((@Resumable = 'Y' AND @CurrentIndexType IN(1,2) AND (@CurrentIsComputed = 0 OR @CurrentIsComputed IS NULL) AND (@CurrentIsClusteredIndexComputed = 0 OR @CurrentIsClusteredIndexComputed IS NULL) AND (@CurrentIsTimestamp = 0 OR @CurrentIsTimestamp IS NULL) AND @CurrentHasFilter = 0 AND (@CurrentHasClusteredColumnstore = 0 OR @CurrentHasClusteredColumnstore IS NULL)) OR @CurrentResumableIndexOperation = 1) AND @TimeLimit IS NOT NULL
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            VALUES('MAX_DURATION = ' + CAST(CASE WHEN DATEDIFF(MINUTE,SYSDATETIME(),DATEADD(SECOND,@TimeLimit,@StartTime)) < 1 THEN 1 WHEN DATEDIFF(MINUTE,SYSDATETIME(),DATEADD(SECOND,@TimeLimit,@StartTime)) > 10080 THEN 10080 ELSE DATEDIFF(MINUTE,SYSDATETIME(),DATEADD(SECOND,@TimeLimit,@StartTime)) END AS nvarchar(max)))
          END

          IF @CurrentAction IN('INDEX_REORGANIZE') AND @LOBCompaction = 'Y'
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            VALUES('LOB_COMPACTION = ON')
          END

          IF @CurrentAction IN('INDEX_REORGANIZE') AND @LOBCompaction = 'N'
          BEGIN
            INSERT INTO @CurrentAlterIndexWithClauseArguments (Argument)
            VALUES('LOB_COMPACTION = OFF')
          END

          IF EXISTS (SELECT * FROM @CurrentAlterIndexWithClauseArguments)
          BEGIN
            SELECT @CurrentCommand += ' WITH (' + STRING_AGG(Argument, ', ') WITHIN GROUP (ORDER BY ID ASC) + ')'
            FROM @CurrentAlterIndexWithClauseArguments
          END

          EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseContext, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 2, @Comment = @CurrentComment, @DatabaseName = @CurrentDatabaseName, @SchemaName = @CurrentSchemaName, @ObjectName = @CurrentObjectName, @ObjectType = @CurrentObjectType, @IndexName = @CurrentIndexName, @IndexType = @CurrentIndexType, @PartitionNumber = @CurrentPartitionNumber, @ExtendedInfo = @CurrentExtendedInfo, @LockMessageSeverity = @LockMessageSeverity, @ExecuteAsUser = @ExecuteAsUser, @LogToTable = @LogToTable, @Execute = @Execute
          SET @Error = @@ERROR
          IF @Error <> 0 SET @CurrentCommandOutput = @Error
          IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput

          IF @Delay > 0
          BEGIN
            SET @CurrentDelay = DATEADD(ss,@Delay,'1900-01-01')
            WAITFOR DELAY @CurrentDelay
          END
        END

        SET @CurrentMaxDOP = @MaxDOP

        -- Should the statistics be updated? - Pre checks and final decision
        IF @CurrentStatisticsID IS NOT NULL
        AND ((@UpdateStatistics = 'ALL' AND (@CurrentIndexType IN (1,2,7) OR @CurrentIndexID IS NULL)) OR (@UpdateStatistics = 'INDEX' AND @CurrentIndexID IS NOT NULL AND @CurrentIndexType IN (1,2,7)) OR (@UpdateStatistics = 'COLUMNS' AND @CurrentIndexID IS NULL))
        AND ((@CurrentIsPartition = 0 AND (@CurrentAction NOT IN('INDEX_REBUILD_ONLINE','INDEX_REBUILD_OFFLINE') OR @CurrentAction IS NULL)) OR (@CurrentIsPartition = 1 AND (@CurrentPartitionNumber = @CurrentPartitionCount OR (@PartitionLevel = 'Y' AND @CurrentIsIncremental = 1))))
        BEGIN
          -- Does the statistics exist?
          SET @CurrentCommand = ''

          IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar(max)) + '; '

          SET @CurrentCommand += 'IF EXISTS(SELECT * FROM sys.stats stats INNER JOIN sys.objects objects ON stats.[object_id] = objects.[object_id] INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] IN(''U'',''V'')' + CASE WHEN @MSShippedObjects = 'N' THEN ' AND objects.is_ms_shipped = 0' ELSE '' END + ' AND schemas.[schema_id] = @ParamSchemaID AND schemas.[name] = @ParamSchemaName AND objects.[object_id] = @ParamObjectID AND objects.[name] = @ParamObjectName AND objects.[type] = @ParamObjectType AND stats.stats_id = @ParamStatisticsID AND stats.[name] = @ParamStatisticsName) BEGIN SET @ParamStatisticsExists = 1 END'

          BEGIN TRY
            EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamSchemaID int, @ParamSchemaName sysname, @ParamObjectID int, @ParamObjectName sysname, @ParamObjectType sysname, @ParamStatisticsID int, @ParamStatisticsName sysname, @ParamStatisticsExists bit OUTPUT', @ParamSchemaID = @CurrentSchemaID, @ParamSchemaName = @CurrentSchemaName, @ParamObjectID = @CurrentObjectID, @ParamObjectName = @CurrentObjectName, @ParamObjectType = @CurrentObjectType, @ParamStatisticsID = @CurrentStatisticsID, @ParamStatisticsName = @CurrentStatisticsName, @ParamStatisticsExists = @CurrentStatisticsExists OUTPUT

            IF @CurrentStatisticsExists IS NULL
            BEGIN
              SET @CurrentStatisticsExists = 0
              GOTO NoAction
            END
          END TRY
          BEGIN CATCH
            SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar(max)) + ', ' + ISNULL(ERROR_MESSAGE(),'') + CASE WHEN ERROR_NUMBER() = 1222 THEN ' The statistics ' + QUOTENAME(@CurrentStatisticsName) + ' on the object ' + QUOTENAME(@CurrentDatabaseName) + '.' + QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName) + ' is locked. It could not be checked if the statistics exists.' ELSE '' END
            SET @Severity = CASE WHEN ERROR_NUMBER() IN(1205,1222) THEN @LockMessageSeverity ELSE 16 END
            RAISERROR('%s',@Severity,1,@ErrorMessage) WITH NOWAIT
            RAISERROR(@EmptyLine,10,1) WITH NOWAIT

            IF NOT (ERROR_NUMBER() IN(1205,1222) AND @LockMessageSeverity = 10)
            BEGIN
              SET @ReturnCode = ERROR_NUMBER()
            END

            GOTO NoAction
          END CATCH

          -- Check non-incremental statistics properties
          IF NOT (@OnlyModifiedStatistics = 'N' AND @StatisticsModificationLevel IS NULL) AND NOT (@PartitionLevel = 'Y' AND @CurrentIsIncremental = 1)
          BEGIN
            SET @CurrentCommand = ''

            IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar(max)) + '; '

            SET @CurrentCommand += 'SELECT @ParamRowCount = [rows], @ParamModificationCounter = modification_counter FROM sys.dm_db_stats_properties (@ParamObjectID, @ParamStatisticsID)'

            BEGIN TRY
              EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamObjectID int, @ParamStatisticsID int, @ParamRowCount bigint OUTPUT, @ParamModificationCounter bigint OUTPUT', @ParamObjectID = @CurrentObjectID, @ParamStatisticsID = @CurrentStatisticsID, @ParamRowCount = @CurrentRowCount OUTPUT, @ParamModificationCounter = @CurrentModificationCounter OUTPUT
            END TRY
            BEGIN CATCH
              SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar(max)) + ', ' + ISNULL(ERROR_MESSAGE(),'') + CASE WHEN ERROR_NUMBER() = 1222 THEN ' The statistics ' + QUOTENAME(@CurrentStatisticsName) + ' on the object ' + QUOTENAME(@CurrentDatabaseName) + '.' + QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName) + ' is locked. The rows and modification_counter could not be checked.' ELSE '' END
              SET @Severity = CASE WHEN ERROR_NUMBER() IN(1205,1222) THEN @LockMessageSeverity ELSE 16 END
              RAISERROR('%s',@Severity,1,@ErrorMessage) WITH NOWAIT
              RAISERROR(@EmptyLine,10,1) WITH NOWAIT

              IF NOT (ERROR_NUMBER() IN(1205,1222) AND @LockMessageSeverity = 10)
              BEGIN
                SET @ReturnCode = ERROR_NUMBER()
              END

              GOTO NoAction
            END CATCH
          END

          -- Check incremental statistics properties
          IF NOT (@OnlyModifiedStatistics = 'N' AND @StatisticsModificationLevel IS NULL) AND @PartitionLevel = 'Y' AND @CurrentIsIncremental = 1
          AND NOT EXISTS (SELECT * FROM @IncrementalStatsProperties WHERE ObjectID = @CurrentObjectID AND StatisticsID = @CurrentStatisticsID)
          BEGIN
            SET @CurrentCommand = ''

            IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar(max)) + '; '

            BEGIN
              SET @CurrentCommand += 'SELECT object_id, stats_id, partition_number, [rows], modification_counter FROM sys.dm_db_incremental_stats_properties (@ParamObjectID, @ParamStatisticsID)'
            END

            BEGIN TRY
              INSERT INTO @IncrementalStatsProperties (ObjectID, StatisticsID, PartitionNumber, [Rows], ModificationCounter)
              EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamObjectID int, @ParamStatisticsID int', @ParamObjectID = @CurrentObjectID, @ParamStatisticsID = @CurrentStatisticsID
            END TRY
            BEGIN CATCH
              SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar(max)) + ', ' + ISNULL(ERROR_MESSAGE(),'') + CASE WHEN ERROR_NUMBER() = 1222 THEN ' The statistics ' + QUOTENAME(@CurrentStatisticsName) + ' on the object ' + QUOTENAME(@CurrentDatabaseName) + '.' + QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName) + ' is locked. The rows and modification_counter could not be checked.' ELSE '' END
              SET @Severity = CASE WHEN ERROR_NUMBER() IN(1205,1222) THEN @LockMessageSeverity ELSE 16 END
              RAISERROR('%s',@Severity,1,@ErrorMessage) WITH NOWAIT
              RAISERROR(@EmptyLine,10,1) WITH NOWAIT

              IF NOT (ERROR_NUMBER() IN(1205,1222) AND @LockMessageSeverity = 10)
              BEGIN
                SET @ReturnCode = ERROR_NUMBER()
              END

              GOTO NoAction
            END CATCH
          END

          SELECT @CurrentRowCount = [Rows],
                 @CurrentModificationCounter = [ModificationCounter]
          FROM @IncrementalStatsProperties
          WHERE ObjectID = @CurrentObjectID
          AND StatisticsID = @CurrentStatisticsID
          AND PartitionNumber = @CurrentPartitionNumber

          -- Check partition statistics
          IF NOT (@OnlyModifiedStatistics = 'N' AND @StatisticsModificationLevel IS NULL) AND @CurrentModificationCounter IS NULL
          BEGIN
            SET @CurrentCommand = ''
            IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar(max)) + '; '

            IF @PartitionLevel = 'Y' AND @CurrentIsIncremental = 1
            BEGIN
              SET @CurrentCommand += 'SELECT @ParamObjectHasRows = CASE WHEN EXISTS (SELECT * FROM sys.dm_db_partition_stats WHERE [object_id] = @ParamObjectID AND index_id IN (0,1) AND partition_number = @ParamPartitionNumber AND row_count > 0) THEN 1 ELSE 0 END'
            END
            ELSE
            BEGIN
              SET @CurrentCommand += 'SELECT @ParamObjectHasRows = CASE WHEN EXISTS (SELECT * FROM sys.dm_db_partition_stats WHERE [object_id] = @ParamObjectID AND index_id IN (0,1) AND row_count > 0) THEN 1 ELSE 0 END'
            END

            BEGIN TRY
              EXECUTE @CurrentDatabase_sp_executesql @stmt = @CurrentCommand, @params = N'@ParamObjectID int, @ParamPartitionNumber int, @ParamObjectHasRows bit OUTPUT', @ParamObjectID = @CurrentObjectID, @ParamPartitionNumber = @CurrentPartitionNumber, @ParamObjectHasRows = @CurrentObjectHasRows OUTPUT
            END TRY
            BEGIN CATCH
              SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar(max)) + ', ' + ISNULL(ERROR_MESSAGE(),'') + CASE WHEN ERROR_NUMBER() = 1222 THEN ' The object ' + QUOTENAME(@CurrentDatabaseName) + '.' + QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName) + ' is locked. The row count could not be checked.' ELSE '' END
              SET @Severity = CASE WHEN ERROR_NUMBER() IN(1205,1222) THEN @LockMessageSeverity ELSE 16 END
              RAISERROR('%s',@Severity,1,@ErrorMessage) WITH NOWAIT
              RAISERROR(@EmptyLine,10,1) WITH NOWAIT
              IF NOT (ERROR_NUMBER() IN(1205,1222) AND @LockMessageSeverity = 10)
              BEGIN
                SET @ReturnCode = ERROR_NUMBER()
              END
              GOTO NoAction
            END CATCH
          END

          -- Update statistics?
          IF ((@OnlyModifiedStatistics = 'N' AND @StatisticsModificationLevel IS NULL) OR (@OnlyModifiedStatistics = 'Y' AND @CurrentModificationCounter > 0) OR ((@CurrentModificationCounter * 1. / NULLIF(@CurrentRowCount,0)) * 100 >= @StatisticsModificationLevel) OR (@StatisticsModificationLevel IS NOT NULL AND @CurrentModificationCounter > 0 AND (@CurrentModificationCounter >= SQRT(@CurrentRowCount * 1000))) OR ((@CurrentIndexType IN (1,2) OR @CurrentIndexID IS NULL) AND @CurrentModificationCounter IS NULL AND @CurrentObjectHasRows = 1))
          BEGIN
            SET @CurrentUpdateStatistics = 'Y'
          END
          ELSE
          BEGIN
            SET @CurrentUpdateStatistics = 'N'
          END
        END

        SET @CurrentStatisticsSample = @StatisticsSample
        SET @CurrentStatisticsPersistSample = @StatisticsPersistSample
        SET @CurrentStatisticsResample = @StatisticsResample

        -- Incremental statistics only supports RESAMPLE
        IF @PartitionLevel = 'Y' AND @CurrentIsIncremental = 1
        BEGIN
          SET @CurrentStatisticsSample = NULL
          SET @CurrentStatisticsPersistSample = NULL
          SET @CurrentStatisticsResample = 'Y'
        END

        -- Create statistics comment
        IF @CurrentUpdateStatistics = 'Y'
        BEGIN
          SET @CurrentComment = 'ObjectType: ' + CASE WHEN @CurrentObjectType = 'U' THEN 'Table' WHEN @CurrentObjectType = 'V' THEN 'View' ELSE 'N/A' END + ', '
          SET @CurrentComment += 'StatisticsType: ' + CASE WHEN @CurrentIndexID IS NOT NULL THEN 'Index' ELSE 'Column' END + ', '
          IF @CurrentIndexID IS NOT NULL SET @CurrentComment += 'IndexType: ' + CASE WHEN @CurrentIndexType = 1 THEN 'Clustered' WHEN @CurrentIndexType = 2 THEN 'NonClustered' WHEN @CurrentIndexType = 3 THEN 'XML' WHEN @CurrentIndexType = 4 THEN 'Spatial' WHEN @CurrentIndexType = 5 THEN 'Clustered Columnstore' WHEN @CurrentIndexType = 6 THEN 'NonClustered Columnstore' WHEN @CurrentIndexType = 7 THEN 'NonClustered Hash' ELSE 'N/A' END + ', '
          SET @CurrentComment += 'Incremental: ' + CASE WHEN @CurrentIsIncremental = 1 THEN 'Yes' WHEN @CurrentIsIncremental = 0 THEN 'No' ELSE 'N/A' END + ', '
          SET @CurrentComment += 'RowCount: ' + ISNULL(CAST(@CurrentRowCount AS nvarchar(max)),'N/A') + ', '
          SET @CurrentComment += 'ModificationCounter: ' + ISNULL(CAST(@CurrentModificationCounter AS nvarchar(max)),'N/A')
        END

        IF @CurrentUpdateStatistics = 'Y' AND (@CurrentRowCount IS NOT NULL OR @CurrentModificationCounter IS NOT NULL)
        BEGIN
          SET @CurrentExtendedInfo = (SELECT *
                                      FROM (SELECT CAST(@CurrentRowCount AS nvarchar(max)) AS [RowCount],
                                                   CAST(@CurrentModificationCounter AS nvarchar(max)) AS ModificationCounter
                                      ) ExtendedInfo FOR XML RAW('ExtendedInfo'), ELEMENTS)
        END
        ELSE
        BEGIN
          SET @CurrentExtendedInfo = NULL
        END

        IF @CurrentUpdateStatistics = 'Y' AND (SYSDATETIME() < DATEADD(SECOND,@TimeLimit,@StartTime) OR @TimeLimit IS NULL)
        BEGIN
          SET @CurrentDatabaseContext = @CurrentDatabaseName

          SET @CurrentCommandType = 'UPDATE_STATISTICS'

          SET @CurrentCommand = ''
          IF @LockTimeout IS NOT NULL SET @CurrentCommand = 'SET LOCK_TIMEOUT ' + CAST(@LockTimeout * 1000 AS nvarchar(max)) + '; '
          SET @CurrentCommand += 'UPDATE STATISTICS ' + QUOTENAME(@CurrentSchemaName) + '.' + QUOTENAME(@CurrentObjectName) + ' ' + QUOTENAME(@CurrentStatisticsName)

          IF @CurrentMaxDOP IS NOT NULL AND (@Version >= 14.03015 OR @EngineEdition = 5 OR (@EngineEdition = 8 AND @ProductUpdateType = 'Continuous'))
          BEGIN
            INSERT INTO @CurrentUpdateStatisticsWithClauseArguments (Argument)
            VALUES('MAXDOP = ' + CAST(@CurrentMaxDOP AS nvarchar(max)))
          END

          IF @CurrentStatisticsSample = 100
          BEGIN
            INSERT INTO @CurrentUpdateStatisticsWithClauseArguments (Argument)
            VALUES('FULLSCAN')
          END

          IF @CurrentStatisticsSample IS NOT NULL AND @CurrentStatisticsSample <> 100
          BEGIN
            INSERT INTO @CurrentUpdateStatisticsWithClauseArguments (Argument)
            VALUES('SAMPLE ' + CAST(@CurrentStatisticsSample AS nvarchar(max)) + ' PERCENT')
          END

          IF @CurrentStatisticsPersistSample = 'Y'
          BEGIN
            INSERT INTO @CurrentUpdateStatisticsWithClauseArguments (Argument)
            VALUES('PERSIST_SAMPLE_PERCENT = ON')
          END

          IF @CurrentStatisticsPersistSample = 'N'
          BEGIN
            INSERT INTO @CurrentUpdateStatisticsWithClauseArguments (Argument)
            VALUES('PERSIST_SAMPLE_PERCENT = OFF')
          END

          IF @CurrentNoRecompute = 1
          BEGIN
            INSERT INTO @CurrentUpdateStatisticsWithClauseArguments (Argument)
            VALUES('NORECOMPUTE')
          END

          IF @CurrentStatisticsResample = 'Y'
          BEGIN
            INSERT INTO @CurrentUpdateStatisticsWithClauseArguments (Argument)
            VALUES('RESAMPLE')
          END

          IF EXISTS (SELECT * FROM @CurrentUpdateStatisticsWithClauseArguments)
          BEGIN
            SELECT @CurrentCommand += ' WITH ' + STRING_AGG(Argument, ', ') WITHIN GROUP (ORDER BY ID ASC)
            FROM @CurrentUpdateStatisticsWithClauseArguments
          END

          IF @PartitionLevel = 'Y' AND @CurrentIsIncremental = 1 AND @CurrentPartitionNumber IS NOT NULL SET @CurrentCommand += ' ON PARTITIONS(' + CAST(@CurrentPartitionNumber AS nvarchar(max)) + ')'

          EXECUTE @CurrentCommandOutput = dbo.CommandExecute @DatabaseContext = @CurrentDatabaseContext, @Command = @CurrentCommand, @CommandType = @CurrentCommandType, @Mode = 2, @Comment = @CurrentComment, @DatabaseName = @CurrentDatabaseName, @SchemaName = @CurrentSchemaName, @ObjectName = @CurrentObjectName, @ObjectType = @CurrentObjectType, @IndexName = @CurrentIndexName, @IndexType = @CurrentIndexType, @StatisticsName = @CurrentStatisticsName, @PartitionNumber = @CurrentPartitionNumber, @ExtendedInfo = @CurrentExtendedInfo, @LockMessageSeverity = @LockMessageSeverity, @ExecuteAsUser = @ExecuteAsUser, @LogToTable = @LogToTable, @Execute = @Execute
          SET @Error = @@ERROR
          IF @Error <> 0 SET @CurrentCommandOutput = @Error
          IF @CurrentCommandOutput <> 0 SET @ReturnCode = @CurrentCommandOutput
        END

        NoAction:

        -- Update that the index or statistics is completed
        UPDATE @tmpIndexesStatistics
        SET Completed = 1
        WHERE Selected = 1
        AND Completed = 0
        AND [Order] = @CurrentIxOrder
        AND ID = @CurrentIxID

        -- Update that statistics on remaining partitions are completed where no update is needed
        IF (NOT EXISTS(SELECT * FROM @ActionsPreferred) OR @CurrentIndexID IS NULL) AND NOT (@OnlyModifiedStatistics = 'N' AND @StatisticsModificationLevel IS NULL) AND @CurrentStatisticsID IS NOT NULL
        BEGIN
          UPDATE tmpIndexesStatistics
          SET Completed = 1
          FROM @tmpIndexesStatistics tmpIndexesStatistics
          INNER JOIN @IncrementalStatsProperties IncrementalStatsProperties ON tmpIndexesStatistics.ObjectID = IncrementalStatsProperties.ObjectID AND tmpIndexesStatistics.StatisticsID = IncrementalStatsProperties.StatisticsID AND tmpIndexesStatistics.PartitionNumber = IncrementalStatsProperties.PartitionNumber
          WHERE tmpIndexesStatistics.ObjectID = @CurrentObjectID
          AND tmpIndexesStatistics.StatisticsID = @CurrentStatisticsID
          AND tmpIndexesStatistics.Selected = 1
          AND tmpIndexesStatistics.Completed = 0
          AND IncrementalStatsProperties.ModificationCounter IS NOT NULL
          AND ((@OnlyModifiedStatistics = 'Y' AND NOT (IncrementalStatsProperties.ModificationCounter > 0))
          OR (@StatisticsModificationLevel IS NOT NULL AND NOT ((IncrementalStatsProperties.ModificationCounter * 1. / NULLIF(IncrementalStatsProperties.[Rows],0)) * 100 >= @StatisticsModificationLevel OR (IncrementalStatsProperties.ModificationCounter > 0 AND IncrementalStatsProperties.ModificationCounter >= SQRT(IncrementalStatsProperties.[Rows] * 1000)))))
        END

        -- Clear variables
        SET @CurrentDatabaseContext = NULL

        SET @CurrentCommand = NULL
        SET @CurrentCommandOutput = NULL
        SET @CurrentCommandType = NULL
        SET @CurrentComment = NULL
        SET @CurrentExtendedInfo = NULL

        SET @CurrentIxID = NULL
        SET @CurrentIxOrder = NULL
        SET @CurrentSchemaID = NULL
        SET @CurrentSchemaName = NULL
        SET @CurrentObjectID = NULL
        SET @CurrentObjectName = NULL
        SET @CurrentObjectType = NULL
        SET @CurrentIsMemoryOptimized = NULL
        SET @CurrentIndexID = NULL
        SET @CurrentIndexName = NULL
        SET @CurrentIndexType = NULL
        SET @CurrentStatisticsID = NULL
        SET @CurrentStatisticsName = NULL
        SET @CurrentPartitionID = NULL
        SET @CurrentPartitionNumber = NULL
        SET @CurrentPartitionCount = NULL
        SET @CurrentInRowDataPageCount = NULL
        SET @CurrentIsPartition = NULL
        SET @CurrentIndexExists = NULL
        SET @CurrentStatisticsExists = NULL
        SET @CurrentIsImageText = NULL
        SET @CurrentIsFileStream = NULL
        SET @CurrentHasClusteredColumnstore = NULL
        SET @CurrentIsColumnstoreOrdered = NULL
        SET @CurrentIsComputed = NULL
        SET @CurrentIsClusteredIndexComputed = NULL
        SET @CurrentIsTimestamp = NULL
        SET @CurrentAllowPageLocks = NULL
        SET @CurrentHasFilter = NULL
        SET @CurrentNoRecompute = NULL
        SET @CurrentIsIncremental = NULL
        SET @CurrentObjectHasRows = NULL
        SET @CurrentRowCount = NULL
        SET @CurrentModificationCounter = NULL
        SET @CurrentOnReadOnlyFileGroup = NULL
        SET @CurrentResumableIndexOperation = NULL
        SET @CurrentFragmentationLevel = NULL
        SET @CurrentPageCount = NULL
        SET @CurrentFragmentationGroup = NULL
        SET @CurrentAction = NULL
        SET @CurrentMaxDOP = NULL
        SET @CurrentUpdateStatistics = NULL
        SET @CurrentStatisticsSample = NULL
        SET @CurrentStatisticsPersistSample = NULL
        SET @CurrentStatisticsResample = NULL

        DELETE FROM @CurrentActionsAllowed
        DELETE FROM @CurrentAlterIndexWithClauseArguments
        DELETE FROM @CurrentUpdateStatisticsWithClauseArguments

      END

    END

    IF @CurrentDatabaseState = 'SUSPECT'
    BEGIN
      SET @ErrorMessage = 'The database ' + QUOTENAME(@CurrentDatabaseName) + ' is in a SUSPECT state.'
      RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
      SET @Error = @@ERROR
      SET @ReturnCode = @Error
      RAISERROR(@EmptyLine,10,1) WITH NOWAIT
    END

    -- Update that the database is completed
    IF @DatabasesInParallel = 'Y'
    BEGIN
      UPDATE dbo.QueueDatabase
      SET DatabaseEndTime = SYSDATETIME()
      WHERE QueueID = @QueueID
      AND DatabaseName = @CurrentDatabaseName
    END
    ELSE
    BEGIN
      UPDATE @tmpDatabases
      SET Completed = 1
      WHERE Selected = 1
      AND Completed = 0
      AND ID = @CurrentDBID
    END

    -- Clear variables
    SET @CurrentDBID = NULL
    SET @CurrentDatabaseName = NULL

    SET @CurrentDatabase_sp_executesql = NULL

    SET @CurrentExecuteAsUserExists = NULL
    SET @CurrentUserAccess = NULL
    SET @CurrentIsReadOnly = NULL
    SET @CurrentDatabaseState = NULL
    SET @CurrentInStandby = NULL
    SET @CurrentRecoveryModel = NULL
    SET @CurrentDatabaseHasReadOnlyFileGroup = NULL

    SET @CurrentAvailabilityGroupReplicaID = NULL
    SET @CurrentAvailabilityGroupID = NULL
    SET @CurrentAvailabilityGroup = NULL
    SET @CurrentAvailabilityGroupRole = NULL
    SET @CurrentDistributedAvailabilityGroup = NULL
    SET @CurrentDistributedAvailabilityGroupReplicaID = NULL
    SET @CurrentDistributedAvailabilityGroupRole = NULL

    SET @CurrentDatabaseMirroringRole = NULL

    SET @CurrentCommand = NULL

    DELETE FROM @tmpIndexesStatistics

    TRUNCATE TABLE #Objects
    TRUNCATE TABLE #Indexes
    TRUNCATE TABLE #Stats
    TRUNCATE TABLE #ExistingObjects
    TRUNCATE TABLE #ExistingIndexes
    DELETE FROM @tmpResumableOperations
    DELETE FROM @IncrementalStatsProperties

  END -- End of database loop

  ----------------------------------------------------------------------------------------------------
  --// Log completing information                                                                 //--
  ----------------------------------------------------------------------------------------------------

  Logging:
  SET @EndMessage = 'Date and time: ' + CONVERT(nvarchar(max),SYSDATETIME(),120)
  RAISERROR('%s',10,1,@EndMessage) WITH NOWAIT

  RAISERROR(@EmptyLine,10,1) WITH NOWAIT

  IF @ReturnCode <> 0
  BEGIN
    RETURN @ReturnCode
  END

  ----------------------------------------------------------------------------------------------------

END

GO

