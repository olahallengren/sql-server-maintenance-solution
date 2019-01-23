USE master
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DatabaseBackup]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[DatabaseBackup] AS'
END
GO
ALTER PROCEDURE [dbo].[DatabaseBackup]

@Databases nvarchar(max) = NULL,
@Directory nvarchar(max) = NULL,
@BackupType nvarchar(max),   -- new Type 'COPY_PENDING_MIRRORS' - see @MirrorWhenNotAvailable for details
@Verify nvarchar(max) = 'N', --  additional to Y and N you can now specify 'SKIP_MIRRORS' to verify the original backup but not the mirrors 
                             --  (particularly usefull with @MirrorType = 'COPY' or 'COPY_LATER')
@CleanupTime int = NULL,
@CleanupMode nvarchar(max) = 'AFTER_BACKUP',
@CleanupUsesCMD CHAR(1) = 'N', -- when = Y it will use a xp_cmd_shell call instead of xp_delete_files, because xp_delete_files could be extremly slow 
                               -- if you do frequent log backups, particularly on SQL 2008 R2 before CU11 (> 2 h runtime)
@Compress nvarchar(max) = NULL,
@CopyOnly nvarchar(max) = 'N',
@ChangeBackupType nvarchar(max) = 'N',
@BackupSoftware nvarchar(max) = NULL,
@CheckSum nvarchar(max) = 'N',
@BlockSize int = NULL,
@BufferCount int = NULL,
@MaxTransferSize int = NULL,
@NumberOfFiles int = NULL,
@CompressionLevel int = NULL,
@Description nvarchar(max) = NULL,
@Threads int = NULL,
@Throttle int = NULL,
@Encrypt nvarchar(max) = 'N',
@EncryptionAlgorithm nvarchar(max) = NULL,
@ServerCertificate nvarchar(max) = NULL,
@ServerAsymmetricKey nvarchar(max) = NULL,
@EncryptionKey nvarchar(max) = NULL,
@ReadWriteFileGroups nvarchar(max) = 'N',
@OverrideBackupPreference nvarchar(max) = 'N',
@NoRecovery nvarchar(max) = 'N',
@URL nvarchar(max) = NULL,
@Credential nvarchar(max) = NULL,
@MirrorDirectory nvarchar(max) = NULL,
@MirrorCleanupTime int = NULL,
@MirrorCleanupMode nvarchar(max) = 'AFTER_BACKUP',
@MirrorURL nvarchar(max) = NULL,
@MirrorDirectory2 nvarchar(max) = NULL,
@MirrorCleanupTime2 int = NULL,
@MirrorCleanupMode2 nvarchar(max) = 'AFTER_BACKUP',
@MirrorDirectory3 nvarchar(max) = NULL,
@MirrorCleanupTime3 int = NULL,
@MirrorCleanupMode3 nvarchar(max) = 'AFTER_BACKUP',

--  Behavior, if the mirrors are not available (e.g. because of restart)
-- 'ERROR'      - the procedure / SQL Agent Job fails; no backup is done
-- 'SKIP'       - the mirror will be ignored, backup will be done on main @Directory and other @MirrorDirectory (if more than one is specified)
-- 'COPY_LATER' - will COPY the backup file(s) from the @Directory to the mirror server as soon ther mirror is available again
--                This will be helpfull, if your mirror is another Server and will f.e. reboot after a windows update.
--                Limitations / Rules:
--                - per default it will copy the skipped files regardless of the Backup type. So - if your mirror is offline, while you are doing a FULL backup and comes online 
--                  again when your next LOG backup occurs, the LOG backup job will copy the full backup to the (original) mirror 
--                  (regardless, if the mirrors specified to the LOG backup are the same as the ones for the FULL backup)
--                - per default it will copy the files after the regular backup is done (so your LOG backup will be done intime, even if the COPY of the FULL backup takes 10 min)
--                - if you don't like this (e.g. because you do LOG Backups every minute), set @SkipPendingMirrorCopies to 'Y' 
--                  and create a new backup job with @BackupType = 'COPY_PENDING_MIRRORS' which will take no new backup but will copy all pendings
--                - it will not copy files older than @MirrorCleanupTime
--                - it will save the commands that will be used for the copy into the master.dbo.CommandLog table, even if @LogToTable is set to 'N'
--                - when a new record is inserted (because a mirror was not available or MirrorType = 'COPY_LATER') the CommandType will be 'PENDING_COPY', 
--                  IndexType the Mirror number (1-3), StartTime the time of the original backup and EndTime the StartTime + @MirrorCleanupTime
--                - when the copy was successfull EndTime will be set to the timestamp of the copy -> pending copies have an EndTime > GetDate()
--                - the Verify and Cleanup command (when CleanUpMode = AFTER_BACKUP) will be excecuted after the last file copy (per mirror)
@MirrorWhenNotAvailable  NVARCHAR(max) = 'ERROR', 
@MirrorWhenNotAvailable2 NVARCHAR(max) = 'ERROR', 
@MirrorWhenNotAvailable3 NVARCHAR(max) = 'ERROR', 

@MirrorType NVARCHAR(max) = 'DEFAULT', -- WHEN 'DEFAULT' it uses the MIRROR TO parameter on SQL Enterprise Editions (or when using an external backup software)
                                       -- WHEN 'COPY' (or the backup runs without external software on a non Enterprise SQL Server) it makes a windows file copy of the original backup files
                                       --             Caution: this mirror type is less secure than the DEFAULT, because there is a little chance that data on the backup @Directory are
                                       --                      altered / delete (by user or firmware or controller bug) between the original backup an the end of the copy process
                                       -- WHEN 'COPY_LATER': creates an entry in the CommandLog table, which causes the next procedure call with @SkipPendingMirrorCopies = 'N' 
                                       --                    or with @BackupType = 'COPY_PENDING_MIRRORS' to copy the files
                                       --                    This makes most sense, if your Mirror target is on a remote site and you want to "mirror" the files asynchron
                                       --                    (use @SkipPendingMirrorCopies = 'Y' on your regular backups and create an extra job with @BackupType = 'COPY_PENDING_MIRRORS')
@MirrorCopyCommand NVARCHAR(100) = 'ROBOCOPY', -- let you use COPY, XCOPY or ROBOCOPY for copying the files to the mirror;
                                    -- This will be necessary when you have databases / backup directories / mirror directories 
                                    -- with very long filenames (e.g. some MS Sharepoint databases that have an UNID in the database name), because COPY fails when the 
                                    -- full file name (source or target) is longer than 256 characters
                                    -- ROBOCOPY supports long pathes too and uses parameters to retry 3 times (every 30 seconds) when the copy fails
@XCOPYFileLetter VARCHAR(10) = NULL,-- must be specified, if @MirrorCopyCommand = 'XCOPY'. When you run 'xcopy c:\temp\file1.txt c:\temp\file2.txt' (c:\temp\file1.txt must exists)
                                    -- XCOPY will ask you, if file2.txt is a (F)ile or (D)irectory. Sadly there is no parameter to skip this question when copying single file 
                                    -- (/I works only for multiple files) and the question / answer is translated in the OS language of the SQL Server.
                                    -- So you have to specify the "answer letter" for the file-answere (e.g. "F" for English "File" or "D" for German "Datei")
                                    -- Caution: if you specify an invalid letter (e.g. "X") the XCOPY statement will never finish -> Job hangs
                                    -- ========
                                    -- Hint: If some of your servers are German and some English, you could specify "FD" too (on the German servers it would ignore the
                                    -- ===== wrong choice "F" and use the correct "D"; be aware that "DF" would not work, since it would answere "D(irectory)" to the English servers
@SkipPendingMirrorCopies NVARCHAR(max) = 'N',

@CleanUpStartTime        TIME = NULL, -- When @CleanUpStartTime and @CleanUpEndTime are specified the delition of old backup files will be only executed, 
@CleanUpEndTime          TIME = NULL, -- when the procedure was called in this timeframe. 
                                      -- Reason: xp_deletefile can be very slow, if there are many files in the backup directory (e.g. because 
                                      -- LOG backups are taken every minute and cleaned up after 14 days -> 14 * 24 * 60 = 20160 files) and should be only 
                                      -- executed once per night to prevent delays in the main working time. 
                                      -- Hint: Set the @CleanUpEndTime as @CleanUpStartTime + regular backup interval to prevent multiple executions
@AvailabilityGroups nvarchar(max) = NULL,
@Updateability nvarchar(max) = 'ALL',
@AdaptiveCompression nvarchar(max) = NULL,
@ModificationLevel int = NULL,
@LogSizeSinceLastLogBackup int = NULL,
@TimeSinceLastLogBackup int = NULL,
@DataDomainBoostHost nvarchar(max) = NULL,
@DataDomainBoostUser nvarchar(max) = NULL,
@DataDomainBoostDevicePath nvarchar(max) = NULL,
@DataDomainBoostLockboxPath nvarchar(max) = NULL,
@DirectoryStructure nvarchar(max) = '{ServerName}${InstanceName}{DirectorySeparator}{DatabaseName}{DirectorySeparator}{BackupType}_{Partial}_{CopyOnly}',
@AvailabilityGroupDirectoryStructure nvarchar(max) = '{ClusterName}${AvailabilityGroupName}{DirectorySeparator}{DatabaseName}{DirectorySeparator}{BackupType}_{Partial}_{CopyOnly}',
@FileName nvarchar(max) = '{ServerName}${InstanceName}_{DatabaseName}_{BackupType}_{Partial}_{CopyOnly}_{Year}{Month}{Day}_{Hour}{Minute}{Second}_{FileNumber}.{FileExtension}',
@AvailabilityGroupFileName nvarchar(max) = '{ClusterName}${AvailabilityGroupName}_{DatabaseName}_{BackupType}_{Partial}_{CopyOnly}_{Year}{Month}{Day}_{Hour}{Minute}{Second}_{FileNumber}.{FileExtension}',
@FileExtensionFull nvarchar(max) = NULL,
@FileExtensionDiff nvarchar(max) = NULL,
@FileExtensionLog nvarchar(max) = NULL,
@Init nvarchar(max) = 'N',
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
  --// Version: 2019-01-13 13:51:41                                                               //--
  ----------------------------------------------------------------------------------------------------

  SET NOCOUNT ON

  DECLARE @StartMessage nvarchar(max)
  DECLARE @EndMessage nvarchar(max)
  DECLARE @DatabaseMessage nvarchar(max)
  DECLARE @ErrorMessage nvarchar(max)

  DECLARE @StartTime datetime
  DECLARE @SchemaName nvarchar(max)
  DECLARE @ObjectName nvarchar(max)
  DECLARE @VersionTimestamp nvarchar(max)
  DECLARE @Parameters nvarchar(max)

  DECLARE @Version numeric(18,10)
  DECLARE @HostPlatform nvarchar(max)
  DECLARE @DirectorySeparator nvarchar(max)
  DECLARE @AmazonRDS bit

  DECLARE @Updated bit

  DECLARE @Cluster nvarchar(max)

  DECLARE @DefaultDirectory nvarchar(4000)

  DECLARE @QueueID int
  DECLARE @QueueStartTime datetime

  DECLARE @CurrentRootDirectoryID int
  DECLARE @CurrentRootDirectoryPath nvarchar(4000)

  DECLARE @CurrentDBID int
  DECLARE @CurrentDatabaseID int
  DECLARE @CurrentDatabaseName nvarchar(max)
  DECLARE @CurrentBackupType nvarchar(max)
  DECLARE @CurrentFileExtension nvarchar(max)
  DECLARE @CurrentFileNumber int
  DECLARE @CurrentDifferentialBaseLSN numeric(25,0)
  DECLARE @CurrentDifferentialBaseIsSnapshot bit
  DECLARE @CurrentLogLSN numeric(25,0)
  DECLARE @CurrentLatestBackup datetime
  DECLARE @CurrentDatabaseNameFS nvarchar(max)
  DECLARE @CurrentDirectoryStructure nvarchar(max)
  DECLARE @CurrentDatabaseFileName nvarchar(max)
  DECLARE @CurrentMaxFilePathLength nvarchar(max)
  DECLARE @CurrentFileName nvarchar(max)
  DECLARE @CurrentDirectoryID int
  DECLARE @CurrentDirectoryPath nvarchar(4000) -- changed from max to 4k because used as parameter to xp_fileexist
  DECLARE @CurrentFilePath nvarchar(4000)      -- changed from max to 4k because used as parameter to xp_fileexist
  DECLARE @CurrentDate datetime
  DECLARE @CurrentCleanupDate datetime
  DECLARE @CurrentIsDatabaseAccessible bit
  DECLARE @CurrentAvailabilityGroup nvarchar(max)
  DECLARE @CurrentAvailabilityGroupRole nvarchar(max)
  DECLARE @CurrentAvailabilityGroupBackupPreference nvarchar(max)
  DECLARE @CurrentIsPreferredBackupReplica bit
  DECLARE @CurrentDatabaseMirroringRole nvarchar(max)
  DECLARE @CurrentLogShippingRole nvarchar(max)
  DECLARE @CurrentIsEncrypted bit
  DECLARE @CurrentIsReadOnly bit
  DECLARE @CurrentBackupSetID int
--DECLARE @CurrentIsMirror bit --  replaced by @CurrentMirror
  DECLARE @CurrentMirror SMALLINT -- added
  DECLARE @CurrentLastLogBackup datetime
  DECLARE @CurrentLogSizeSinceLastLogBackup float
  DECLARE @CurrentAllocatedExtentPageCount bigint
  DECLARE @CurrentModifiedExtentPageCount bigint

  DECLARE @CurrentCommand01 nvarchar(max)
  DECLARE @CurrentCommand02 nvarchar(max)
  DECLARE @CurrentCommand03 nvarchar(max)
  DECLARE @CurrentCommand04 nvarchar(max)
  DECLARE @CurrentCommand05 nvarchar(max)
  DECLARE @CurrentCommand06 nvarchar(max)
  DECLARE @CurrentCommand07 nvarchar(max)
  DECLARE @CurrentCommand08 nvarchar(max)
  DECLARE @CurrentCommand09 nvarchar(max)

  DECLARE @CurrentCommandOutput01 int
  DECLARE @CurrentCommandOutput02 int
  DECLARE @CurrentCommandOutput03 int
  DECLARE @CurrentCommandOutput04 int
  DECLARE @CurrentCommandOutput05 int
  DECLARE @CurrentCommandOutput08 int
  DECLARE @CurrentCommandOutput09 int

  DECLARE @CurrentCommandType01 nvarchar(max)
  DECLARE @CurrentCommandType02 nvarchar(max)
  DECLARE @CurrentCommandType03 nvarchar(max)
  DECLARE @CurrentCommandType04 nvarchar(max)
  DECLARE @CurrentCommandType05 nvarchar(max)
  DECLARE @CurrentCommandType08 nvarchar(max)
  DECLARE @CurrentCommandType09 nvarchar(max)
  DECLARE @CurrentCommandLogId int
  DECLARE @CommandLogIdAtStart     INT  -- the highest master.dbo.CommandLog.ID at the start (to prevent that pending copies that where created in the current session are copied to the mirrors)
  DECLARE @cmd                     NVARCHAR(4000); -- used for direct xp_cmd_shell call (to get a list of backup files)
  DECLARE @Template                NVARCHAR(4000);
  DECLARE @CurrentExtendedInfo     XML

  DECLARE @Directories TABLE (ID int PRIMARY KEY,
                              DirectoryPath nvarchar(max),
                              Mirror SMALLINT, -- changed from bit to SMALLINT
                              Completed BIT,
                              Available BIT DEFAULT 1)

  DECLARE @URLs TABLE (ID int PRIMARY KEY,
                       DirectoryPath nvarchar(max),
                       Mirror bit)

  DECLARE @DirectoryInfo TABLE (FileExists bit,
                                FileIsADirectory bit,
                                ParentDirectoryExists bit)

  DECLARE @tmpDatabases TABLE (ID int IDENTITY,
                               DatabaseName nvarchar(max),
                               DatabaseNameFS nvarchar(max),
                               DatabaseType nvarchar(max),
                               AvailabilityGroup bit,
                               StartPosition int,
                               DatabaseSize bigint,
                               LogSizeSinceLastLogBackup float,
                               [Order] int,
                               Selected bit,
                               Completed bit,
                               PRIMARY KEY(Selected, Completed, [Order], ID))

  DECLARE @tmpAvailabilityGroups TABLE (ID int IDENTITY PRIMARY KEY,
                                        AvailabilityGroupName nvarchar(max),
                                        StartPosition int,
                                        Selected bit)

  DECLARE @tmpDatabasesAvailabilityGroups TABLE (DatabaseName nvarchar(max),
                                                 AvailabilityGroupName nvarchar(max))

  DECLARE @SelectedDatabases TABLE (DatabaseName nvarchar(max),
                                    DatabaseType nvarchar(max),
                                    AvailabilityGroup nvarchar(max),
                                    StartPosition int,
                                    Selected bit)

  DECLARE @SelectedAvailabilityGroups TABLE (AvailabilityGroupName nvarchar(max),
                                             StartPosition int,
                                             Selected bit)

  DECLARE @CurrentBackupSet TABLE (ID int IDENTITY PRIMARY KEY,
                                   Mirror SMALLINT,
                                   VerifyCompleted bit,
                                   VerifyOutput int)

  DECLARE @CurrentDirectories TABLE (ID int PRIMARY KEY,
                                     DirectoryPath nvarchar(max),
                                     Mirror SMALLINT,
                                     DirectoryNumber int,
                                     CleanupDate datetime,
                                     CleanupMode nvarchar(max),
                                     CreateCompleted bit,
                                     CleanupCompleted bit,
                                     CreateOutput int,
                                     CleanupOutput int)

  DECLARE @CurrentURLs TABLE (ID int PRIMARY KEY,
                              DirectoryPath nvarchar(max),
                              Mirror bit,
                              DirectoryNumber int)

  DECLARE @CurrentFiles TABLE ([Type] nvarchar(max),
                               FilePath nvarchar(max),
                               Mirror SMALLINT,
                               FileNumber SMALLINT)

  DECLARE @CurrentCleanupDates TABLE (CleanupDate datetime, Mirror SMALLINT)
  
  -- added for faster Cleanup when @CleanupUsesCMD = 1
  -- must not be a table variable because of bad execution plan (no statistics) leading to poor performance
  CREATE TABLE #CleanUpFiles  (FileWithPath NVARCHAR(2048), 
                               CharBackupTime AS RIGHT(LEFT(FileWithPath, LEN(FileWithPath) - CHARINDEX('.', REVERSE(FileWithPath))  ), 15), 
                               ShouldBeDeleted TINYINT NOT NULL DEFAULT 0
                               );
  CREATE CLUSTERED INDEX #CleanUpFiles_ix ON #CleanUpFiles (CharBackupTime);  

  DECLARE @DirectoryCheck bit

  DECLARE @Error int
  DECLARE @ReturnCode int

  DECLARE @EmptyLine nvarchar(max)

  SET @Error = 0
  SET @ReturnCode = 0

  SET @EmptyLine = CHAR(9)

  SET @Version = CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)),CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - 1) + '.' + REPLACE(RIGHT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)), LEN(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)))),'.','') AS numeric(18,10))

  IF @Version >= 14
  BEGIN
    SELECT @HostPlatform = host_platform
    FROM sys.dm_os_host_info
  END
  ELSE
  BEGIN
    SET @HostPlatform = 'Windows'
  END

  SET @AmazonRDS = CASE WHEN DB_ID('rdsadmin') IS NOT NULL AND SUSER_SNAME(0x01) = 'rdsa' THEN 1 ELSE 0 END

  ----------------------------------------------------------------------------------------------------
  --// Log initial information                                                                    //--
  ----------------------------------------------------------------------------------------------------

  SET @StartTime = GETDATE()
  SET @SchemaName = (SELECT schemas.name FROM sys.schemas schemas INNER JOIN sys.objects objects ON schemas.[schema_id] = objects.[schema_id] WHERE [object_id] = @@PROCID)
  SET @ObjectName = OBJECT_NAME(@@PROCID)
  SET @VersionTimestamp = SUBSTRING(OBJECT_DEFINITION(@@PROCID),CHARINDEX('--// Version: ',OBJECT_DEFINITION(@@PROCID)) + LEN('--// Version: ') + 1, 19)

  SET @Parameters = '@Databases = ' + ISNULL('''' + REPLACE(@Databases,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @Directory = ' + ISNULL('''' + REPLACE(@Directory,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @BackupType = ' + ISNULL('''' + REPLACE(@BackupType,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @Verify = ' + ISNULL('''' + REPLACE(@Verify,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @CleanupTime = ' + ISNULL(CAST(@CleanupTime AS nvarchar),'NULL')
  SET @Parameters = @Parameters + ', @CleanupMode = ' + ISNULL('''' + REPLACE(@CleanupMode,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @CleanupUsesCMD = ' + ISNULL('''' + REPLACE(@CleanupUsesCMD,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @CleanUpStartTime = ' + ISNULL(CAST(@CleanUpStartTime AS NVARCHAR(20)),'NULL')
  SET @Parameters = @Parameters + ', @CleanUpEndTime = ' + ISNULL(CAST(@CleanUpEndTime AS NVARCHAR(20)),'NULL')    

  SET @Parameters = @Parameters + ', @Compress = ' + ISNULL('''' + REPLACE(@Compress,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @CopyOnly = ' + ISNULL('''' + REPLACE(@CopyOnly,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @ChangeBackupType = ' + ISNULL('''' + REPLACE(@ChangeBackupType,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @BackupSoftware = ' + ISNULL('''' + REPLACE(@BackupSoftware,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @CheckSum = ' + ISNULL('''' + REPLACE(@CheckSum,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @BlockSize = ' + ISNULL(CAST(@BlockSize AS nvarchar),'NULL')
  SET @Parameters = @Parameters + ', @BufferCount = ' + ISNULL(CAST(@BufferCount AS nvarchar),'NULL')
  SET @Parameters = @Parameters + ', @MaxTransferSize = ' + ISNULL(CAST(@MaxTransferSize AS nvarchar),'NULL')
  SET @Parameters = @Parameters + ', @NumberOfFiles = ' + ISNULL(CAST(@NumberOfFiles AS nvarchar),'NULL')
  SET @Parameters = @Parameters + ', @CompressionLevel = ' + ISNULL(CAST(@CompressionLevel AS nvarchar),'NULL')
  SET @Parameters = @Parameters + ', @Description = ' + ISNULL('''' + REPLACE(@Description,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @Threads = ' + ISNULL(CAST(@Threads AS nvarchar),'NULL')
  SET @Parameters = @Parameters + ', @Throttle = ' + ISNULL(CAST(@Throttle AS nvarchar),'NULL')
  SET @Parameters = @Parameters + ', @Encrypt = ' + ISNULL('''' + REPLACE(@Encrypt,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @EncryptionAlgorithm = ' + ISNULL('''' + REPLACE(@EncryptionAlgorithm,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @ServerCertificate = ' + ISNULL('''' + REPLACE(@ServerCertificate,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @ServerAsymmetricKey = ' + ISNULL('''' + REPLACE(@ServerAsymmetricKey,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @EncryptionKey = ' + ISNULL('''' + REPLACE(@EncryptionKey,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @ReadWriteFileGroups = ' + ISNULL('''' + REPLACE(@ReadWriteFileGroups,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @OverrideBackupPreference = ' + ISNULL('''' + REPLACE(@OverrideBackupPreference,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @NoRecovery = ' + ISNULL('''' + REPLACE(@NoRecovery,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @URL = ' + ISNULL('''' + REPLACE(@URL,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @Credential = ' + ISNULL('''' + REPLACE(@Credential,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @MirrorDirectory = ' + ISNULL('''' + REPLACE(@MirrorDirectory,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @MirrorCleanupTime = ' + ISNULL(CAST(@MirrorCleanupTime AS nvarchar),'NULL')
  SET @Parameters = @Parameters + ', @MirrorCleanupMode = ' + ISNULL('''' + REPLACE(@MirrorCleanupMode,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @MirrorURL = ' + ISNULL('''' + REPLACE(@MirrorURL,'''','''''') + '''','NULL')

  SET @Parameters = @Parameters + ', @MirrorWhenNotAvailable = ' + ISNULL('''' + REPLACE(@MirrorWhenNotAvailable,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @MirrorDirectory2 = ' + ISNULL('''' + REPLACE(@MirrorDirectory2,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @MirrorCleanupTime2 = ' + ISNULL(CAST(@MirrorCleanupTime2 AS nvarchar),'NULL')
  SET @Parameters = @Parameters + ', @MirrorCleanupMode2 = ' + ISNULL('''' + REPLACE(@MirrorCleanupMode2,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @MirrorWhenNotAvailable2 = ' + ISNULL('''' + REPLACE(@MirrorWhenNotAvailable2,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @MirrorDirectory3 = ' + ISNULL('''' + REPLACE(@MirrorDirectory3,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @MirrorCleanupTime3 = ' + ISNULL(CAST(@MirrorCleanupTime3 AS nvarchar),'NULL')
  SET @Parameters = @Parameters + ', @MirrorCleanupMode3 = ' + ISNULL('''' + REPLACE(@MirrorCleanupMode3,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @MirrorWhenNotAvailable3 = ' + ISNULL('''' + REPLACE(@MirrorWhenNotAvailable3,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @MirrorType = ' + ISNULL('''' + REPLACE(@MirrorType,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @SkipPendingMirrorCopies = ' + ISNULL('''' + REPLACE(@SkipPendingMirrorCopies,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @SkipPendingMirrorCopies = ' + ISNULL('''' + REPLACE(@SkipPendingMirrorCopies,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @MirrorCopyCommand = ' + ISNULL('''' + REPLACE(@MirrorCopyCommand,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @XCOPYFileLetter = ' + ISNULL('''' + REPLACE(@XCOPYFileLetter,'''','''''') + '''','NULL')

  SET @Parameters = @Parameters + ', @AvailabilityGroups = ' + ISNULL('''' + REPLACE(@AvailabilityGroups,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @Updateability = ' + ISNULL('''' + REPLACE(@Updateability,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @AdaptiveCompression = ' + ISNULL('''' + REPLACE(@AdaptiveCompression,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @ModificationLevel = ' + ISNULL(CAST(@ModificationLevel AS nvarchar),'NULL')
  SET @Parameters = @Parameters + ', @LogSizeSinceLastLogBackup = ' + ISNULL(CAST(@LogSizeSinceLastLogBackup AS nvarchar),'NULL')
  SET @Parameters = @Parameters + ', @TimeSinceLastLogBackup = ' + ISNULL(CAST(@TimeSinceLastLogBackup AS nvarchar),'NULL')
  SET @Parameters = @Parameters + ', @DataDomainBoostHost = ' + ISNULL('''' + REPLACE(@DataDomainBoostHost,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @DataDomainBoostUser = ' + ISNULL('''' + REPLACE(@DataDomainBoostUser,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @DataDomainBoostDevicePath = ' + ISNULL('''' + REPLACE(@DataDomainBoostDevicePath,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @DataDomainBoostLockboxPath = ' + ISNULL('''' + REPLACE(@DataDomainBoostLockboxPath,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @DirectoryStructure = ' + ISNULL('''' + REPLACE(@DirectoryStructure,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @AvailabilityGroupDirectoryStructure = ' + ISNULL('''' + REPLACE(@AvailabilityGroupDirectoryStructure,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @FileName = ' + ISNULL('''' + REPLACE(@FileName,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @AvailabilityGroupFileName = ' + ISNULL('''' + REPLACE(@AvailabilityGroupFileName,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @FileExtensionFull = ' + ISNULL('''' + REPLACE(@FileExtensionFull,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @FileExtensionDiff = ' + ISNULL('''' + REPLACE(@FileExtensionDiff,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @FileExtensionLog = ' + ISNULL('''' + REPLACE(@FileExtensionLog,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @Init = ' + ISNULL('''' + REPLACE(@Init,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @DatabaseOrder = ' + ISNULL('''' + REPLACE(@DatabaseOrder,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @DatabasesInParallel = ' + ISNULL('''' + REPLACE(@DatabasesInParallel,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @LogToTable = ' + ISNULL('''' + REPLACE(@LogToTable,'''','''''') + '''','NULL')
  SET @Parameters = @Parameters + ', @Execute = ' + ISNULL('''' + REPLACE(@Execute,'''','''''') + '''','NULL')

  -- block added - set @MirrorType to COPY when using native mirror backups on a non Enterprise Edition
  IF  (@MirrorDirectory IS NOT NULL OR @MirrorDirectory2 IS NOT NULL OR @MirrorDirectory3 IS NOT NULL)
  AND SERVERPROPERTY('EngineEdition') <> 3  -- no Enterprise / Dev / Evaluation edition
  AND @MirrorType NOT LIKE 'COPY%'
  AND @BackupSoftware IS NULL               -- all three external backup tools supports mirrored backups on any SQL Server edition
  BEGIN
      SET @MirrorType   = 'COPY'
      SET @Parameters = @Parameters + '@MirrorType changed to ''COPY'' because mirrored backups are an Enterprise only feature.' + CHAR(13) + CHAR(10)
  END

  SET @StartMessage = 'Date and time: ' + CONVERT(nvarchar,@StartTime,120)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Server: ' + CAST(SERVERPROPERTY('ServerName') AS nvarchar(max))
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Version: ' + CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Edition: ' + CAST(SERVERPROPERTY('Edition') AS nvarchar(max))
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Platform: ' + @HostPlatform
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Procedure: ' + QUOTENAME(DB_NAME(DB_ID())) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@ObjectName)
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Parameters: ' + @Parameters
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Version: ' + @VersionTimestamp
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  SET @StartMessage = 'Source: https://ola.hallengren.com'
  RAISERROR('%s',10,1,@StartMessage) WITH NOWAIT

  RAISERROR(@EmptyLine,10,1) WITH NOWAIT

  ----------------------------------------------------------------------------------------------------
  --// Check core requirements                                                                    //--
  ----------------------------------------------------------------------------------------------------

  IF NOT (SELECT [compatibility_level] FROM sys.databases WHERE database_id = DB_ID()) >= 90
  BEGIN
    SET @ErrorMessage = 'The database ' + QUOTENAME(DB_NAME(DB_ID())) + ' has to be in compatibility level 90 or higher.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF NOT (SELECT uses_ansi_nulls FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
  BEGIN
    SET @ErrorMessage = 'ANSI_NULLS has to be set to ON for the stored procedure.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF NOT (SELECT uses_quoted_identifier FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
  BEGIN
    SET @ErrorMessage = 'QUOTED_IDENTIFIER has to be set to ON for the stored procedure.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'P' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandExecute')
  BEGIN
    SET @ErrorMessage = 'The stored procedure CommandExecute is missing. Download https://ola.hallengren.com/scripts/CommandExecute.sql.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'P' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandExecute' AND OBJECT_DEFINITION(objects.[object_id]) NOT LIKE '%@LockMessageSeverity%')
  BEGIN
    SET @ErrorMessage = 'The stored procedure CommandExecute needs to be updated. Download https://ola.hallengren.com/scripts/CommandExecute.sql.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (   @LogToTable = 'Y' 
      OR 'COPY_LATER' IN (@MirrorWhenNotAvailable, @MirrorWhenNotAvailable2, @MirrorWhenNotAvailable3, @MirrorType)
      OR @BackupType = 'COPY_PENDING_MIRRORS'                                                                      
     )
  AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandLog')
  BEGIN
    SET @ErrorMessage = 'The table CommandLog is missing. Download https://ola.hallengren.com/scripts/CommandLog.sql.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @DatabasesInParallel = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'Queue')
  BEGIN
    SET @ErrorMessage = 'The table Queue is missing. Download https://ola.hallengren.com/scripts/Queue.sql.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @DatabasesInParallel = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'QueueDatabase')
  BEGIN
    SET @ErrorMessage = 'The table QueueDatabase is missing. Download https://ola.hallengren.com/scripts/QueueDatabase.sql.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @@TRANCOUNT <> 0
  BEGIN
    SET @ErrorMessage = 'The transaction count is not 0.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @AmazonRDS = 1
  BEGIN
    SET @ErrorMessage = 'The stored procedure DatabaseBackup is not supported on Amazon RDS.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @Error <> 0
  BEGIN
    SET @ReturnCode = @Error
    GOTO Logging
  END

  -- get initial CommandLog.ID
  IF (   'COPY_LATER' IN (@MirrorWhenNotAvailable, @MirrorWhenNotAvailable2, @MirrorWhenNotAvailable3, @MirrorType)
      OR @BackupType = 'COPY_PENDING_MIRRORS')
  AND OBJECT_ID('master.dbo.CommandLog') IS NOT NULL
     SET @CommandLogIdAtStart = ISNULL((SELECT MAX(id) FROM master.dbo.CommandLog AS cl), 0);

  ----------------------------------------------------------------------------------------------------
  --// Select databases                                                                           //--
  ----------------------------------------------------------------------------------------------------

  SET @Databases = REPLACE(@Databases, CHAR(10), '')
  SET @Databases = REPLACE(@Databases, CHAR(13), '')

  WHILE CHARINDEX(', ',@Databases) > 0 SET @Databases = REPLACE(@Databases,', ',',')
  WHILE CHARINDEX(' ,',@Databases) > 0 SET @Databases = REPLACE(@Databases,' ,',',')

  SET @Databases = LTRIM(RTRIM(@Databases));

  WITH Databases1 (StartPosition, EndPosition, DatabaseItem) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(',', @Databases, 1), 0), LEN(@Databases) + 1) AS EndPosition,
         SUBSTRING(@Databases, 1, ISNULL(NULLIF(CHARINDEX(',', @Databases, 1), 0), LEN(@Databases) + 1) - 1) AS DatabaseItem
  WHERE @Databases IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(',', @Databases, EndPosition + 1), 0), LEN(@Databases) + 1) AS EndPosition,
         SUBSTRING(@Databases, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(',', @Databases, EndPosition + 1), 0), LEN(@Databases) + 1) - EndPosition - 1) AS DatabaseItem
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

  IF @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1
  BEGIN
    INSERT INTO @tmpAvailabilityGroups (AvailabilityGroupName, Selected)
    SELECT name AS AvailabilityGroupName,
           0 AS Selected
    FROM sys.availability_groups

    INSERT INTO @tmpDatabasesAvailabilityGroups (DatabaseName, AvailabilityGroupName)
    SELECT availability_databases_cluster.database_name, availability_groups.name
    FROM sys.availability_databases_cluster availability_databases_cluster
    INNER JOIN sys.availability_groups availability_groups ON availability_databases_cluster.group_id = availability_groups.group_id
  END

  INSERT INTO @tmpDatabases (DatabaseName, DatabaseNameFS, DatabaseType, AvailabilityGroup, [Order], Selected, Completed)
  SELECT [name] AS DatabaseName,
         RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE([name],'\',''),'/',''),':',''),'*',''),'?',''),'"',''),'<',''),'>',''),'|','')) AS DatabaseNameFS,
         CASE WHEN name IN('master','msdb','model') THEN 'S' ELSE 'U' END AS DatabaseType,
         NULL AS AvailabilityGroup,
         0 AS [Order],
         0 AS Selected,
         0 AS Completed
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
  ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
  AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
  AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
  WHERE SelectedDatabases.Selected = 1

  UPDATE tmpDatabases
  SET tmpDatabases.Selected = SelectedDatabases.Selected
  FROM @tmpDatabases tmpDatabases
  INNER JOIN @SelectedDatabases SelectedDatabases
  ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
  AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
  AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
  WHERE SelectedDatabases.Selected = 0

  UPDATE tmpDatabases
  SET tmpDatabases.StartPosition = SelectedDatabases2.StartPosition
  FROM @tmpDatabases tmpDatabases
  INNER JOIN (SELECT tmpDatabases.DatabaseName, MIN(SelectedDatabases.StartPosition) AS StartPosition
              FROM @tmpDatabases tmpDatabases
              INNER JOIN @SelectedDatabases SelectedDatabases
              ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
              AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
              AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
              WHERE SelectedDatabases.Selected = 1
              GROUP BY tmpDatabases.DatabaseName) SelectedDatabases2
  ON tmpDatabases.DatabaseName = SelectedDatabases2.DatabaseName

  IF @Databases IS NOT NULL AND (NOT EXISTS(SELECT * FROM @SelectedDatabases) OR EXISTS(SELECT * FROM @SelectedDatabases WHERE DatabaseName IS NULL OR DatabaseName = ''))
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @Databases is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END
  ----------------------------------------------------------------------------------------------------
  --// Select availability groups                                                                 //--
  ----------------------------------------------------------------------------------------------------

  IF @AvailabilityGroups IS NOT NULL AND @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1
  BEGIN

    SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, CHAR(10), '')
    SET @AvailabilityGroups = REPLACE(@AvailabilityGroups, CHAR(13), '')

    WHILE CHARINDEX(', ',@AvailabilityGroups) > 0 SET @AvailabilityGroups = REPLACE(@AvailabilityGroups,', ',',')
    WHILE CHARINDEX(' ,',@AvailabilityGroups) > 0 SET @AvailabilityGroups = REPLACE(@AvailabilityGroups,' ,',',')

    SET @AvailabilityGroups = LTRIM(RTRIM(@AvailabilityGroups));

    WITH AvailabilityGroups1 (StartPosition, EndPosition, AvailabilityGroupItem) AS
    (
    SELECT 1 AS StartPosition,
           ISNULL(NULLIF(CHARINDEX(',', @AvailabilityGroups, 1), 0), LEN(@AvailabilityGroups) + 1) AS EndPosition,
           SUBSTRING(@AvailabilityGroups, 1, ISNULL(NULLIF(CHARINDEX(',', @AvailabilityGroups, 1), 0), LEN(@AvailabilityGroups) + 1) - 1) AS AvailabilityGroupItem
    WHERE @AvailabilityGroups IS NOT NULL
    UNION ALL
    SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
           ISNULL(NULLIF(CHARINDEX(',', @AvailabilityGroups, EndPosition + 1), 0), LEN(@AvailabilityGroups) + 1) AS EndPosition,
           SUBSTRING(@AvailabilityGroups, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(',', @AvailabilityGroups, EndPosition + 1), 0), LEN(@AvailabilityGroups) + 1) - EndPosition - 1) AS AvailabilityGroupItem
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
    ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'_','[_]')
    WHERE SelectedAvailabilityGroups.Selected = 1

    UPDATE tmpAvailabilityGroups
    SET tmpAvailabilityGroups.Selected = SelectedAvailabilityGroups.Selected
    FROM @tmpAvailabilityGroups tmpAvailabilityGroups
    INNER JOIN @SelectedAvailabilityGroups SelectedAvailabilityGroups
    ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'_','[_]')
    WHERE SelectedAvailabilityGroups.Selected = 0

    UPDATE tmpAvailabilityGroups
    SET tmpAvailabilityGroups.StartPosition = SelectedAvailabilityGroups2.StartPosition
    FROM @tmpAvailabilityGroups tmpAvailabilityGroups
    INNER JOIN (SELECT tmpAvailabilityGroups.AvailabilityGroupName, MIN(SelectedAvailabilityGroups.StartPosition) AS StartPosition
                FROM @tmpAvailabilityGroups tmpAvailabilityGroups
                INNER JOIN @SelectedAvailabilityGroups SelectedAvailabilityGroups
                ON tmpAvailabilityGroups.AvailabilityGroupName LIKE REPLACE(SelectedAvailabilityGroups.AvailabilityGroupName,'_','[_]')
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

  IF @AvailabilityGroups IS NOT NULL AND (NOT EXISTS(SELECT * FROM @SelectedAvailabilityGroups) OR EXISTS(SELECT * FROM @SelectedAvailabilityGroups WHERE AvailabilityGroupName IS NULL OR AvailabilityGroupName = '') OR @Version < 11 OR SERVERPROPERTY('IsHadrEnabled') = 0)
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @AvailabilityGroups is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (@Databases IS NULL AND @AvailabilityGroups IS NULL)
  BEGIN
    SET @ErrorMessage = 'You need to specify one of the parameters @Databases and @AvailabilityGroups.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (@Databases IS NOT NULL AND @AvailabilityGroups IS NOT NULL)
  BEGIN
    SET @ErrorMessage = 'You can only specify one of the parameters @Databases and @AvailabilityGroups.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  ----------------------------------------------------------------------------------------------------
  --// Check database names                                                                       //--
  ----------------------------------------------------------------------------------------------------

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + ', '
  FROM @tmpDatabases
  WHERE Selected = 1
  AND DatabaseNameFS = ''
  ORDER BY DatabaseName ASC
  IF @@ROWCOUNT > 0
  BEGIN
    SET @ErrorMessage = 'The names of the following databases are not supported: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + ', '
  FROM @tmpDatabases
  WHERE UPPER(DatabaseNameFS) IN(SELECT UPPER(DatabaseNameFS) FROM @tmpDatabases GROUP BY UPPER(DatabaseNameFS) HAVING COUNT(*) > 1)
  AND UPPER(DatabaseNameFS) IN(SELECT UPPER(DatabaseNameFS) FROM @tmpDatabases WHERE Selected = 1)
  AND DatabaseNameFS <> ''
  ORDER BY DatabaseName ASC
  OPTION (RECOMPILE)
  IF @@ROWCOUNT > 0
  BEGIN
    SET @ErrorMessage = 'The names of the following databases are not unique in the file system: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  ----------------------------------------------------------------------------------------------------
  --// Select directories                                                                         //--
  ----------------------------------------------------------------------------------------------------

  -- tfranz: Routine replaced to prevent 4 times nearly equal code (beside the directory parameter)
  --         the main directory (Default from Registry, when NULL) and up to three MirrorDirectories will now inserted with one statement
  --         
  IF @Directory IS NULL AND @URL IS NULL AND @HostPlatform = 'Windows' AND (@BackupSoftware <> 'DATA_DOMAIN_BOOST' OR @BackupSoftware IS NULL)
  BEGIN
     EXECUTE [master].dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'SOFTWARE\Microsoft\MSSQLServer\MSSQLServer', N'BackupDirectory', @DefaultDirectory OUTPUT;
  END

  IF @DefaultDirectory LIKE 'http://%' OR @DefaultDirectory LIKE 'https://%'
  BEGIN
    SET @URL = @DefaultDirectory
  END
  ;
  WITH WrkDirs AS (SELECT LTRIM(RTRIM(REPLACE(REPLACE(WrkDirectory,  ', ', ','), ' ,', ','))) AS WrkDirectory, -- remove "formating spaces"
                          MirrorNo
                     FROM (VALUES (CASE WHEN @Directory IS NULL AND @URL IS NULL AND @HostPlatform = 'Linux'
                                        THEN '.'
                                        WHEN @Directory IS NOT NULL
                                        THEN @Directory
                                        WHEN @DefaultDirectory IS NOT NULL AND @URL IS NULL -- @DefaultDirectory contains an URL
                                        THEN @DefaultDirectory
                                   END, 0),
                                  (@MirrorDirectory,  1), 
                                  (@MirrorDirectory2, 2), 
                                  (@MirrorDirectory3, 3)
                          ) m(WrkDirectory, MirrorNo)
                  ),
       Directories (StartPosition, EndPosition, Directory, WrkDirectory, MirrorNo) AS
     ( -- Split the directories, if backup goes to several target disks 
       SELECT 1 AS StartPosition,
              ISNULL(NULLIF(CHARINDEX(',', WrkDirectory, 1), 0), LEN(WrkDirectory) + 1) AS EndPosition,
              SUBSTRING(WrkDirectory, 1, ISNULL(NULLIF(CHARINDEX(',', WrkDirectory, 1), 0), LEN(WrkDirectory) + 1) - 1) AS Directory,
              WrkDirectory,
              MirrorNo
         FROM WrkDirs
        WHERE WrkDirectory IS NOT NULL
       UNION ALL
       SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
              ISNULL(NULLIF(CHARINDEX(',', WrkDirectory, EndPosition + 1), 0), LEN(WrkDirectory) + 1) AS EndPosition,
              SUBSTRING(WrkDirectory, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(',', WrkDirectory, EndPosition + 1), 0), LEN(WrkDirectory) + 1) - EndPosition - 1) AS Directory,
              WrkDirectory,
              MirrorNo
         FROM Directories
        WHERE EndPosition < LEN(WrkDirectory) + 1
     )
   INSERT INTO @Directories (ID, DirectoryPath, Mirror, Completed)
   SELECT ROW_NUMBER() OVER(ORDER BY StartPosition ASC) AS ID,
          Directory,
          MirrorNo,
          0
   FROM Directories
   OPTION (MAXRECURSION 0)
  ;

  ----------------------------------------------------------------------------------------------------
  --// Check directories                                                                          //--
  ----------------------------------------------------------------------------------------------------

  SET @DirectoryCheck = 1

  -- tfranz: redesigned for more specific error messages; use the same checks for all Directory parameters
  SET @ErrorMessage = NULL;
  SELECT @ErrorMessage =
         ISNULL(N'The value for the parameter '
               + STUFF((SELECT DISTINCT N'/' + CASE Mirror WHEN 0 THEN N'@Directory' WHEN 1 THEN N'@MirrorDirectory' WHEN 2 THEN N'@MirrorDirectory2' WHEN 3 THEN N'@MirrorDirectory3' END 
                          FROM @Directories 
                         WHERE (NOT (DirectoryPath LIKE N'_:' OR DirectoryPath LIKE N'_:\%' OR DirectoryPath LIKE N'\\%\%')) -- no valid path format (drive, local path or UNC path)
                            OR DirectoryPath LIKE N'__%:%'        -- colon (':') after the second character
                            OR NULLIF(LTRIM(DirectoryPath), N'') IS NULL -- empty path specified
                           FOR XML PATH('i'), root('c'), type
                       ).query('/c/i').value(N'.', 'nvarchar(4000)') , 1, 1, N'')  
               + N' is not supported.' + CHAR(13) + CHAR(10), N'')
       + CASE WHEN (SELECT COUNT(*) FROM (SELECT DISTINCT COUNT(*) num FROM @Directories AS d GROUP BY d.Mirror) sub) > 1
              THEN  N'The number of comma separated target directories in the parameters @Directory, @MirrorDirectory, @MirrorDirectory2 and @MirrorDirectory3 must be the same if the parameter is not NULL.' + CHAR(13) + CHAR(10)
              ELSE N''
         END
       + CASE WHEN EXISTS (SELECT * FROM @Directories GROUP BY DirectoryPath HAVING COUNT(*) <> 1)
              THEN N'The same directory was multiple times defined in @Directory / @MirrorDirectory / @MirrorDirectory2 / @MirrorDirectory3' + CHAR(13) + CHAR(10)
              ELSE N''
         END
       + CASE WHEN @BackupSoftware NOT IN('SQLBACKUP','SQLSAFE')
              THEN N'' -- just for performance (no SELECT if other software is used)
              WHEN @BackupSoftware IN('SQLBACKUP','SQLSAFE')
               AND (SELECT MAX(number) FROM (SELECT d2.Mirror, COUNT(*) AS number FROM @Directories AS d2 WHERE d2.Mirror > 0 GROUP BY d2.Mirror) AS sub) > 1
               AND @MirrorType     = 'DEFAULT'
              THEN N'Your backup software ' + @BackupSoftware + N' does not support Backup Mirroring to multiple target directories.' + CHAR(13) + CHAR(10) 
              ELSE N''
         END
       + CASE WHEN @BackupSoftware = 'SQLSAFE'
               AND @MirrorDirectory3 IS NOT NULL -- see http://community.idera.com/forums/topic/backup-mirroring/
               AND @MirrorType     = 'DEFAULT'
              THEN N'Idera SQL Safe supports only two mirror directories. Please remove the @MirrorDirectory3 parameter or set @MirrorType to ''COPY''.' + CHAR(13) + CHAR(10) 
              ELSE N''
         END
       + N' '
      -- removed: check for Enterprise Edition when mirroring is used, because the external backup tools supports mirroring in any SQL Editions
      -- and when it is not Enterprise it will copy the backup files manual to the mirror target(s)
  IF NULLIF(@ErrorMessage, N'') IS NOT NULL
  BEGIN
     RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
     SET @Error = @@ERROR
     RAISERROR(@EmptyLine,10,1) WITH NOWAIT
     SET @DirectoryCheck = 0
  END

  -- check for enabled xp_cmd used for Fake-Mirroring for native SQL Backups on non-Enterprise servers
  -- (by copying the backup files) or @CleanupUsesCMD
  IF (     @MirrorType LIKE 'COPY%'
      AND (@MirrorDirectory IS NOT NULL OR @MirrorDirectory2 IS NOT NULL OR @MirrorDirectory3 IS NOT NULL)
     )
   OR @CleanupUsesCMD = 'Y'
  BEGIN
       SET @ErrorMessage = NULL
       IF (SELECT CONVERT(BIT, value_in_use) FROM sys.configurations WHERE name = 'xp_cmdshell') = 0
       -- commented out, because - without SA - you usually have no select privilege on sys.credentials
       --OR (    IS_SRVROLEMEMBER('sysadmin') = 0
       --    AND NOT EXISTS (SELECT * FROM sys.credentials AS c WHERE name = '##xp_cmdshell_proxy_account##')
       --   )
       BEGIN
            IF @CleanupUsesCMD = 'Y'
               SET @ErrorMessage = 'To use the CMD-Mode for backup cleanups (@CleanupUsesCMD = ''Y'') the sys configuration xp_cmdshell has to been enabled.'
            ELSE 
               SET @ErrorMessage = '@MirrorType is set to ''COPY'' (manual or automatic, if a @MirrorDirectory is specified but the procedure runs on a non-Enterprise edition (without MIRROR TO-support) and not using an external @BackupSoftware). ' + CHAR(13) + CHAR(10)
                                 + 'To use the copy mirroring (file copy after backup) the sys configuration xp_cmdshell has to been enabled. '
            ;
            SET @ErrorMessage = @ErrorMessage + CHAR(13) + CHAR(10)
                              + 'When the backup runs from an account without sysadmin privilege a xp_cmdshell_proxy_account must be specified (using sp_xp_cmdshell_proxy_account) ' + CHAR(13) + CHAR(10)
                              + 'which needs read rights to the @Directory and write / delete rights to each @MirrorDirectory' + CHAR(13) + CHAR(10) + ' '
            RAISERROR(@ErrorMessage,16,1) WITH NOWAIT
            SET @Error = @@ERROR
            SET @DirectoryCheck = 0
       END

       IF @MirrorType = 'COPY_LATER'  
          UPDATE @Directories SET Mirror = Mirror * -1 WHERE Mirror > 0 -- negate mirror number -> the copy command will be pasted into the command log instead of being executed
  END
  
  
  IF @DirectoryCheck = 1
  BEGIN
    WHILE (1 = 1)
    BEGIN
      SELECT TOP 1 @CurrentRootDirectoryID = ID,
                   @CurrentRootDirectoryPath = DirectoryPath,
                   @CurrentMirror            = ABS(Mirror)
      FROM @Directories
      WHERE Completed = 0
      ORDER BY ID ASC

      IF @@ROWCOUNT = 0
      BEGIN
        BREAK
      END

      INSERT INTO @DirectoryInfo (FileExists, FileIsADirectory, ParentDirectoryExists)
      EXECUTE [master].dbo.xp_fileexist @CurrentRootDirectoryPath

      IF NOT EXISTS (SELECT * FROM @DirectoryInfo WHERE FileExists = 0 AND FileIsADirectory = 1 AND ParentDirectoryExists = 1)
      BEGIN
          IF (@CurrentMirror = 0) 
          OR (@CurrentMirror = 1 AND @MirrorWhenNotAvailable  = 'ERROR')
          OR (@CurrentMirror = 2 AND @MirrorWhenNotAvailable2 = 'ERROR')
          OR (@CurrentMirror = 3 AND @MirrorWhenNotAvailable3 = 'ERROR')
          BEGIN
              SET @ErrorMessage = 'The directory ' + @CurrentRootDirectoryPath + ' does not exist.' + CHAR(13) + CHAR(10) + ' '
              RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
              SET @Error = @@ERROR
              RAISERROR(@EmptyLine,10,1) WITH NOWAIT
          END

          -- delete the whole mirror, when one of the mirror directories (comma separated list) is not available
          IF (@CurrentMirror = 1 AND @MirrorWhenNotAvailable  = 'SKIP')
          OR (@CurrentMirror = 2 AND @MirrorWhenNotAvailable2 = 'SKIP')
          OR (@CurrentMirror = 3 AND @MirrorWhenNotAvailable3 = 'SKIP')
             DELETE FROM @Directories WHERE Mirror = @CurrentMirror

          -- Negate mirror number to prevent them from being used in the WHILE BETWEEN 1 AND 3-Loops
          IF (@CurrentMirror = 1 AND @MirrorWhenNotAvailable  = 'COPY_LATER')
          OR (@CurrentMirror = 2 AND @MirrorWhenNotAvailable2 = 'COPY_LATER')
          OR (@CurrentMirror = 3 AND @MirrorWhenNotAvailable3 = 'COPY_LATER')
             UPDATE @Directories SET Mirror = Mirror * -1, Available = 0 WHERE Mirror = @CurrentMirror AND Mirror > 0
      END
      UPDATE @Directories
      SET Completed = 1
      WHERE ID = @CurrentRootDirectoryID

      SET @CurrentRootDirectoryID = NULL
      SET @CurrentRootDirectoryPath = NULL

      DELETE FROM @DirectoryInfo
    END -- WHILE loop
  END

  ----------------------------------------------------------------------------------------------------
  --// Select URLs                                                                                //--
  ----------------------------------------------------------------------------------------------------

  SET @URL = REPLACE(@URL, CHAR(10), '')
  SET @URL = REPLACE(@URL, CHAR(13), '')

  WHILE CHARINDEX(', ',@URL) > 0 SET @URL = REPLACE(@URL,', ',',')
  WHILE CHARINDEX(' ,',@URL) > 0 SET @URL = REPLACE(@URL,' ,',',')

  SET @URL = LTRIM(RTRIM(@URL));

  WITH URLs (StartPosition, EndPosition, [URL]) AS
  (
  SELECT 1 AS StartPosition,
          ISNULL(NULLIF(CHARINDEX(',', @URL, 1), 0), LEN(@URL) + 1) AS EndPosition,
          SUBSTRING(@URL, 1, ISNULL(NULLIF(CHARINDEX(',', @URL, 1), 0), LEN(@URL) + 1) - 1) AS [URL]
  WHERE @URL IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
          ISNULL(NULLIF(CHARINDEX(',', @URL, EndPosition + 1), 0), LEN(@URL) + 1) AS EndPosition,
          SUBSTRING(@URL, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(',', @URL, EndPosition + 1), 0), LEN(@URL) + 1) - EndPosition - 1) AS [URL]
  FROM URLs
  WHERE EndPosition < LEN(@URL) + 1
  )
  INSERT INTO @URLs (ID, DirectoryPath, Mirror)
  SELECT ROW_NUMBER() OVER(ORDER BY StartPosition ASC) AS ID,
          [URL],
          0
  FROM URLs
  OPTION (MAXRECURSION 0)

  SET @MirrorURL = REPLACE(@MirrorURL, CHAR(10), '')
  SET @MirrorURL = REPLACE(@MirrorURL, CHAR(13), '')

  WHILE CHARINDEX(', ',@MirrorURL) > 0 SET @MirrorURL = REPLACE(@MirrorURL,', ',',')
  WHILE CHARINDEX(' ,',@MirrorURL) > 0 SET @MirrorURL = REPLACE(@MirrorURL,' ,',',')

  SET @MirrorURL = LTRIM(RTRIM(@MirrorURL));

  WITH URLs (StartPosition, EndPosition, [URL]) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(',', @MirrorURL, 1), 0), LEN(@MirrorURL) + 1) AS EndPosition,
         SUBSTRING(@MirrorURL, 1, ISNULL(NULLIF(CHARINDEX(',', @MirrorURL, 1), 0), LEN(@MirrorURL) + 1) - 1) AS [URL]
  WHERE @MirrorURL IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(',', @MirrorURL, EndPosition + 1), 0), LEN(@MirrorURL) + 1) AS EndPosition,
         SUBSTRING(@MirrorURL, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(',', @MirrorURL, EndPosition + 1), 0), LEN(@MirrorURL) + 1) - EndPosition - 1) AS [URL]
  FROM URLs
  WHERE EndPosition < LEN(@MirrorURL) + 1
  )
  INSERT INTO @URLs (ID, DirectoryPath, Mirror)
  SELECT (SELECT COUNT(*) FROM @URLs) + ROW_NUMBER() OVER(ORDER BY StartPosition ASC) AS ID,
         [URL],
         1
  FROM URLs
  OPTION (MAXRECURSION 0)

  ----------------------------------------------------------------------------------------------------
  --// Check URLs                                                                          //--
  ----------------------------------------------------------------------------------------------------

  IF EXISTS(SELECT * FROM @URLs WHERE Mirror = 0 AND DirectoryPath NOT LIKE 'https://%/%')
  OR EXISTS (SELECT * FROM @URLs GROUP BY DirectoryPath HAVING COUNT(*) <> 1)
  OR ((SELECT COUNT(*) FROM @URLs WHERE Mirror = 0) <> (SELECT COUNT(*) FROM @URLs WHERE Mirror = 1) AND (SELECT COUNT(*) FROM @URLs WHERE Mirror = 1) > 0)
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @URL is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF EXISTS(SELECT * FROM @URLs WHERE Mirror = 1 AND DirectoryPath NOT LIKE 'https://%/%')
  OR EXISTS (SELECT * FROM @URLs GROUP BY DirectoryPath HAVING COUNT(*) <> 1)
  OR ((SELECT COUNT(*) FROM @URLs WHERE Mirror = 0) <> (SELECT COUNT(*) FROM @URLs WHERE Mirror = 1) AND (SELECT COUNT(*) FROM @URLs WHERE Mirror = 1) > 0)
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @MirrorURL is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  ----------------------------------------------------------------------------------------------------
  --// Get directory separator                                                                   //--
  ----------------------------------------------------------------------------------------------------

  SELECT @DirectorySeparator = CASE
  WHEN @URL IS NOT NULL THEN '/'
  WHEN @HostPlatform = 'Windows' THEN '\'
  WHEN @HostPlatform = 'Linux' THEN '/'
  END

  UPDATE @Directories
  SET DirectoryPath = LEFT(DirectoryPath,LEN(DirectoryPath) - 1)
  WHERE RIGHT(DirectoryPath,1) = @DirectorySeparator

  UPDATE @URLs
  SET DirectoryPath = LEFT(DirectoryPath,LEN(DirectoryPath) - 1)
  WHERE RIGHT(DirectoryPath,1) = @DirectorySeparator

  ----------------------------------------------------------------------------------------------------
  --// Get file extension                                                                         //--
  ----------------------------------------------------------------------------------------------------

  IF @FileExtensionFull IS NULL
  BEGIN
    SELECT @FileExtensionFull = CASE
    WHEN @BackupSoftware IS NULL THEN 'bak'
    WHEN @BackupSoftware = 'LITESPEED' THEN 'bak'
    WHEN @BackupSoftware = 'SQLBACKUP' THEN 'sqb'
    WHEN @BackupSoftware = 'SQLSAFE' THEN 'safe'
    END
  END

  IF @FileExtensionDiff IS NULL
  BEGIN
    SELECT @FileExtensionDiff = CASE
    WHEN @BackupSoftware IS NULL THEN 'bak'
    WHEN @BackupSoftware = 'LITESPEED' THEN 'bak'
    WHEN @BackupSoftware = 'SQLBACKUP' THEN 'sqb'
    WHEN @BackupSoftware = 'SQLSAFE' THEN 'safe'
    END
  END

  IF @FileExtensionLog IS NULL
  BEGIN
    SELECT @FileExtensionLog = CASE
    WHEN @BackupSoftware IS NULL THEN 'trn'
    WHEN @BackupSoftware = 'LITESPEED' THEN 'trn'
    WHEN @BackupSoftware = 'SQLBACKUP' THEN 'sqb'
    WHEN @BackupSoftware = 'SQLSAFE' THEN 'safe'
    END
  END

  ----------------------------------------------------------------------------------------------------
  --// Get default compression                                                                    //--
  ----------------------------------------------------------------------------------------------------

  IF @Compress IS NULL
  BEGIN
    SELECT @Compress = CASE
    WHEN @BackupSoftware IS NULL AND EXISTS(SELECT * FROM sys.configurations WHERE name = 'backup compression default' AND value_in_use = 1) THEN 'Y'
    WHEN @BackupSoftware IS NULL AND NOT EXISTS(SELECT * FROM sys.configurations WHERE name = 'backup compression default' AND value_in_use = 1) THEN 'N'
    WHEN @BackupSoftware IS NOT NULL AND (@CompressionLevel IS NULL OR @CompressionLevel > 0)  THEN 'Y'
    WHEN @BackupSoftware IS NOT NULL AND @CompressionLevel = 0  THEN 'N'
    END
  END

  ----------------------------------------------------------------------------------------------------
  --// Get number of files                                                                        //--
  ----------------------------------------------------------------------------------------------------

  IF @NumberOfFiles IS NULL
  BEGIN
    SELECT @NumberOfFiles = CASE
    WHEN @BackupSoftware = 'DATA_DOMAIN_BOOST' THEN 1
    WHEN @URL IS NOT NULL THEN (SELECT COUNT(*) FROM @URLs WHERE Mirror = 0)
    ELSE (SELECT COUNT(*) FROM @Directories WHERE Mirror = 0)
    END
  END

  ----------------------------------------------------------------------------------------------------
  --// Check input parameters                                                                     //--
  ----------------------------------------------------------------------------------------------------

  IF @BackupType NOT IN ('FULL','DIFF','LOG', 'COPY_PENDING_MIRRORS') OR @BackupType IS NULL
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @BackupType is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @MirrorType NOT IN ('DEFAULT', 'COPY', 'COPY_LATER') OR @MirrorType IS NULL
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @MirrorType is not supported.' + CHAR(13) + CHAR(10) + ' '
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF SERVERPROPERTY('EngineEdition') = 8 AND NOT (@BackupType = 'FULL' AND @CopyOnly = 'Y')
  BEGIN
    SET @ErrorMessage = 'SQL Database Managed Instance only supports COPY_ONLY full backups.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @Verify NOT IN ('Y','N', 'SKIP_MIRRORS') OR @Verify IS NULL OR (@BackupSoftware = 'SQLSAFE' AND @Encrypt = 'Y' AND @Verify = 'Y') OR (@Verify = 'Y' AND @BackupSoftware = 'DATA_DOMAIN_BOOST')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @Verify is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @CleanupTime < 0
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @CleanupTime is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @CleanupTime IS NOT NULL AND @URL IS NOT NULL
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @CleanupTime is not supported. Cleanup is not supported on Azure Blob Storage.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @CleanupTime IS NOT NULL AND @HostPlatform = 'Linux'
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @CleanupTime is not supported. Cleanup is not supported on Linux.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @CleanupTime IS NOT NULL AND ((@DirectoryStructure NOT LIKE '%{DatabaseName}%' OR @DirectoryStructure IS NULL) OR (SERVERPROPERTY('IsHadrEnabled') = 1 AND (@AvailabilityGroupDirectoryStructure NOT LIKE '%{DatabaseName}%' OR @AvailabilityGroupDirectoryStructure IS NULL)))
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @CleanupTime is not supported. Cleanup is not supported if the token {DatabaseName} is not part of the directory.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @CleanupTime IS NOT NULL AND ((@DirectoryStructure NOT LIKE '%{BackupType}%' OR @DirectoryStructure IS NULL) OR (SERVERPROPERTY('IsHadrEnabled') = 1 AND (@AvailabilityGroupDirectoryStructure NOT LIKE '%{BackupType}%' OR @AvailabilityGroupDirectoryStructure IS NULL)))
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @CleanupTime is not supported. Cleanup is not supported if the token {BackupType} is not part of the directory.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @CleanupTime IS NOT NULL AND @CopyOnly = 'Y' AND ((@DirectoryStructure NOT LIKE '%{CopyOnly}%' OR @DirectoryStructure IS NULL) OR (SERVERPROPERTY('IsHadrEnabled') = 1 AND (@AvailabilityGroupDirectoryStructure NOT LIKE '%{CopyOnly}%' OR @AvailabilityGroupDirectoryStructure IS NULL)))
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @CleanupTime is not supported. Cleanup is not supported if the token {CopyOnly} is not part of the directory.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @CleanupMode NOT IN('BEFORE_BACKUP','AFTER_BACKUP') OR @CleanupMode IS NULL
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @CleanupMode is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @CleanupUsesCMD NOT IN ('Y','N') OR @CleanupUsesCMD IS NULL 
  OR @HostPlatform = 'Linux' 
  OR @BackupSoftware IS NOT NULL
  OR @FileName NOT LIKE '%{Year}{Month}{Day}[_]{Hour}{Minute}{Second}[_]{FileNumber}.{FileExtension}'
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @CleanupUsesCMD is not supported.' + CHAR(13) + CHAR(10) + ' '
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @Compress NOT IN ('Y','N') OR @Compress IS NULL OR (@Compress = 'Y' AND @BackupSoftware IS NULL AND NOT ((@Version >= 10 AND @Version < 10.5 AND SERVERPROPERTY('EngineEdition') = 3) OR (@Version >= 10.5 AND (SERVERPROPERTY('EngineEdition') IN (3, 8) OR SERVERPROPERTY('EditionID') IN (-1534726760, 284895786))))) OR (@Compress = 'N' AND @BackupSoftware IN ('LITESPEED','SQLBACKUP','SQLSAFE') AND (@CompressionLevel IS NULL OR @CompressionLevel >= 1)) OR (@Compress = 'Y' AND @BackupSoftware IN ('LITESPEED','SQLBACKUP','SQLSAFE') AND @CompressionLevel = 0)
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @Compress is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @CopyOnly NOT IN ('Y','N') OR @CopyOnly IS NULL
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @CopyOnly is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @ChangeBackupType NOT IN ('Y','N') OR @ChangeBackupType IS NULL
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @ChangeBackupType is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @BackupSoftware NOT IN ('LITESPEED','SQLBACKUP','SQLSAFE','DATA_DOMAIN_BOOST') OR (@BackupSoftware IS NOT NULL AND @HostPlatform = 'Linux')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @BackupSoftware is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @BackupSoftware = 'LITESPEED' AND NOT EXISTS (SELECT * FROM [master].sys.objects WHERE [type] = 'X' AND [name] = 'xp_backup_database')
  BEGIN
    SET @ErrorMessage = 'LiteSpeed for SQL Server is not installed. Download https://www.quest.com/products/litespeed-for-sql-server/.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @BackupSoftware = 'SQLBACKUP' AND NOT EXISTS (SELECT * FROM [master].sys.objects WHERE [type] = 'X' AND [name] = 'sqlbackup')
  BEGIN
    SET @ErrorMessage = 'Red Gate SQL Backup Pro is not installed. Download https://www.red-gate.com/products/dba/sql-backup/.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @BackupSoftware = 'SQLSAFE' AND NOT EXISTS (SELECT * FROM [master].sys.objects WHERE [type] = 'X' AND [name] = 'xp_ss_backup')
  BEGIN
    SET @ErrorMessage = 'Idera SQL Safe Backup is not installed. Download https://www.idera.com/productssolutions/sqlserver/sqlsafebackup.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @BackupSoftware = 'DATA_DOMAIN_BOOST' AND NOT EXISTS (SELECT * FROM [master].sys.objects WHERE [type] = 'PC' AND [name] = 'emc_run_backup')
  BEGIN
    SET @ErrorMessage = 'EMC Data Domain Boost is not installed. Download https://www.emc.com/en-us/data-protection/data-domain.htm.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @CheckSum NOT IN ('Y','N') OR @CheckSum IS NULL
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @CheckSum is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @BlockSize NOT IN (512,1024,2048,4096,8192,16384,32768,65536) OR (@BlockSize IS NOT NULL AND @BackupSoftware = 'SQLBACKUP') OR (@BlockSize IS NOT NULL AND @BackupSoftware = 'SQLSAFE') OR (@BlockSize IS NOT NULL AND @URL IS NOT NULL AND @Credential IS NOT NULL) OR (@BlockSize IS NOT NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @BlockSize is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @BufferCount <= 0 OR @BufferCount > 2147483647 OR (@BufferCount IS NOT NULL AND @BackupSoftware = 'SQLBACKUP') OR (@BufferCount IS NOT NULL AND @BackupSoftware = 'SQLSAFE')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @BufferCount is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @MaxTransferSize < 65536 OR @MaxTransferSize > 4194304 OR (@MaxTransferSize > 1048576 AND @BackupSoftware = 'SQLBACKUP') OR (@MaxTransferSize IS NOT NULL AND @BackupSoftware = 'SQLSAFE') OR (@MaxTransferSize IS NOT NULL AND @URL IS NOT NULL AND @Credential IS NOT NULL) OR (@MaxTransferSize IS NOT NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @MaxTransferSize is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @NumberOfFiles < 1 OR @NumberOfFiles > 64 OR (@NumberOfFiles > 32 AND @BackupSoftware = 'SQLBACKUP') OR @NumberOfFiles IS NULL OR @NumberOfFiles < (SELECT COUNT(*) FROM @Directories WHERE Mirror = 0) OR @NumberOfFiles % (SELECT NULLIF(COUNT(*),0) FROM @Directories WHERE Mirror = 0) > 0 OR (@URL IS NOT NULL AND @Credential IS NOT NULL AND @NumberOfFiles <> 1) OR (@NumberOfFiles > 1 AND @BackupSoftware IN('SQLBACKUP','SQLSAFE') AND EXISTS(SELECT * FROM @Directories WHERE Mirror = 1)) OR (@NumberOfFiles > 32 AND @BackupSoftware = 'DATA_DOMAIN_BOOST') OR @NumberOfFiles < (SELECT COUNT(*) FROM @URLs WHERE Mirror = 0) OR @NumberOfFiles % (SELECT NULLIF(COUNT(*),0) FROM @URLs WHERE Mirror = 0) > 0
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @NumberOfFiles is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (@BackupSoftware IS NULL AND @CompressionLevel IS NOT NULL) OR (@BackupSoftware = 'LITESPEED' AND (@CompressionLevel < 0 OR @CompressionLevel > 8)) OR (@BackupSoftware = 'SQLBACKUP' AND (@CompressionLevel < 0 OR @CompressionLevel > 4)) OR (@BackupSoftware = 'SQLSAFE' AND (@CompressionLevel < 1 OR @CompressionLevel > 4)) OR (@CompressionLevel IS NOT NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @CompressionLevel is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF LEN(@Description) > 255 OR (@BackupSoftware = 'LITESPEED' AND LEN(@Description) > 128) OR (@BackupSoftware = 'DATA_DOMAIN_BOOST' AND LEN(@Description) > 254)
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @Description is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @Threads IS NOT NULL AND (@BackupSoftware NOT IN('LITESPEED','SQLBACKUP','SQLSAFE') OR @BackupSoftware IS NULL) OR (@BackupSoftware = 'LITESPEED' AND (@Threads < 1 OR @Threads > 32)) OR (@BackupSoftware = 'SQLBACKUP' AND (@Threads < 2 OR @Threads > 32)) OR (@BackupSoftware = 'SQLSAFE' AND (@Threads < 1 OR @Threads > 64))
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @Threads is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @Throttle IS NOT NULL AND (@BackupSoftware NOT IN('LITESPEED') OR @BackupSoftware IS NULL) OR @Throttle < 1 OR @Throttle > 100
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @Throttle is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @Encrypt NOT IN('Y','N') OR @Encrypt IS NULL OR (@Encrypt = 'Y' AND @BackupSoftware IS NULL AND NOT (@Version >= 12 AND (SERVERPROPERTY('EngineEdition') = 3) OR SERVERPROPERTY('EditionID') IN(-1534726760, 284895786))) OR (@Encrypt = 'Y' AND @BackupSoftware = 'DATA_DOMAIN_BOOST')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @Encrypt is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (@BackupSoftware IS NULL AND @Encrypt = 'Y' AND (@EncryptionAlgorithm NOT IN('AES_128','AES_192','AES_256','TRIPLE_DES_3KEY') OR @EncryptionAlgorithm IS NULL)) OR (@BackupSoftware = 'LITESPEED' AND @Encrypt = 'Y' AND (@EncryptionAlgorithm NOT IN('RC2_40','RC2_56','RC2_112','RC2_128','TRIPLE_DES_3KEY','RC4_128','AES_128','AES_192','AES_256') OR @EncryptionAlgorithm IS NULL)) OR (@BackupSoftware = 'SQLBACKUP' AND @Encrypt = 'Y' AND (@EncryptionAlgorithm NOT IN('AES_128','AES_256') OR @EncryptionAlgorithm IS NULL)) OR (@BackupSoftware = 'SQLSAFE' AND @Encrypt = 'Y' AND (@EncryptionAlgorithm NOT IN('AES_128','AES_256') OR @EncryptionAlgorithm IS NULL)) OR (@EncryptionAlgorithm IS NOT NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @EncryptionAlgorithm is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (NOT (@BackupSoftware IS NULL AND @Encrypt = 'Y') AND @ServerCertificate IS NOT NULL) OR (@BackupSoftware IS NULL AND @Encrypt = 'Y' AND @ServerCertificate IS NULL AND @ServerAsymmetricKey IS NULL) OR (@BackupSoftware IS NULL AND @Encrypt = 'Y' AND @ServerCertificate IS NOT NULL AND @ServerAsymmetricKey IS NOT NULL) OR (@ServerCertificate IS NOT NULL AND NOT EXISTS(SELECT * FROM master.sys.certificates WHERE name = @ServerCertificate))
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @ServerCertificate is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (NOT (@BackupSoftware IS NULL AND @Encrypt = 'Y') AND @ServerAsymmetricKey IS NOT NULL) OR (@BackupSoftware IS NULL AND @Encrypt = 'Y' AND @ServerAsymmetricKey IS NULL AND @ServerCertificate IS NULL) OR (@BackupSoftware IS NULL AND @Encrypt = 'Y' AND @ServerAsymmetricKey IS NOT NULL AND @ServerCertificate IS NOT NULL) OR (@ServerAsymmetricKey IS NOT NULL AND NOT EXISTS(SELECT * FROM master.sys.asymmetric_keys WHERE name = @ServerAsymmetricKey))
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @ServerAsymmetricKey is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (@EncryptionKey IS NOT NULL AND @BackupSoftware IS NULL) OR (@EncryptionKey IS NOT NULL AND @Encrypt = 'N') OR (@EncryptionKey IS NULL AND @Encrypt = 'Y' AND @BackupSoftware IN('LITESPEED','SQLBACKUP','SQLSAFE')) OR (@EncryptionKey IS NOT NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @EncryptionKey is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @ReadWriteFileGroups NOT IN('Y','N') OR @ReadWriteFileGroups IS NULL OR (@ReadWriteFileGroups = 'Y' AND @BackupType = 'LOG')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @ReadWriteFileGroups is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @OverrideBackupPreference NOT IN('Y','N') OR @OverrideBackupPreference IS NULL
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @OverrideBackupPreference is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @NoRecovery NOT IN('Y','N') OR @NoRecovery IS NULL OR (@NoRecovery = 'Y' AND @BackupType <> 'LOG') OR (@NoRecovery = 'Y' AND @BackupSoftware = 'SQLSAFE')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @NoRecovery is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (@URL IS NOT NULL AND @Directory IS NOT NULL) OR (@URL IS NOT NULL AND @MirrorDirectory IS NOT NULL) OR (@URL IS NOT NULL AND @Version < 11.03339) OR (@URL IS NOT NULL AND @BackupSoftware IS NOT NULL)
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @URL is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (@Credential IS NULL AND @URL IS NOT NULL AND NOT (@Version >= 13 OR SERVERPROPERTY('EngineEdition') = 8)) OR (@Credential IS NOT NULL AND @URL IS NULL) OR (@URL IS NOT NULL AND @Credential IS NULL AND NOT EXISTS(SELECT * FROM sys.credentials WHERE UPPER(credential_identity) = 'SHARED ACCESS SIGNATURE')) OR (@Credential IS NOT NULL AND NOT EXISTS(SELECT * FROM sys.credentials WHERE name = @Credential))
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @Credential is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  -- unified for all @MirrorCleanup... parameters
  SET @ErrorMessage = NULL
  SELECT @ErrorMessage = 'The value for the parameter(s) '
                       + (SELECT STUFF((SELECT '/' + val 
                                          FROM (          SELECT CASE WHEN @MirrorCleanupTime   < 0 OR (@MirrorCleanupTime  IS NOT NULL     AND @MirrorDirectory         IS NULL) THEN '@MirrorCleanupTime'       END val
                                                UNION ALL SELECT CASE WHEN @MirrorCleanupTime2  < 0 OR (@MirrorCleanupTime2 IS NOT NULL     AND @MirrorDirectory2        IS NULL) THEN '@MirrorCleanupTime2'      END val
                                                UNION ALL SELECT CASE WHEN @MirrorCleanupTime3  < 0 OR (@MirrorCleanupTime3 IS NOT NULL     AND @MirrorDirectory3        IS NULL) THEN '@MirrorCleanupTime3'      END val
                                                UNION ALL SELECT CASE WHEN @MirrorCleanupMode       NOT IN('BEFORE_BACKUP','AFTER_BACKUP')   OR @MirrorCleanupMode       IS NULL  THEN '@MirrorCleanupMode'       END val
                                                UNION ALL SELECT CASE WHEN @MirrorCleanupMode2      NOT IN('BEFORE_BACKUP','AFTER_BACKUP')   OR @MirrorCleanupMode2      IS NULL  THEN '@MirrorCleanupMode2'      END val
                                                UNION ALL SELECT CASE WHEN @MirrorCleanupMode3      NOT IN('BEFORE_BACKUP','AFTER_BACKUP')   OR @MirrorCleanupMode3      IS NULL  THEN '@MirrorCleanupMode3'      END val
                                                UNION ALL SELECT CASE WHEN @MirrorWhenNotAvailable  NOT IN('ERROR','SKIP','COPY_LATER')      OR @MirrorWhenNotAvailable  IS NULL  THEN '@MirrorWhenNotAvailable'  END val
                                                UNION ALL SELECT CASE WHEN @MirrorWhenNotAvailable2 NOT IN('ERROR','SKIP','COPY_LATER')      OR @MirrorWhenNotAvailable2 IS NULL  THEN '@MirrorWhenNotAvailable2' END val
                                                UNION ALL SELECT CASE WHEN @MirrorWhenNotAvailable3 NOT IN('ERROR','SKIP','COPY_LATER')      OR @MirrorWhenNotAvailable3 IS NULL  THEN '@MirrorWhenNotAvailable3' END val
                                               ) sub
                                         WHERE sub.val IS NOT NULL
                                           FOR XML PATH('i'), root('c'), type).query('/c/i').value('.', 'nvarchar(4000)'), 1, 1, '' )
                          ) + ' is not supported.' + CHAR(13) + CHAR(10) + ' '
  IF NULLIF(@ErrorMessage, '') IS NOT NULL
  BEGIN
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

 
  IF @MirrorCopyCommand IS NULL SET @MirrorCopyCommand = 'COPY'
  IF @MirrorCopyCommand NOT IN ('COPY', 'XCOPY', 'ROBOCOPY')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @MirrorCopyCommand is not supported.' + CHAR(13) + CHAR(10) + ' '
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

 
  IF @MirrorCopyCommand = 'XCOPY' 
  AND (@MirrorType LIKE 'COPY%' OR 'COPY_LATER' IN (@MirrorWhenNotAvailable, @MirrorWhenNotAvailable2, @MirrorWhenNotAvailable3))
  AND (@MirrorDirectory IS NOT NULL OR @MirrorDirectory2 IS NOT NULL OR @MirrorDirectory3 IS NOT NULL)
  AND ISNULL(LTRIM(@XCOPYFileLetter), '') = ''
  BEGIN
    SET @ErrorMessage = '@XCOPYFileLetter must not be empty when @MirrorCopyCommand is set to ''XCOPY''' + CHAR(13) + CHAR(10) + ' '
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

 
  IF @SkipPendingMirrorCopies NOT IN('Y','N') OR @SkipPendingMirrorCopies IS NULL
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @SkipPendingMirrorCopies is not supported.' + CHAR(13) + CHAR(10) + ' '
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (@MirrorURL IS NOT NULL AND @Directory IS NOT NULL) OR (@MirrorURL IS NOT NULL AND @MirrorDirectory IS NOT NULL) OR (@MirrorURL IS NOT NULL AND @Version < 11.03339) OR (@MirrorURL IS NOT NULL AND @BackupSoftware IS NOT NULL) OR (@MirrorURL IS NOT NULL AND @URL IS NULL)
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @MirrorURL is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @Updateability NOT IN('READ_ONLY','READ_WRITE','ALL') OR @Updateability IS NULL
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @Updateability is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @AdaptiveCompression NOT IN('SIZE','SPEED') OR (@AdaptiveCompression IS NOT NULL AND (@BackupSoftware NOT IN('LITESPEED') OR @BackupSoftware IS NULL))
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @AdaptiveCompression is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (@ModificationLevel IS NOT NULL AND NOT ((@Version >= 12.06024 AND @Version < 13) OR @Version >= 13.05026)) OR (@ModificationLevel IS NOT NULL AND @ChangeBackupType = 'N') OR (@ModificationLevel IS NOT NULL AND @BackupType <> 'DIFF')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @ModificationLevel is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (@LogSizeSinceLastLogBackup IS NOT NULL AND NOT ((@Version >= 12.06024 AND @Version < 13) OR @Version >= 13.05026)) OR (@LogSizeSinceLastLogBackup IS NOT NULL AND @TimeSinceLastLogBackup IS NULL) OR (@LogSizeSinceLastLogBackup IS NULL AND @TimeSinceLastLogBackup IS NOT NULL) OR (@LogSizeSinceLastLogBackup IS NOT NULL AND @BackupType <> 'LOG')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @LogSizeSinceLastLogBackup is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (@TimeSinceLastLogBackup IS NOT NULL AND NOT ((@Version >= 12.06024 AND @Version < 13) OR @Version >= 13.05026)) OR (@TimeSinceLastLogBackup IS NOT NULL AND @LogSizeSinceLastLogBackup IS NULL) OR (@TimeSinceLastLogBackup IS NULL AND @LogSizeSinceLastLogBackup IS NOT NULL) OR (@TimeSinceLastLogBackup IS NOT NULL AND @BackupType <> 'LOG')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @TimeSinceLastLogBackup is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (@DataDomainBoostHost IS NOT NULL AND (@BackupSoftware <> 'DATA_DOMAIN_BOOST' OR @BackupSoftware IS NULL)) OR (@DataDomainBoostHost IS NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @DataDomainBoostHost is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (@DataDomainBoostUser IS NOT NULL AND (@BackupSoftware <> 'DATA_DOMAIN_BOOST' OR @BackupSoftware IS NULL)) OR (@DataDomainBoostUser IS NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @DataDomainBoostUser is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (@DataDomainBoostDevicePath IS NOT NULL AND (@BackupSoftware <> 'DATA_DOMAIN_BOOST' OR @BackupSoftware IS NULL)) OR (@DataDomainBoostDevicePath IS NULL AND @BackupSoftware = 'DATA_DOMAIN_BOOST')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @DataDomainBoostDevicePath is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @DataDomainBoostLockboxPath IS NOT NULL AND (@BackupSoftware <> 'DATA_DOMAIN_BOOST' OR @BackupSoftware IS NULL)
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @DataDomainBoostLockboxPath is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @DirectoryStructure = ''
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @DirectoryStructure is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @AvailabilityGroupDirectoryStructure = ''
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @AvailabilityGroupDirectoryStructure is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @FileName IS NULL OR @FileName = '' OR @FileName NOT LIKE '%.{FileExtension}' OR (@NumberOfFiles > 1 AND @FileName NOT LIKE '%{FileNumber}%') OR @FileName LIKE '%{DirectorySeperator}%' OR @FileName LIKE '%/%' OR @FileName LIKE '%\%'
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @FileName is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF (SERVERPROPERTY('IsHadrEnabled') = 1 AND @AvailabilityGroupFileName IS NULL) OR @AvailabilityGroupFileName = '' OR @AvailabilityGroupFileName NOT LIKE '%.{FileExtension}' OR (@NumberOfFiles > 1 AND @AvailabilityGroupFileName NOT LIKE '%{FileNumber}%') OR @AvailabilityGroupFileName LIKE '%{DirectorySeperator}%' OR @AvailabilityGroupFileName LIKE '%/%' OR @AvailabilityGroupFileName LIKE '%\%'
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @AvailabilityGroupFileName is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@DirectoryStructure,'{DirectorySeparator}',''),'{ServerName}',''),'{InstanceName}',''),'{ServiceName}',''),'{ClusterName}',''),'{AvailabilityGroupName}',''),'{DatabaseName}',''),'{BackupType}',''),'{Partial}',''),'{CopyOnly}',''),'{Description}',''),'{Year}',''),'{Month}',''),'{Day}',''),'{Hour}',''),'{Minute}',''),'{Second}',''),'{Millisecond}',''),'{MajorVersion}',''),'{MinorVersion}','') LIKE '%{%'
  OR REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@DirectoryStructure,'{DirectorySeparator}',''),'{ServerName}',''),'{InstanceName}',''),'{ServiceName}',''),'{ClusterName}',''),'{AvailabilityGroupName}',''),'{DatabaseName}',''),'{BackupType}',''),'{Partial}',''),'{CopyOnly}',''),'{Description}',''),'{Year}',''),'{Month}',''),'{Day}',''),'{Hour}',''),'{Minute}',''),'{Second}',''),'{Millisecond}',''),'{MajorVersion}',''),'{MinorVersion}','') LIKE '%}%'
  BEGIN
    SET @ErrorMessage = 'The parameter @DirectoryStructure contains one or more tokens that are not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@AvailabilityGroupDirectoryStructure,'{DirectorySeparator}',''),'{ServerName}',''),'{InstanceName}',''),'{ServiceName}',''),'{ClusterName}',''),'{AvailabilityGroupName}',''),'{DatabaseName}',''),'{BackupType}',''),'{Partial}',''),'{CopyOnly}',''),'{Description}',''),'{Year}',''),'{Month}',''),'{Day}',''),'{Hour}',''),'{Minute}',''),'{Second}',''),'{Millisecond}',''),'{MajorVersion}',''),'{MinorVersion}','') LIKE '%{%'
  OR REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@AvailabilityGroupDirectoryStructure,'{DirectorySeparator}',''),'{ServerName}',''),'{InstanceName}',''),'{ServiceName}',''),'{ClusterName}',''),'{AvailabilityGroupName}',''),'{DatabaseName}',''),'{BackupType}',''),'{Partial}',''),'{CopyOnly}',''),'{Description}',''),'{Year}',''),'{Month}',''),'{Day}',''),'{Hour}',''),'{Minute}',''),'{Second}',''),'{Millisecond}',''),'{MajorVersion}',''),'{MinorVersion}','') LIKE '%}%'
  BEGIN
    SET @ErrorMessage = 'The parameter @AvailabilityGroupDirectoryStructure contains one or more tokens that are not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@FileName,'{DirectorySeparator}',''),'{ServerName}',''),'{InstanceName}',''),'{ServiceName}',''),'{ClusterName}',''),'{AvailabilityGroupName}',''),'{DatabaseName}',''),'{BackupType}',''),'{Partial}',''),'{CopyOnly}',''),'{Description}',''),'{Year}',''),'{Month}',''),'{Day}',''),'{Hour}',''),'{Minute}',''),'{Second}',''),'{Millisecond}',''),'{FileNumber}',''),'{FileExtension}',''),'{MajorVersion}',''),'{MinorVersion}','') LIKE '%{%'
  OR REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@FileName,'{DirectorySeparator}',''),'{ServerName}',''),'{InstanceName}',''),'{ServiceName}',''),'{ClusterName}',''),'{AvailabilityGroupName}',''),'{DatabaseName}',''),'{BackupType}',''),'{Partial}',''),'{CopyOnly}',''),'{Description}',''),'{Year}',''),'{Month}',''),'{Day}',''),'{Hour}',''),'{Minute}',''),'{Second}',''),'{Millisecond}',''),'{FileNumber}',''),'{FileExtension}',''),'{MajorVersion}',''),'{MinorVersion}','') LIKE '%}%'
  BEGIN
    SET @ErrorMessage = 'The parameter @FileName contains one or more tokens that are not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@AvailabilityGroupFileName,'{DirectorySeparator}',''),'{ServerName}',''),'{InstanceName}',''),'{ServiceName}',''),'{ClusterName}',''),'{AvailabilityGroupName}',''),'{DatabaseName}',''),'{BackupType}',''),'{Partial}',''),'{CopyOnly}',''),'{Description}',''),'{Year}',''),'{Month}',''),'{Day}',''),'{Hour}',''),'{Minute}',''),'{Second}',''),'{Millisecond}',''),'{FileNumber}',''),'{FileExtension}',''),'{MajorVersion}',''),'{MinorVersion}','') LIKE '%{%'
  OR REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@AvailabilityGroupFileName,'{DirectorySeparator}',''),'{ServerName}',''),'{InstanceName}',''),'{ServiceName}',''),'{ClusterName}',''),'{AvailabilityGroupName}',''),'{DatabaseName}',''),'{BackupType}',''),'{Partial}',''),'{CopyOnly}',''),'{Description}',''),'{Year}',''),'{Month}',''),'{Day}',''),'{Hour}',''),'{Minute}',''),'{Second}',''),'{Millisecond}',''),'{FileNumber}',''),'{FileExtension}',''),'{MajorVersion}',''),'{MinorVersion}','') LIKE '%}%'
  BEGIN
    SET @ErrorMessage = 'The parameter @AvailabilityGroupFileName contains one or more tokens that are not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @FileExtensionFull LIKE '%.%'
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @FileExtensionFull is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @FileExtensionDiff LIKE '%.%'
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @FileExtensionDiff is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @FileExtensionLog LIKE '%.%'
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @FileExtensionLog is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @Init NOT IN('Y','N') OR @Init IS NULL OR (@Init = 'Y' AND @BackupType = 'LOG') OR (@Init = 'Y' AND @BackupSoftware = 'DATA_DOMAIN_BOOST')
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @Init is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @DatabaseOrder NOT IN('DATABASE_NAME_ASC','DATABASE_NAME_DESC','DATABASE_SIZE_ASC','DATABASE_SIZE_DESC','LOG_SIZE_SINCE_LAST_LOG_BACKUP_ASC','LOG_SIZE_SINCE_LAST_LOG_BACKUP_DESC') OR (@DatabaseOrder IN('LOG_SIZE_SINCE_LAST_LOG_BACKUP_ASC','LOG_SIZE_SINCE_LAST_LOG_BACKUP_DESC') AND NOT ((@Version >= 12.06024 AND @Version < 13) OR @Version >= 13.05026 OR SERVERPROPERTY('EngineEdition') = 8)) OR (@DatabaseOrder IS NOT NULL AND SERVERPROPERTY('EngineEdition') = 5)
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @DatabaseOrder is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @DatabasesInParallel NOT IN('Y','N') OR @DatabasesInParallel IS NULL OR (@DatabasesInParallel = 'Y' AND SERVERPROPERTY('EngineEdition') = 5)
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @DatabasesInParallel is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @LogToTable NOT IN('Y','N') OR @LogToTable IS NULL
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @LogToTable is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @Execute NOT IN('Y','N') OR @Execute IS NULL
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @Execute is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  IF @Error <> 0
  BEGIN
    SET @ErrorMessage = 'The documentation is available at https://ola.hallengren.com/sql-server-backup.html.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
    SET @ReturnCode = @Error
    GOTO Logging
  END

  ----------------------------------------------------------------------------------------------------
  --// Check that selected databases and availability groups exist                                //--
  ----------------------------------------------------------------------------------------------------

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + ', '
  FROM @SelectedDatabases
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases)
  IF @@ROWCOUNT > 0
  BEGIN
    SET @ErrorMessage = 'The following databases in the @Databases parameter do not exist: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.'
    RAISERROR('%s',10,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(AvailabilityGroupName) + ', '
  FROM @SelectedAvailabilityGroups
  WHERE AvailabilityGroupName NOT LIKE '%[%]%'
  AND AvailabilityGroupName NOT IN (SELECT AvailabilityGroupName FROM @tmpAvailabilityGroups)
  IF @@ROWCOUNT > 0
  BEGIN
    SET @ErrorMessage = 'The following availability groups do not exist: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.'
    RAISERROR('%s',10,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  ----------------------------------------------------------------------------------------------------
  --// Check @@SERVERNAME                                                                         //--
  ----------------------------------------------------------------------------------------------------

  IF @@SERVERNAME <> CAST(SERVERPROPERTY('ServerName') AS nvarchar(max)) AND SERVERPROPERTY('IsHadrEnabled') = 1
  BEGIN
    SET @ErrorMessage = 'The @@SERVERNAME does not match SERVERPROPERTY(''ServerName''). See ' + CASE WHEN SERVERPROPERTY('IsClustered') = 0 THEN 'https://docs.microsoft.com/en-us/sql/database-engine/install-windows/rename-a-computer-that-hosts-a-stand-alone-instance-of-sql-server' WHEN SERVERPROPERTY('IsClustered') = 1 THEN 'https://docs.microsoft.com/en-us/sql/sql-server/failover-clusters/install/rename-a-sql-server-failover-cluster-instance' END + '.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  ----------------------------------------------------------------------------------------------------
  --// Check Availability Group cluster name                                                      //--
  ----------------------------------------------------------------------------------------------------

  IF @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1
  BEGIN
    SELECT @Cluster = NULLIF(cluster_name,'')
    FROM sys.dm_hadr_cluster
  END

  ----------------------------------------------------------------------------------------------------
  --// Update database order                                                                      //--
  ----------------------------------------------------------------------------------------------------

  IF @DatabaseOrder IN('DATABASE_SIZE_ASC','DATABASE_SIZE_DESC')
  BEGIN
    UPDATE tmpDatabases
    SET DatabaseSize = (SELECT SUM(size) FROM sys.master_files WHERE [type] = 0 AND database_id = DB_ID(tmpDatabases.DatabaseName))
    FROM @tmpDatabases tmpDatabases
  END

  IF @DatabaseOrder IN('LOG_SIZE_SINCE_LAST_LOG_BACKUP_ASC','LOG_SIZE_SINCE_LAST_LOG_BACKUP_DESC')
  BEGIN
    UPDATE tmpDatabases
    SET LogSizeSinceLastLogBackup = (SELECT log_since_last_log_backup_mb FROM sys.dm_db_log_stats(DB_ID(tmpDatabases.DatabaseName)))
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
  ELSE
  IF @DatabaseOrder = 'LOG_SIZE_SINCE_LAST_LOG_BACKUP_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY LogSizeSinceLastLogBackup ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'LOG_SIZE_SINCE_LAST_LOG_BACKUP_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY LogSizeSinceLastLogBackup DESC) AS RowNumber
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
      AND [Parameters] = @Parameters

      IF @QueueID IS NULL
      BEGIN
        BEGIN TRANSACTION

        SELECT @QueueID = QueueID
        FROM dbo.[Queue] WITH (UPDLOCK, TABLOCK)
        WHERE SchemaName = @SchemaName
        AND ObjectName = @ObjectName
        AND [Parameters] = @Parameters

        IF @QueueID IS NULL
        BEGIN
          INSERT INTO dbo.[Queue] (SchemaName, ObjectName, [Parameters])
          SELECT @SchemaName, @ObjectName, @Parameters

          SET @QueueID = SCOPE_IDENTITY()
        END

        COMMIT TRANSACTION
      END

      BEGIN TRANSACTION

      UPDATE [Queue]
      SET QueueStartTime = GETDATE(),
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
        AND NOT EXISTS (SELECT * FROM dbo.QueueDatabase WHERE DatabaseName = tmpDatabases.DatabaseName AND QueueID = @QueueID)

        DELETE QueueDatabase
        FROM dbo.QueueDatabase QueueDatabase
        WHERE QueueID = @QueueID
        AND NOT EXISTS (SELECT * FROM @tmpDatabases tmpDatabases WHERE DatabaseName = QueueDatabase.DatabaseName AND Selected = 1)

        UPDATE QueueDatabase
        SET DatabaseOrder = tmpDatabases.[Order]
        FROM dbo.QueueDatabase QueueDatabase
        INNER JOIN @tmpDatabases tmpDatabases ON QueueDatabase.DatabaseName = tmpDatabases.DatabaseName
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
      SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar) + ', ' + ISNULL(ERROR_MESSAGE(),'')
      RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
      RAISERROR(@EmptyLine,10,1) WITH NOWAIT
      SET @ReturnCode = ERROR_NUMBER()
      GOTO Logging
    END CATCH

  END

  ----------------------------------------------------------------------------------------------------
  --// Execute backup commands                                                                    //--
  ----------------------------------------------------------------------------------------------------

  WHILE (1 = 1)
    AND @BackupType <> 'COPY_PENDING_MIRRORS' -- skip the whole regular backup stuff, if only the pending mirrors should be copied
  BEGIN

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
      SET DatabaseStartTime = GETDATE(),
          DatabaseEndTime = NULL,
          SessionID = @@SPID,
          RequestID = (SELECT request_id FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          RequestStartTime = (SELECT start_time FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          @CurrentDatabaseName = DatabaseName,
          @CurrentDatabaseNameFS = (SELECT DatabaseNameFS FROM @tmpDatabases WHERE DatabaseName = QueueDatabase.DatabaseName)
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
                   @CurrentDatabaseName = DatabaseName,
                   @CurrentDatabaseNameFS = DatabaseNameFS
      FROM @tmpDatabases
      WHERE Selected = 1
      AND Completed = 0
      ORDER BY [Order] ASC
    END

    IF @@ROWCOUNT = 0
    BEGIN
      BREAK
    END

    SET @CurrentDatabaseID = DB_ID(@CurrentDatabaseName)

    IF DATABASEPROPERTYEX(@CurrentDatabaseName,'Status') = 'ONLINE'
    BEGIN
      IF EXISTS (SELECT * FROM sys.database_recovery_status WHERE database_id = @CurrentDatabaseID AND database_guid IS NOT NULL)
      BEGIN
        SET @CurrentIsDatabaseAccessible = 1
      END
      ELSE
      BEGIN
        SET @CurrentIsDatabaseAccessible = 0
      END
    END

    IF @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1
    BEGIN
      SELECT @CurrentAvailabilityGroup = availability_groups.name,
             @CurrentAvailabilityGroupRole = dm_hadr_availability_replica_states.role_desc,
             @CurrentAvailabilityGroupBackupPreference = UPPER(availability_groups.automated_backup_preference_desc)
      FROM sys.databases databases
      INNER JOIN sys.availability_databases_cluster availability_databases_cluster ON databases.group_database_id = availability_databases_cluster.group_database_id
      INNER JOIN sys.availability_groups availability_groups ON availability_databases_cluster.group_id = availability_groups.group_id
      INNER JOIN sys.dm_hadr_availability_replica_states dm_hadr_availability_replica_states ON availability_groups.group_id = dm_hadr_availability_replica_states.group_id AND databases.replica_id = dm_hadr_availability_replica_states.replica_id
      WHERE databases.name = @CurrentDatabaseName
    END

    IF @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1 AND @CurrentAvailabilityGroup IS NOT NULL
    BEGIN
      SELECT @CurrentIsPreferredBackupReplica = sys.fn_hadr_backup_is_preferred_replica(@CurrentDatabaseName)
    END

    SELECT @CurrentDifferentialBaseLSN = differential_base_lsn
    FROM sys.master_files
    WHERE database_id = @CurrentDatabaseID
    AND [type] = 0
    AND [file_id] = 1

    -- Workaround for a bug in SQL Server 2005
    IF @Version >= 9 AND @Version < 10
    AND EXISTS(SELECT * FROM sys.master_files WHERE database_id = @CurrentDatabaseID AND [type] = 0 AND [file_id] = 1 AND differential_base_lsn IS NOT NULL AND differential_base_guid IS NOT NULL AND differential_base_time IS NULL)
    BEGIN
      SET @CurrentDifferentialBaseLSN = NULL
    END

    IF DATABASEPROPERTYEX(@CurrentDatabaseName,'Status') = 'ONLINE'
    BEGIN
      SELECT @CurrentLogLSN = last_log_backup_lsn
      FROM sys.database_recovery_status
      WHERE database_id = @CurrentDatabaseID
    END

    IF DATABASEPROPERTYEX(@CurrentDatabaseName,'Status') = 'ONLINE' AND ((@Version >= 12.06024 AND @Version < 13) OR @Version >= 13.05026) AND (@CurrentAvailabilityGroupRole = 'PRIMARY' OR @CurrentAvailabilityGroupRole IS NULL)
    BEGIN
      SET @CurrentCommand07 = 'SELECT @ParamAllocatedExtentPageCount = SUM(allocated_extent_page_count), @ParamModifiedExtentPageCount = SUM(modified_extent_page_count) FROM ' + QUOTENAME(@CurrentDatabaseName) + '.sys.dm_db_file_space_usage'

      EXECUTE sp_executesql @statement = @CurrentCommand07, @params = N'@ParamAllocatedExtentPageCount bigint OUTPUT, @ParamModifiedExtentPageCount bigint OUTPUT', @ParamAllocatedExtentPageCount = @CurrentAllocatedExtentPageCount OUTPUT, @ParamModifiedExtentPageCount = @CurrentModifiedExtentPageCount OUTPUT
    END

    SET @CurrentBackupType = @BackupType

    IF @ChangeBackupType = 'Y'
    BEGIN
      IF @CurrentBackupType = 'LOG' AND DATABASEPROPERTYEX(@CurrentDatabaseName,'Recovery') <> 'SIMPLE' AND @CurrentLogLSN IS NULL AND @CurrentDatabaseName <> 'master'
      BEGIN
        SET @CurrentBackupType = 'DIFF'
      END
      IF @CurrentBackupType = 'DIFF' AND (@CurrentDatabaseName = 'master' OR @CurrentDifferentialBaseLSN IS NULL OR (@CurrentModifiedExtentPageCount * 1. / @CurrentAllocatedExtentPageCount * 100 >= @ModificationLevel))
      BEGIN
        SET @CurrentBackupType = 'FULL'
      END
    END

    IF DATABASEPROPERTYEX(@CurrentDatabaseName,'Status') = 'ONLINE' AND ((@Version >= 12.06024 AND @Version < 13) OR @Version >= 13.05026)
    BEGIN
      SELECT @CurrentLastLogBackup = log_backup_time,
             @CurrentLogSizeSinceLastLogBackup = log_since_last_log_backup_mb
      FROM sys.dm_db_log_stats (@CurrentDatabaseID)
    END

    IF @CurrentBackupType = 'DIFF'
    BEGIN
      SELECT @CurrentDifferentialBaseIsSnapshot = is_snapshot
      FROM msdb.dbo.backupset
      WHERE database_name = @CurrentDatabaseName
      AND [type] = 'D'
      AND checkpoint_lsn = @CurrentDifferentialBaseLSN
    END

    IF @ChangeBackupType = 'Y'
    BEGIN
      IF @CurrentBackupType = 'DIFF' AND @CurrentDifferentialBaseIsSnapshot = 1
      BEGIN
        SET @CurrentBackupType = 'FULL'
      END
    END

    SELECT @CurrentDatabaseMirroringRole = UPPER(mirroring_role_desc)
    FROM sys.database_mirroring
    WHERE database_id = @CurrentDatabaseID

    IF EXISTS (SELECT * FROM msdb.dbo.log_shipping_primary_databases WHERE primary_database = @CurrentDatabaseName)
    BEGIN
      SET @CurrentLogShippingRole = 'PRIMARY'
    END
    ELSE
    IF EXISTS (SELECT * FROM msdb.dbo.log_shipping_secondary_databases WHERE secondary_database = @CurrentDatabaseName)
    BEGIN
      SET @CurrentLogShippingRole = 'SECONDARY'
    END

    SELECT @CurrentIsReadOnly = is_read_only
    FROM sys.databases
    WHERE name = @CurrentDatabaseName

    IF @Version >= 10
    BEGIN
      SET @CurrentCommand06 = 'SELECT @ParamIsEncrypted = is_encrypted FROM sys.databases WHERE name = @ParamDatabaseName'

      EXECUTE sp_executesql @statement = @CurrentCommand06, @params = N'@ParamDatabaseName nvarchar(max), @ParamIsEncrypted bit OUTPUT', @ParamDatabaseName = @CurrentDatabaseName, @ParamIsEncrypted = @CurrentIsEncrypted OUTPUT
    END

    SET @DatabaseMessage = 'Date and time: ' + CONVERT(nvarchar,GETDATE(),120)
    RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

    SET @DatabaseMessage = 'Database: ' + QUOTENAME(@CurrentDatabaseName)
    RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

    SET @DatabaseMessage = 'Status: ' + CAST(DATABASEPROPERTYEX(@CurrentDatabaseName,'Status') AS nvarchar)
    RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

    SET @DatabaseMessage = 'Standby: ' + CASE WHEN DATABASEPROPERTYEX(@CurrentDatabaseName,'IsInStandBy') = 1 THEN 'Yes' ELSE 'No' END
    RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

    SET @DatabaseMessage =  'Updateability: ' + CASE WHEN @CurrentIsReadOnly = 1 THEN 'READ_ONLY' WHEN  @CurrentIsReadOnly = 0 THEN 'READ_WRITE' END
    RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

    SET @DatabaseMessage =  'User access: ' + CAST(DATABASEPROPERTYEX(@CurrentDatabaseName,'UserAccess') AS nvarchar)
    RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

    IF @CurrentIsDatabaseAccessible IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Is accessible: ' + CASE WHEN @CurrentIsDatabaseAccessible = 1 THEN 'Yes' ELSE 'No' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    SET @DatabaseMessage = 'Recovery model: ' + CAST(DATABASEPROPERTYEX(@CurrentDatabaseName,'Recovery') AS nvarchar)
    RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

    SET @DatabaseMessage = 'Encrypted: ' + CASE WHEN @CurrentIsEncrypted = 1 THEN 'Yes' WHEN @CurrentIsEncrypted = 0 THEN 'No' ELSE 'N/A' END
    RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

    IF @CurrentAvailabilityGroup IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Availability group: ' + @CurrentAvailabilityGroup
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Availability group role: ' + @CurrentAvailabilityGroupRole
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Availability group backup preference: ' + @CurrentAvailabilityGroupBackupPreference
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Is preferred backup replica: ' + CASE WHEN @CurrentIsPreferredBackupReplica = 1 THEN 'Yes' WHEN @CurrentIsPreferredBackupReplica = 0 THEN 'No' ELSE 'N/A' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    IF @CurrentDatabaseMirroringRole IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Database mirroring role: ' + @CurrentDatabaseMirroringRole
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    IF @CurrentLogShippingRole IS NOT NULL
    BEGIN
      SET @DatabaseMessage = 'Log shipping role: ' + @CurrentLogShippingRole
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    SET @DatabaseMessage = 'Differential base LSN: ' + ISNULL(CAST(@CurrentDifferentialBaseLSN AS nvarchar),'N/A')
    RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

    IF @CurrentBackupType = 'DIFF'
    BEGIN
      SET @DatabaseMessage = 'Differential base is snapshot: ' + CASE WHEN @CurrentDifferentialBaseIsSnapshot = 1 THEN 'Yes' WHEN @CurrentDifferentialBaseIsSnapshot = 0 THEN 'No' ELSE 'N/A' END
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    SET @DatabaseMessage = 'Last log backup LSN: ' + ISNULL(CAST(@CurrentLogLSN AS nvarchar),'N/A')
    RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

    IF @CurrentBackupType IN('DIFF','FULL') AND ((@Version >= 12.06024 AND @Version < 13) OR @Version >= 13.05026)
    BEGIN
      SET @DatabaseMessage = 'Allocated extent page count: ' + ISNULL(CAST(@CurrentAllocatedExtentPageCount AS nvarchar),'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Modified extent page count: ' + ISNULL(CAST(@CurrentModifiedExtentPageCount AS nvarchar),'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    IF @CurrentBackupType = 'LOG' AND ((@Version >= 12.06024 AND @Version < 13) OR @Version >= 13.05026)
    BEGIN
      SET @DatabaseMessage = 'Last log backup: ' + ISNULL(CONVERT(nvarchar(19),@CurrentLastLogBackup,120),'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT

      SET @DatabaseMessage = 'Log size since last log backup (MB): ' + ISNULL(CAST(@CurrentLogSizeSinceLastLogBackup AS nvarchar),'N/A')
      RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
    END

    RAISERROR(@EmptyLine,10,1) WITH NOWAIT

    IF DATABASEPROPERTYEX(@CurrentDatabaseName,'Status') = 'ONLINE'
    AND (@CurrentIsDatabaseAccessible = 1 OR @CurrentIsDatabaseAccessible IS NULL)
    AND DATABASEPROPERTYEX(@CurrentDatabaseName,'IsInStandBy') = 0
    AND NOT (@CurrentBackupType = 'LOG' AND (DATABASEPROPERTYEX(@CurrentDatabaseName,'Recovery') = 'SIMPLE' OR @CurrentLogLSN IS NULL))
    AND NOT (@CurrentBackupType = 'DIFF' AND @CurrentDifferentialBaseLSN IS NULL)
    AND NOT (@CurrentBackupType IN('DIFF','LOG') AND @CurrentDatabaseName = 'master')
    AND NOT (@CurrentAvailabilityGroup IS NOT NULL AND @CurrentBackupType = 'FULL' AND @CopyOnly = 'N' AND (@CurrentAvailabilityGroupRole <> 'PRIMARY' OR @CurrentAvailabilityGroupRole IS NULL))
    AND NOT (@CurrentAvailabilityGroup IS NOT NULL AND @CurrentBackupType = 'FULL' AND @CopyOnly = 'Y' AND (@CurrentIsPreferredBackupReplica <> 1 OR @CurrentIsPreferredBackupReplica IS NULL) AND @OverrideBackupPreference = 'N')
    AND NOT (@CurrentAvailabilityGroup IS NOT NULL AND @CurrentBackupType = 'DIFF' AND (@CurrentAvailabilityGroupRole <> 'PRIMARY' OR @CurrentAvailabilityGroupRole IS NULL))
    AND NOT (@CurrentAvailabilityGroup IS NOT NULL AND @CurrentBackupType = 'LOG' AND @CopyOnly = 'N' AND (@CurrentIsPreferredBackupReplica <> 1 OR @CurrentIsPreferredBackupReplica IS NULL) AND @OverrideBackupPreference = 'N')
    AND NOT (@CurrentAvailabilityGroup IS NOT NULL AND @CurrentBackupType = 'LOG' AND @CopyOnly = 'Y' AND (@CurrentAvailabilityGroupRole <> 'PRIMARY' OR @CurrentAvailabilityGroupRole IS NULL))
    AND NOT ((@CurrentLogShippingRole = 'PRIMARY' AND @CurrentLogShippingRole IS NOT NULL) AND @CurrentBackupType = 'LOG')
    AND NOT (@CurrentIsReadOnly = 1 AND @Updateability = 'READ_WRITE')
    AND NOT (@CurrentIsReadOnly = 0 AND @Updateability = 'READ_ONLY')
    AND NOT (@CurrentBackupType = 'LOG' AND @LogSizeSinceLastLogBackup IS NOT NULL AND @TimeSinceLastLogBackup IS NOT NULL AND NOT(@CurrentLogSizeSinceLastLogBackup >= @LogSizeSinceLastLogBackup OR @CurrentLogSizeSinceLastLogBackup IS NULL OR DATEDIFF(SECOND,@CurrentLastLogBackup,GETDATE()) >= @TimeSinceLastLogBackup OR @CurrentLastLogBackup IS NULL))
    BEGIN

      IF @CurrentBackupType = 'LOG' AND (@CleanupTime        IS NOT NULL
                                      OR @MirrorCleanupTime  IS NOT NULL
                                      OR @MirrorCleanupTime2 IS NOT NULL
                                      OR @MirrorCleanupTime3 IS NOT NULL
                                         )
      BEGIN
        SELECT @CurrentLatestBackup = MAX(backup_finish_date)
        FROM msdb.dbo.backupset
        WHERE ([type] IN('D','I')
        OR database_backup_lsn < @CurrentDifferentialBaseLSN)
        AND is_damaged = 0
        AND database_name = @CurrentDatabaseName
      END

      SET @CurrentDate = GETDATE()

      INSERT INTO @CurrentCleanupDates (CleanupDate)
      SELECT @CurrentDate

      IF @CurrentBackupType = 'LOG'
      BEGIN
        INSERT INTO @CurrentCleanupDates (CleanupDate)
        SELECT @CurrentLatestBackup
      END

      SELECT @CurrentDirectoryStructure = CASE
      WHEN @CurrentAvailabilityGroup IS NOT NULL THEN @AvailabilityGroupDirectoryStructure
      ELSE @DirectoryStructure
      END

      IF @CurrentDirectoryStructure IS NOT NULL
      BEGIN
      -- Directory structure - remove tokens that are not needed
        IF @ReadWriteFileGroups = 'N' SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Partial}','')
        IF @CopyOnly = 'N' SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{CopyOnly}','')
        IF @Cluster IS NULL SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{ClusterName}','')
        IF @CurrentAvailabilityGroup IS NULL SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{AvailabilityGroupName}','')
        IF SERVERPROPERTY('InstanceName') IS NULL SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{InstanceName}','')
        IF @@SERVICENAME IS NULL SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{ServiceName}','')
        IF @Description IS NULL SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Description}','')

        IF @Directory IS NULL AND @MirrorDirectory IS NULL AND @URL IS NULL AND @DefaultDirectory LIKE '%' + '.' + @@SERVICENAME + @DirectorySeparator + 'MSSQL' + @DirectorySeparator + 'Backup'
        BEGIN
          SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{ServerName}','')
          SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{InstanceName}','')
          SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{ClusterName}','')
          SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{AvailabilityGroupName}','')
        END

        WHILE (@Updated = 1 OR @Updated IS NULL)
        BEGIN
          SET @Updated = 0

          IF CHARINDEX('\',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'\','{DirectorySeparator}')
            SET @Updated = 1
          END

          IF CHARINDEX('/',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'/','{DirectorySeparator}')
            SET @Updated = 1
          END

          IF CHARINDEX('__',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'__','_')
            SET @Updated = 1
          END

          IF CHARINDEX('--',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'--','-')
            SET @Updated = 1
          END

          IF CHARINDEX('{DirectorySeparator}{DirectorySeparator}',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{DirectorySeparator}{DirectorySeparator}','{DirectorySeparator}')
            SET @Updated = 1
          END

          IF CHARINDEX('{DirectorySeparator}$',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{DirectorySeparator}$','{DirectorySeparator}')
            SET @Updated = 1
          END
          IF CHARINDEX('${DirectorySeparator}',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'${DirectorySeparator}','{DirectorySeparator}')
            SET @Updated = 1
          END

          IF CHARINDEX('{DirectorySeparator}_',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{DirectorySeparator}_','{DirectorySeparator}')
            SET @Updated = 1
          END
          IF CHARINDEX('_{DirectorySeparator}',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'_{DirectorySeparator}','{DirectorySeparator}')
            SET @Updated = 1
          END

          IF CHARINDEX('{DirectorySeparator}-',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{DirectorySeparator}-','{DirectorySeparator}')
            SET @Updated = 1
          END
          IF CHARINDEX('-{DirectorySeparator}',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'-{DirectorySeparator}','{DirectorySeparator}')
            SET @Updated = 1
          END

          IF CHARINDEX('_$',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'_$','_')
            SET @Updated = 1
          END
          IF CHARINDEX('$_',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'$_','_')
            SET @Updated = 1
          END

          IF CHARINDEX('-$',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'-$','-')
            SET @Updated = 1
          END
          IF CHARINDEX('$-',@CurrentDirectoryStructure) > 0
          BEGIN
            SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'$-','-')
            SET @Updated = 1
          END

          IF LEFT(@CurrentDirectoryStructure,1) = '_'
          BEGIN
            SET @CurrentDirectoryStructure = RIGHT(@CurrentDirectoryStructure,LEN(@CurrentDirectoryStructure) - 1)
            SET @Updated = 1
          END
          IF RIGHT(@CurrentDirectoryStructure,1) = '_'
          BEGIN
            SET @CurrentDirectoryStructure = LEFT(@CurrentDirectoryStructure,LEN(@CurrentDirectoryStructure) - 1)
            SET @Updated = 1
          END

          IF LEFT(@CurrentDirectoryStructure,1) = '-'
          BEGIN
            SET @CurrentDirectoryStructure = RIGHT(@CurrentDirectoryStructure,LEN(@CurrentDirectoryStructure) - 1)
            SET @Updated = 1
          END
          IF RIGHT(@CurrentDirectoryStructure,1) = '-'
          BEGIN
            SET @CurrentDirectoryStructure = LEFT(@CurrentDirectoryStructure,LEN(@CurrentDirectoryStructure) - 1)
            SET @Updated = 1
          END

          IF LEFT(@CurrentDirectoryStructure,1) = '$'
          BEGIN
            SET @CurrentDirectoryStructure = RIGHT(@CurrentDirectoryStructure,LEN(@CurrentDirectoryStructure) - 1)
            SET @Updated = 1
          END
          IF RIGHT(@CurrentDirectoryStructure,1) = '$'
          BEGIN
            SET @CurrentDirectoryStructure = LEFT(@CurrentDirectoryStructure,LEN(@CurrentDirectoryStructure) - 1)
            SET @Updated = 1
          END

          IF LEFT(@CurrentDirectoryStructure,20) = '{DirectorySeparator}'
          BEGIN
            SET @CurrentDirectoryStructure = RIGHT(@CurrentDirectoryStructure,LEN(@CurrentDirectoryStructure) - 20)
            SET @Updated = 1
          END
          IF RIGHT(@CurrentDirectoryStructure,20) = '{DirectorySeparator}'
          BEGIN
            SET @CurrentDirectoryStructure = LEFT(@CurrentDirectoryStructure,LEN(@CurrentDirectoryStructure) - 20)
            SET @Updated = 1
          END
        END

        SET @Updated = NULL

        -- Directory structure - replace tokens with real values
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{DirectorySeparator}',@DirectorySeparator)
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{ServerName}',CASE WHEN SERVERPROPERTY('EngineEdition') = 8 THEN LEFT(CAST(SERVERPROPERTY('ServerName') AS nvarchar(max)),CHARINDEX('.',CAST(SERVERPROPERTY('ServerName') AS nvarchar(max))) - 1) ELSE CAST(SERVERPROPERTY('MachineName') AS nvarchar(max)) END)
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{InstanceName}',ISNULL(CAST(SERVERPROPERTY('InstanceName') AS nvarchar(max)),''))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{ServiceName}',ISNULL(@@SERVICENAME,''))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{ClusterName}',ISNULL(@Cluster,''))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{AvailabilityGroupName}',ISNULL(@CurrentAvailabilityGroup,''))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{DatabaseName}',@CurrentDatabaseNameFS)
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{BackupType}',@CurrentBackupType)
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Partial}','PARTIAL')
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{CopyOnly}','COPY_ONLY')
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Description}',LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ISNULL(@Description,''),'\',''),'/',''),':',''),'*',''),'?',''),'"',''),'<',''),'>',''),'|',''))))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Year}',CAST(DATEPART(YEAR,@CurrentDate) AS nvarchar))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Month}',RIGHT('0' + CAST(DATEPART(MONTH,@CurrentDate) AS nvarchar),2))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Day}',RIGHT('0' + CAST(DATEPART(DAY,@CurrentDate) AS nvarchar),2))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Hour}',RIGHT('0' + CAST(DATEPART(HOUR,@CurrentDate) AS nvarchar),2))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Minute}',RIGHT('0' + CAST(DATEPART(MINUTE,@CurrentDate) AS nvarchar),2))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Second}',RIGHT('0' + CAST(DATEPART(SECOND,@CurrentDate) AS nvarchar),2))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{Millisecond}',RIGHT('00' + CAST(DATEPART(MILLISECOND,@CurrentDate) AS nvarchar),3))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{MajorVersion}',ISNULL(CAST(SERVERPROPERTY('ProductMajorVersion') AS nvarchar),PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar),4)))
        SET @CurrentDirectoryStructure = REPLACE(@CurrentDirectoryStructure,'{MinorVersion}',ISNULL(CAST(SERVERPROPERTY('ProductMinorVersion') AS nvarchar),PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar),3)))
      END

      INSERT INTO @CurrentDirectories (ID, DirectoryPath, Mirror, DirectoryNumber, CreateCompleted, CleanupCompleted)
      SELECT ROW_NUMBER() OVER (ORDER BY ID),
             DirectoryPath + CASE WHEN @CurrentDirectoryStructure IS NOT NULL THEN @DirectorySeparator + @CurrentDirectoryStructure ELSE '' END,
             Mirror,
             ROW_NUMBER() OVER (PARTITION BY Mirror ORDER BY ID ASC),
             0,
             0
      FROM @Directories
      ORDER BY ID ASC

      INSERT INTO @CurrentURLs (ID, DirectoryPath, Mirror, DirectoryNumber)
      SELECT ROW_NUMBER() OVER (ORDER BY ID),
             DirectoryPath + CASE WHEN @CurrentDirectoryStructure IS NOT NULL THEN @DirectorySeparator + @CurrentDirectoryStructure ELSE '' END,
             Mirror,
             ROW_NUMBER() OVER (PARTITION BY Mirror ORDER BY ID ASC)
      FROM @URLs
      ORDER BY ID ASC

      SELECT @CurrentDatabaseFileName = CASE
      WHEN @CurrentAvailabilityGroup IS NOT NULL THEN @AvailabilityGroupFileName
      ELSE @FileName
      END

      -- File name - remove tokens that are not needed
      IF @ReadWriteFileGroups = 'N' SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Partial}','')
      IF @CopyOnly = 'N' SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{CopyOnly}','')
      IF @Cluster IS NULL SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{ClusterName}','')
      IF @CurrentAvailabilityGroup IS NULL SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{AvailabilityGroupName}','')
      IF SERVERPROPERTY('InstanceName') IS NULL SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{InstanceName}','')
      IF @@SERVICENAME IS NULL SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{ServiceName}','')
      IF @Description IS NULL SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Description}','')
      IF @NumberOfFiles = 1 SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{FileNumber}','')

      WHILE (@Updated = 1 OR @Updated IS NULL)
      BEGIN
        SET @Updated = 0

        IF CHARINDEX('__',@CurrentDatabaseFileName) > 0
        BEGIN
          SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'__','_')
          SET @Updated = 1
        END

        IF CHARINDEX('--',@CurrentDatabaseFileName) > 0
        BEGIN
          SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'--','-')
          SET @Updated = 1
        END

        IF CHARINDEX('_$',@CurrentDatabaseFileName) > 0
        BEGIN
          SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'_$','_')
          SET @Updated = 1
        END
        IF CHARINDEX('$_',@CurrentDatabaseFileName) > 0
        BEGIN
          SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'$_','_')
          SET @Updated = 1
        END

        IF CHARINDEX('-$',@CurrentDatabaseFileName) > 0
        BEGIN
          SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'-$','-')
          SET @Updated = 1
        END
        IF CHARINDEX('$-',@CurrentDatabaseFileName) > 0
        BEGIN
          SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'$-','-')
          SET @Updated = 1
        END

        IF CHARINDEX('_.',@CurrentDatabaseFileName) > 0
        BEGIN
          SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'_.','.')
          SET @Updated = 1
        END

        IF CHARINDEX('-.',@CurrentDatabaseFileName) > 0
        BEGIN
          SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'-.','.')
          SET @Updated = 1
        END

        IF LEFT(@CurrentDatabaseFileName,1) = '_'
        BEGIN
          SET @CurrentDatabaseFileName = RIGHT(@CurrentDatabaseFileName,LEN(@CurrentDatabaseFileName) - 1)
          SET @Updated = 1
        END
        IF RIGHT(@CurrentDatabaseFileName,1) = '_'
        BEGIN
          SET @CurrentDatabaseFileName = LEFT(@CurrentDatabaseFileName,LEN(@CurrentDatabaseFileName) - 1)
          SET @Updated = 1
        END

        IF LEFT(@CurrentDatabaseFileName,1) = '-'
        BEGIN
          SET @CurrentDatabaseFileName = RIGHT(@CurrentDatabaseFileName,LEN(@CurrentDatabaseFileName) - 1)
          SET @Updated = 1
        END
        IF RIGHT(@CurrentDatabaseFileName,1) = '-'
        BEGIN
          SET @CurrentDatabaseFileName = LEFT(@CurrentDatabaseFileName,LEN(@CurrentDatabaseFileName) - 1)
          SET @Updated = 1
        END

        IF LEFT(@CurrentDatabaseFileName,1) = '$'
        BEGIN
          SET @CurrentDatabaseFileName = RIGHT(@CurrentDatabaseFileName,LEN(@CurrentDatabaseFileName) - 1)
          SET @Updated = 1
        END
        IF RIGHT(@CurrentDatabaseFileName,1) = '$'
        BEGIN
          SET @CurrentDatabaseFileName = LEFT(@CurrentDatabaseFileName,LEN(@CurrentDatabaseFileName) - 1)
          SET @Updated = 1
        END
      END

      SET @Updated = NULL

      SELECT @CurrentFileExtension = CASE
      WHEN @CurrentBackupType = 'FULL' THEN @FileExtensionFull
      WHEN @CurrentBackupType = 'DIFF' THEN @FileExtensionDiff
      WHEN @CurrentBackupType = 'LOG' THEN @FileExtensionLog
      END

      -- File name - replace tokens with real values
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{ServerName}',CASE WHEN SERVERPROPERTY('EngineEdition') = 8 THEN LEFT(CAST(SERVERPROPERTY('ServerName') AS nvarchar(max)),CHARINDEX('.',CAST(SERVERPROPERTY('ServerName') AS nvarchar(max))) - 1) ELSE CAST(SERVERPROPERTY('MachineName') AS nvarchar(max)) END)
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{InstanceName}',ISNULL(CAST(SERVERPROPERTY('InstanceName') AS nvarchar(max)),''))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{ServiceName}',ISNULL(@@SERVICENAME,''))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{ClusterName}',ISNULL(@Cluster,''))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{AvailabilityGroupName}',ISNULL(@CurrentAvailabilityGroup,''))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{BackupType}',@CurrentBackupType)
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Partial}','PARTIAL')
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{CopyOnly}','COPY_ONLY')
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Description}',LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ISNULL(@Description,''),'\',''),'/',''),':',''),'*',''),'?',''),'"',''),'<',''),'>',''),'|',''))))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Year}',CAST(DATEPART(YEAR,@CurrentDate) AS nvarchar))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Month}',RIGHT('0' + CAST(DATEPART(MONTH,@CurrentDate) AS nvarchar),2))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Day}',RIGHT('0' + CAST(DATEPART(DAY,@CurrentDate) AS nvarchar),2))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Hour}',RIGHT('0' + CAST(DATEPART(HOUR,@CurrentDate) AS nvarchar),2))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Minute}',RIGHT('0' + CAST(DATEPART(MINUTE,@CurrentDate) AS nvarchar),2))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Second}',RIGHT('0' + CAST(DATEPART(SECOND,@CurrentDate) AS nvarchar),2))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{Millisecond}',RIGHT('00' + CAST(DATEPART(MILLISECOND,@CurrentDate) AS nvarchar),3))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{FileExtension}',@CurrentFileExtension)
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{MajorVersion}',ISNULL(CAST(SERVERPROPERTY('ProductMajorVersion') AS nvarchar),PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar),4)))
      SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{MinorVersion}',ISNULL(CAST(SERVERPROPERTY('ProductMinorVersion') AS nvarchar),PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar),3)))

      SELECT @CurrentMaxFilePathLength = CASE
      WHEN EXISTS (SELECT * FROM @CurrentDirectories) THEN (SELECT MAX(LEN(DirectoryPath + @DirectorySeparator)) FROM @CurrentDirectories)
      WHEN EXISTS (SELECT * FROM @CurrentURLs) THEN (SELECT MAX(LEN(DirectoryPath + @DirectorySeparator)) FROM @CurrentURLs)
      END
      + LEN(REPLACE(REPLACE(@CurrentDatabaseFileName,'{DatabaseName}',@CurrentDatabaseNameFS), '{FileNumber}', CASE WHEN @NumberOfFiles > 1 AND @NumberOfFiles <= 9 THEN '1' WHEN @NumberOfFiles >= 10 THEN '01' ELSE '' END))

      -- The maximum length of a backup device is 259 characters
      IF @CurrentMaxFilePathLength > 259
      BEGIN
        SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{DatabaseName}',LEFT(@CurrentDatabaseNameFS,CASE WHEN (LEN(@CurrentDatabaseNameFS) + 259 - @CurrentMaxFilePathLength - 3) < 20 THEN 20 ELSE (LEN(@CurrentDatabaseNameFS) + 259 - @CurrentMaxFilePathLength - 3) END) + '...')
      END
      ELSE
      BEGIN
        SET @CurrentDatabaseFileName = REPLACE(@CurrentDatabaseFileName,'{DatabaseName}',@CurrentDatabaseNameFS)
      END

      IF EXISTS (SELECT * FROM @CurrentDirectories WHERE Mirror = 0)
      BEGIN
        SET @CurrentFileNumber = 0

        WHILE @CurrentFileNumber < @NumberOfFiles
        BEGIN
          SET @CurrentFileNumber = @CurrentFileNumber + 1

          SELECT @CurrentDirectoryPath = DirectoryPath
          FROM @CurrentDirectories
          WHERE @CurrentFileNumber >= (DirectoryNumber - 1) * (SELECT @NumberOfFiles / COUNT(*) FROM @CurrentDirectories WHERE Mirror = 0) + 1
          AND @CurrentFileNumber <= DirectoryNumber * (SELECT @NumberOfFiles / COUNT(*) FROM @CurrentDirectories WHERE Mirror = 0)
          AND Mirror = 0
          ORDER BY DirectoryNumber

          SET @CurrentFileName = REPLACE(@CurrentDatabaseFileName, '{FileNumber}', CASE WHEN @NumberOfFiles > 1 AND @NumberOfFiles <= 9 THEN CAST(@CurrentFileNumber AS nvarchar) WHEN @NumberOfFiles >= 10 THEN RIGHT('0' + CAST(@CurrentFileNumber AS nvarchar),2) ELSE '' END)

          SET @CurrentFilePath = @CurrentDirectoryPath + @DirectorySeparator + @CurrentFileName

          INSERT INTO @CurrentFiles ([Type], FilePath, Mirror, FileNumber)
          SELECT 'DISK', @CurrentFilePath, 0, @CurrentFileNumber FileNumber

          SET @CurrentDirectoryPath = NULL
          SET @CurrentFileName = NULL
          SET @CurrentFilePath = NULL
        END

        INSERT INTO @CurrentBackupSet (Mirror, VerifyCompleted)
        SELECT 0, 0
      END

      -- WHILE-Loop for mirrors
      SET @CurrentMirror = -4
      WHILE @CurrentMirror < 4 
      BEGIN
          IF @CurrentMirror <> 0
          AND EXISTS (SELECT * FROM @CurrentDirectories WHERE Mirror = @CurrentMirror)
          BEGIN
            SET @CurrentFileNumber = 0

            WHILE @CurrentFileNumber < @NumberOfFiles
            BEGIN
              SET @CurrentFileNumber = @CurrentFileNumber + 1

              SELECT @CurrentDirectoryPath = DirectoryPath
                FROM @CurrentDirectories
               WHERE @CurrentFileNumber >= (DirectoryNumber - 1) * (SELECT @NumberOfFiles / ISNULL(COUNT(*), 1) FROM @CurrentDirectories WHERE Mirror = @CurrentMirror) + 1
                 AND @CurrentFileNumber <=  DirectoryNumber      * (SELECT @NumberOfFiles / ISNULL(COUNT(*), 1) FROM @CurrentDirectories WHERE Mirror = @CurrentMirror)
                 AND Mirror = @CurrentMirror
               ORDER BY DirectoryNumber

              SET @CurrentFileName = REPLACE(@CurrentDatabaseFileName, '{FileNumber}', CASE WHEN @NumberOfFiles > 1 AND @NumberOfFiles <= 9 THEN CAST(@CurrentFileNumber AS nvarchar) WHEN @NumberOfFiles >= 10 THEN RIGHT('0' + CAST(@CurrentFileNumber AS nvarchar),2) ELSE '' END)

              SET @CurrentFilePath = @CurrentDirectoryPath + @DirectorySeparator + @CurrentFileName

              INSERT INTO @CurrentFiles ([Type], FilePath, Mirror, FileNumber)
              SELECT 'DISK', @CurrentFilePath, @CurrentMirror, @CurrentFileNumber FileNumber

              SET @CurrentDirectoryPath = NULL
              SET @CurrentFileName = NULL
              SET @CurrentFilePath = NULL
            END

            INSERT INTO @CurrentBackupSet (Mirror, VerifyCompleted)
            SELECT @CurrentMirror, 0
          END
          SET @CurrentMirror = @CurrentMirror + 1
      END -- WHILE @CurrentMirror

      IF EXISTS (SELECT * FROM @CurrentURLs WHERE Mirror = 0)
      BEGIN
        SET @CurrentFileNumber = 0

        WHILE @CurrentFileNumber < @NumberOfFiles
        BEGIN
          SET @CurrentFileNumber = @CurrentFileNumber + 1

          SELECT @CurrentDirectoryPath = DirectoryPath
          FROM @CurrentURLs
          WHERE @CurrentFileNumber >= (DirectoryNumber - 1) * (SELECT @NumberOfFiles / COUNT(*) FROM @CurrentURLs WHERE Mirror = 0) + 1
          AND @CurrentFileNumber <= DirectoryNumber * (SELECT @NumberOfFiles / COUNT(*) FROM @CurrentURLs WHERE Mirror = 0)
          AND Mirror = 0

          SET @CurrentFileName = REPLACE(@CurrentDatabaseFileName, '{FileNumber}', CASE WHEN @NumberOfFiles > 1 AND @NumberOfFiles <= 9 THEN CAST(@CurrentFileNumber AS nvarchar) WHEN @NumberOfFiles >= 10 THEN RIGHT('0' + CAST(@CurrentFileNumber AS nvarchar),2) ELSE '' END)

          SET @CurrentFilePath = @CurrentDirectoryPath + @DirectorySeparator + @CurrentFileName

          INSERT INTO @CurrentFiles ([Type], FilePath, Mirror)
          SELECT 'URL', @CurrentFilePath, 0

          SET @CurrentDirectoryPath = NULL
          SET @CurrentFileName = NULL
          SET @CurrentFilePath = NULL
        END

        INSERT INTO @CurrentBackupSet (Mirror, VerifyCompleted)
        SELECT 0, 0
      END

      IF EXISTS (SELECT * FROM @CurrentURLs WHERE Mirror = 1)
      BEGIN
        SET @CurrentFileNumber = 0

        WHILE @CurrentFileNumber < @NumberOfFiles
        BEGIN
          SET @CurrentFileNumber = @CurrentFileNumber + 1

          SELECT @CurrentDirectoryPath = DirectoryPath
          FROM @CurrentURLs
          WHERE @CurrentFileNumber >= (DirectoryNumber - 1) * (SELECT @NumberOfFiles / COUNT(*) FROM @CurrentURLs WHERE Mirror = 0) + 1
          AND @CurrentFileNumber <= DirectoryNumber * (SELECT @NumberOfFiles / COUNT(*) FROM @CurrentURLs WHERE Mirror = 0)
          AND Mirror = 1

          SET @CurrentFileName = REPLACE(@CurrentDatabaseFileName, '{FileNumber}', CASE WHEN @NumberOfFiles > 1 AND @NumberOfFiles <= 9 THEN CAST(@CurrentFileNumber AS nvarchar) WHEN @NumberOfFiles >= 10 THEN RIGHT('0' + CAST(@CurrentFileNumber AS nvarchar),2) ELSE '' END)

          SET @CurrentFilePath = @CurrentDirectoryPath + @DirectorySeparator + @CurrentFileName

          INSERT INTO @CurrentFiles ([Type], FilePath, Mirror)
          SELECT 'URL', @CurrentFilePath, 1

          SET @CurrentDirectoryPath = NULL
          SET @CurrentFileName = NULL
          SET @CurrentFilePath = NULL
        END

        INSERT INTO @CurrentBackupSet (Mirror, VerifyCompleted)
        SELECT 1, 0
      END

      -- Create directory
      IF @HostPlatform = 'Windows'
      AND (@BackupSoftware <> 'DATA_DOMAIN_BOOST' OR @BackupSoftware IS NULL)
      BEGIN
        WHILE (1 = 1)
        BEGIN
          SELECT TOP 1 @CurrentDirectoryID = ID,
                       @CurrentDirectoryPath = DirectoryPath
          FROM @CurrentDirectories
          WHERE CreateCompleted = 0
            AND Mirror >= 0 -- negative Mirrors are pending copies for not available servers (or when @MirrorType = 'COPY_LATER')
          ORDER BY ID ASC

          IF @@ROWCOUNT = 0
          BEGIN
            BREAK
          END

          -- create subdirectory only when it does not exists (otherwise spam in the command log / print output)
          DELETE FROM @DirectoryInfo;
          INSERT INTO @DirectoryInfo (FileExists, FileIsADirectory, ParentDirectoryExists)
          EXECUTE [master].dbo.xp_fileexist @CurrentDirectoryPath

          IF NOT EXISTS (SELECT 1 FROM @DirectoryInfo WHERE FileIsADirectory = 1)
          BEGIN
              SET @CurrentCommandType01 = 'xp_create_subdir'
              SET @CurrentCommand01 = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = [master].dbo.xp_create_subdir N''' + REPLACE(@CurrentDirectoryPath,'''','''''') + ''' IF @ReturnCode <> 0 RAISERROR(''Error creating directory.'', 16, 1)'
              EXECUTE @CurrentCommandOutput01 = [dbo].[CommandExecute] @Command = @CurrentCommand01, @CommandType = @CurrentCommandType01, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
              SET @Error = @@ERROR
              IF @Error <> 0 SET @CurrentCommandOutput01 = @Error
              IF @CurrentCommandOutput01 <> 0 SET @ReturnCode = @CurrentCommandOutput01
          END 
          ELSE
          BEGIN
             SET @CurrentCommandOutput01 = 0
          END;

          UPDATE @CurrentDirectories
          SET CreateCompleted = 1,
              CreateOutput = @CurrentCommandOutput01
          WHERE ID = @CurrentDirectoryID

          SET @CurrentDirectoryID = NULL
          SET @CurrentDirectoryPath = NULL

          SET @CurrentCommand01 = NULL

          SET @CurrentCommandOutput01 = NULL

          SET @CurrentCommandType01 = NULL
        END
      END

      -- set pending mirrors to ok; otherwise the following steps will fail
      UPDATE @CurrentDirectories 
         SET CreateCompleted = 0, CreateOutput = 0
       WHERE Mirror < 0 
      ;
      IF @CleanupMode = 'BEFORE_BACKUP'
      BEGIN
        INSERT INTO @CurrentCleanupDates (CleanupDate, Mirror)
        SELECT DATEADD(hh,-(@CleanupTime),GETDATE()), 0

        IF NOT EXISTS(SELECT * FROM @CurrentCleanupDates WHERE (Mirror = 0 OR Mirror IS NULL) AND CleanupDate IS NULL)
        BEGIN
          UPDATE @CurrentDirectories
          SET CleanupDate = (SELECT MIN(CleanupDate)
                             FROM @CurrentCleanupDates
                             WHERE (Mirror = 0 OR Mirror IS NULL)),
              CleanupMode = 'BEFORE_BACKUP'
          WHERE Mirror = 0
        END
      END

      SET @CurrentMirror = 1
      WHILE @CurrentMirror < 4
      BEGIN
          IF (@CurrentMirror = 1 AND @MirrorCleanupMode  = 'BEFORE_BACKUP')
          OR (@CurrentMirror = 2 AND @MirrorCleanupMode2 = 'BEFORE_BACKUP')
          OR (@CurrentMirror = 3 AND @MirrorCleanupMode3 = 'BEFORE_BACKUP')
          BEGIN
            INSERT INTO @CurrentCleanupDates (CleanupDate, Mirror)
            SELECT DATEADD(hh,-(CASE WHEN @CurrentMirror = 1 THEN @MirrorCleanupTime
                                     WHEN @CurrentMirror = 2 THEN @MirrorCleanupTime2
                                     WHEN @CurrentMirror = 3 THEN @MirrorCleanupTime3
                                END
                               ),GETDATE()), @CurrentMirror

            IF NOT EXISTS(SELECT * FROM @CurrentCleanupDates WHERE (Mirror = @CurrentMirror OR Mirror IS NULL) AND CleanupDate IS NULL)
            BEGIN
              UPDATE @CurrentDirectories
              SET CleanupDate = (SELECT MIN(CleanupDate)
                                 FROM @CurrentCleanupDates
                                 WHERE (Mirror IN (@CurrentMirror, @CurrentMirror * -1) OR Mirror IS NULL)),
                  CleanupMode = 'BEFORE_BACKUP'
              WHERE Mirror IN (@CurrentMirror, @CurrentMirror * -1)
            END
          END
          SET @CurrentMirror = @CurrentMirror + 1
      END

      -- Delete old backup files, before backup
      IF NOT EXISTS (SELECT * FROM @CurrentDirectories WHERE CreateOutput <> 0 OR CreateOutput IS NULL)
      AND @HostPlatform = 'Windows'
      AND (@BackupSoftware <> 'DATA_DOMAIN_BOOST' OR @BackupSoftware IS NULL)
      AND @CurrentBackupType = @BackupType
      AND CAST(@StartTime AS TIME) BETWEEN ISNULL(@CleanUpStartTime, CAST('00:00:00.000' AS TIME))  -- only when in the specified timeframe
                                       AND ISNULL(@CleanUpEndTime  , CAST('23:59:59.999' AS TIME))

      BEGIN
        WHILE (1 = 1)
        BEGIN
          SELECT TOP 1 @CurrentDirectoryID = ID,
                       @CurrentDirectoryPath = DirectoryPath,
                       @CurrentCleanupDate = CleanupDate
          FROM @CurrentDirectories
          WHERE CleanupDate IS NOT NULL
          AND CleanupMode = 'BEFORE_BACKUP'
          AND CleanupCompleted = 0
          ORDER BY ID ASC

          IF @@ROWCOUNT = 0
          BEGIN
            BREAK
          END

          IF @BackupSoftware IS NULL
          BEGIN
            SET @CurrentCommandType02 = 'xp_delete_file'

            IF @CleanupUsesCMD = 'N'
               SET @CurrentCommand02 = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = [master].dbo.xp_delete_file 0, N''' + REPLACE(@CurrentDirectoryPath,'''','''''') + ''', ''' + @CurrentFileExtension + ''', ''' + CONVERT(nvarchar(19),@CurrentCleanupDate,126) + ''' IF @ReturnCode <> 0 RAISERROR(''Error deleting files.'', 16, 1)'
            ELSE -- for compatibility reasons the CommandType will not be changed, when using cmdshell for cleanup
               BEGIN
                   -- get list of all backup files in the current folder (only for the current database)
                   SET @cmd = 'DIR /b /s /oN /a-d "'+ REPLACE(@CurrentDirectoryPath,N'''',N'''''') + '\*_' + @CurrentDatabaseName + '_*.' + @CurrentFileExtension  + '"'
                   PRINT @cmd
                   
                   DELETE FROM #CleanUpFiles WHERE 1 = 1

                   INSERT INTO #CleanUpFiles(FileWithPath)
                   EXEC master.sys.xp_cmdshell @cmd
                   ;
                   DELETE FROM #CleanUpFiles 
                    WHERE FileWithPath IS NULL                     -- there will always at least one empty line
                       OR LEN(CharBackupTime) <> 15                -- wrong formated timestamp (e.g. some manually created / renamed files)
                       OR ISNUMERIC(LEFT(CharBackupTime, 8))  = 0
                       OR ISNUMERIC(RIGHT(CharBackupTime, 6)) = 0
                   ;
                   UPDATE #CleanUpFiles
                      SET ShouldBeDeleted = 1
                    WHERE CharBackupTime < REPLACE(REPLACE(REPLACE(CONVERT(VARCHAR(50), @CurrentCleanupDate, 120), '-', ''), ' ', '_'), ':', '')
                   ;
                   
                   SET @Template = (SELECT TOP 1 REPLACE(cuf.FileWithPath, cuf.CharBackupTime, '*?*') template -- returns e.g. \\msdb01\z$\MSDB01\ProdDB\LOG\MSDB01_ProdDB_LOG_*?*.trn
                                      FROM #CleanUpFiles AS cuf)
                   ;
                   WITH groups AS -- Step 1: group the cleanup files by year / year + month / year + month + day / year + month + day + hour where ALL files should be deleted
                              (
                               SELECT LEFT(cuf.CharBackupTime, cte.endpos) grouppart, cte.endpos
                                 FROM #CleanUpFiles AS cuf
                                CROSS APPLY (VALUES (4), (6), (8), (11)) cte(endpos) -- end position of year, month, day and hour in CharBackupTime
                                GROUP BY LEFT(cuf.CharBackupTime, cte.endpos), cte.endpos
                                HAVING MIN(cuf.ShouldBeDeleted) = MAX(cuf.ShouldBeDeleted)
                                   AND MIN(cuf.ShouldBeDeleted) = 1
                              )
                   SELECT @CurrentCommand02 = 
                          N'DECLARE @ReturnCode int; '
                        + N'EXECUTE @ReturnCode = master.sys.xp_cmdshell '''
                        + STUFF((
                                 -- Step 2: limit to the top level group (e.g. month instead month + day + hour), if all files for this month should be deleted
                                SELECT ' & DEL "'+ REPLACE(@Template, '?', g2.grouppart) + '"' AS cmd -- -> e.g. DEL "\\msdb01\z$\MSDB01\ProdDB\LOG\MSDB01_ProdDB_LOG_*2015*.trn"
                                  FROM groups g
                                 RIGHT JOIN groups g2
                                    ON g2.grouppart LIKE g.grouppart + '%'
                                   AND g2.endpos > g.endpos
                                 WHERE g.grouppart IS NULL
                                   AND g2.grouppart IS NOT NULL
                                 ORDER BY g2.grouppart
                                   FOR XML PATH('i'), root('c'), type
                                ).query('/c/i').value('.', 'nvarchar(4000)')
                                , 1, 3, '' )
                        + N'''; '
                        + N'IF @ReturnCode <> 0 RAISERROR(''Error deleting files.'', 16, 1)'
                   ;
                   IF @CurrentCommand02 IS NULL SET @CurrentCommand05 = N'PRINT ''Cleanup: Nothing to do - no older backup files found''';
              END -- ELSE (@CleanupUsesCMD = 'Y')
          END

          IF @BackupSoftware = 'LITESPEED'
          BEGIN
            SET @CurrentCommandType02 = 'xp_slssqlmaint'

            SET @CurrentCommand02 = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = [master].dbo.xp_slssqlmaint N''-MAINTDEL -DELFOLDER "' + REPLACE(@CurrentDirectoryPath,'''','''''') + '" -DELEXTENSION "' + @CurrentFileExtension + '" -DELUNIT "' + CAST(DATEDIFF(mi,@CurrentCleanupDate,GETDATE()) + 1 AS nvarchar) + '" -DELUNITTYPE "minutes" -DELUSEAGE'' IF @ReturnCode <> 0 RAISERROR(''Error deleting LiteSpeed backup files.'', 16, 1)'
          END

          IF @BackupSoftware = 'SQLBACKUP'
          BEGIN
            SET @CurrentCommandType02 = 'sqbutility'

            SET @CurrentCommand02 = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = [master].dbo.sqbutility 1032, N''' + REPLACE(@CurrentDatabaseName,'''','''''') + ''', N''' + REPLACE(@CurrentDirectoryPath,'''','''''') + ''', ''' + CASE WHEN @CurrentBackupType = 'FULL' THEN 'D' WHEN @CurrentBackupType = 'DIFF' THEN 'I' WHEN @CurrentBackupType = 'LOG' THEN 'L' END + ''', ''' + CAST(DATEDIFF(hh,@CurrentCleanupDate,GETDATE()) + 1 AS nvarchar) + 'h'', ' + ISNULL('''' + REPLACE(@EncryptionKey,'''','''''') + '''','NULL') + ' IF @ReturnCode <> 0 RAISERROR(''Error deleting SQLBackup backup files.'', 16, 1)'
          END

          IF @BackupSoftware = 'SQLSAFE'
          BEGIN
            SET @CurrentCommandType02 = 'xp_ss_delete'

            SET @CurrentCommand02 = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = [master].dbo.xp_ss_delete @filename = N''' + REPLACE(@CurrentDirectoryPath,'''','''''') + '\*.' + @CurrentFileExtension + ''', @age = ''' + CAST(DATEDIFF(mi,@CurrentCleanupDate,GETDATE()) + 1 AS nvarchar) + 'Minutes'' IF @ReturnCode <> 0 RAISERROR(''Error deleting SQLsafe backup files.'', 16, 1)'
          END

          IF @CurrentMirror >= 0 -- do not execute for pending mirrors with (negative id); execute only if mirror is available and of course for the main @Directory
             EXECUTE @CurrentCommandOutput02 = [dbo].[CommandExecute] @Command = @CurrentCommand02, @CommandType = @CurrentCommandType02, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
          ELSE -- @CurrentMirror  < 0
          BEGIN
             -- pending mirrors: insert into CommandLog
             INSERT INTO dbo.CommandLog (DatabaseName, CommandType, IndexType, Command, ExtendedInfo,
                         StartTime, EndTime, ErrorNumber, ErrorMessage)
             VALUES (@CurrentDatabaseName, 'PENDING_CLEANUP_BEFORE', ABS(@CurrentMirror), @CurrentCommand02, CAST(@CurrentDirectoryPath AS XML),
                     GETDATE(),
                     DATEADD(HOUR, CASE ABS(@CurrentMirror)
                                        WHEN 1 THEN @MirrorCleanupTime
                                        WHEN 2 THEN @MirrorCleanupTime2
                                        WHEN 3 THEN @MirrorCleanupTime3
                                   END, GETDATE()),
                     CASE WHEN @Execute = 'Y' THEN -1 ELSE 0 END,
                     CASE WHEN @Execute = 'Y' THEN 'Not started' ELSE 'not executed because @Execute was set to N' END
                    )
             SET @CurrentCommandOutput02 = @@ERROR
          END 
          SET @Error = @@ERROR
          IF @Error <> 0 SET @CurrentCommandOutput02 = @Error
          IF @CurrentCommandOutput02 <> 0 SET @ReturnCode = @CurrentCommandOutput02

          UPDATE @CurrentDirectories
          SET CleanupCompleted = 1,
              CleanupOutput = @CurrentCommandOutput02
          WHERE ID = @CurrentDirectoryID

          SET @CurrentDirectoryID = NULL
          SET @CurrentDirectoryPath = NULL
          SET @CurrentCleanupDate = NULL

          SET @CurrentCommand02 = NULL

          SET @CurrentCommandOutput02 = NULL

          SET @CurrentCommandType02 = NULL
        END
      END

      -- Perform a backup
      IF NOT EXISTS (SELECT * FROM @CurrentDirectories WHERE CreateOutput <> 0 OR CreateOutput IS NULL) OR @HostPlatform = 'Linux'
      BEGIN
        IF @BackupSoftware IS NULL
        BEGIN
          SELECT @CurrentCommandType03 = CASE
          WHEN @CurrentBackupType IN('DIFF','FULL') THEN 'BACKUP_DATABASE'
          WHEN @CurrentBackupType = 'LOG' THEN 'BACKUP_LOG'
          END

          SELECT @CurrentCommand03 = CASE
          WHEN @CurrentBackupType IN('DIFF','FULL') THEN 'BACKUP DATABASE ' + QUOTENAME(@CurrentDatabaseName)
          WHEN @CurrentBackupType = 'LOG' THEN 'BACKUP LOG ' + QUOTENAME(@CurrentDatabaseName)
          END

          IF @ReadWriteFileGroups = 'Y' AND @CurrentDatabaseName <> 'master' SET @CurrentCommand03 = @CurrentCommand03 + ' READ_WRITE_FILEGROUPS'

          SET @CurrentCommand03 = @CurrentCommand03 + ' TO'

          SELECT @CurrentCommand03 = @CurrentCommand03 + ' ' + [Type] + ' = N''' + REPLACE(FilePath,'''','''''') + '''' + CASE WHEN ROW_NUMBER() OVER (ORDER BY FileNumber ASC) <> @NumberOfFiles THEN ',' ELSE '' END -- ORDER BY FileNumber instead of FilePath
          FROM @CurrentFiles
          WHERE Mirror = 0
          ORDER BY FileNumber ASC; -- changed from FilePath to FileNumber, otherwise it would possibly store the same part of a multi file backup on 
                                   -- the same mirror instead of alternating (e.g. @Directory = '\\srv1\z$,\\srv2\z$', @MirrorDirectory='\\srv2\y$,\\srv1\y$')

          SET @CurrentMirror = 1
          WHILE @CurrentMirror < 4 -- Mirrors 1-3
            AND SERVERPROPERTY('EngineEdition') = 3 -- MIRROR TO is an Enterprise-only feature, so check for Enterprise / Developer / Evaluation edition
            AND @MirrorType = 'DEFAULT'
          BEGIN
              IF EXISTS(SELECT * FROM @CurrentFiles WHERE Mirror = @CurrentMirror)
              BEGIN
                SET @CurrentCommand03 = @CurrentCommand03 + ' MIRROR TO'

                SELECT @CurrentCommand03 = @CurrentCommand03 + ' ' + [Type] + ' = N''' + REPLACE(FilePath,'''','''''') + '''' + CASE WHEN ROW_NUMBER() OVER (ORDER BY FileNumber ASC) <> @NumberOfFiles THEN ',' ELSE '' END
                  FROM @CurrentFiles
                 WHERE Mirror = @CurrentMirror
                 ORDER BY FileNumber ASC
              END
              SET @CurrentMirror = @CurrentMirror + 1
          END 

          SET @CurrentCommand03 = @CurrentCommand03 + ' WITH '
          IF @CheckSum = 'Y' SET @CurrentCommand03 = @CurrentCommand03 + 'CHECKSUM'
          IF @CheckSum = 'N' SET @CurrentCommand03 = @CurrentCommand03 + 'NO_CHECKSUM'

          -- added spaces and line breaks for a better readable output
          IF @Version >= 10 
          BEGIN
            SET @CurrentCommand03 = @CurrentCommand03 + CASE WHEN @Compress = 'Y' AND (@CurrentIsEncrypted = 0 OR (@CurrentIsEncrypted = 1 AND @Version >= 13 AND @MaxTransferSize > 65536)) THEN CHAR(13) + CHAR(10) + '         , COMPRESSION' ELSE CHAR(13) + CHAR(10) + '         , NO_COMPRESSION' END
          END
          IF @CurrentBackupType = 'DIFF' SET @CurrentCommand03 = @CurrentCommand03 + CHAR(13) + CHAR(10) + '         , DIFFERENTIAL'

          IF EXISTS(SELECT * FROM @CurrentFiles WHERE Mirror > 0)
          BEGIN
            SET @CurrentCommand03 = @CurrentCommand03 + CHAR(13) + CHAR(10) + '         , FORMAT'
          END

          IF @CopyOnly = 'Y' SET @CurrentCommand03 = @CurrentCommand03 + CHAR(13) + CHAR(10) + '         , COPY_ONLY'
          IF @NoRecovery = 'Y' AND @CurrentBackupType = 'LOG' SET @CurrentCommand03 = @CurrentCommand03 + ',' + CHAR(13) + CHAR(10) + '         , NORECOVERY'
          IF @Init = 'Y' SET @CurrentCommand03 = @CurrentCommand03 + CHAR(13) + CHAR(10) + '           , INIT'
          IF @BlockSize IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + CHAR(13) + CHAR(10) + '         , BLOCKSIZE = ' + CAST(@BlockSize AS nvarchar)
          IF @BufferCount IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + CHAR(13) + CHAR(10) + '         , BUFFERCOUNT = ' + CAST(@BufferCount AS nvarchar)
          IF @MaxTransferSize IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + CHAR(13) + CHAR(10) + '         , MAXTRANSFERSIZE = ' + CAST(@MaxTransferSize AS nvarchar)
          IF @Description IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + CHAR(13) + CHAR(10) + '         , DESCRIPTION = N''' + REPLACE(@Description,'''','''''') + ''''
          IF @Encrypt = 'Y' SET @CurrentCommand03 = @CurrentCommand03 + CHAR(13) + CHAR(10) + '         , ENCRYPTION (ALGORITHM = ' + UPPER(@EncryptionAlgorithm) + ', '
          IF @Encrypt = 'Y' AND @ServerCertificate IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + 'SERVER CERTIFICATE = ' + QUOTENAME(@ServerCertificate)
          IF @Encrypt = 'Y' AND @ServerAsymmetricKey IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + 'SERVER ASYMMETRIC KEY = ' + QUOTENAME(@ServerAsymmetricKey)
          IF @Encrypt = 'Y' SET @CurrentCommand03 = @CurrentCommand03 + ')'
          IF @URL IS NOT NULL AND @Credential IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03  + CHAR(13) + CHAR(10) + '          , CREDENTIAL = N''' + REPLACE(@Credential,'''','''''') + ''''
        END

        IF @BackupSoftware = 'LITESPEED'
        BEGIN
          SELECT @CurrentCommandType03 = CASE
          WHEN @CurrentBackupType IN('DIFF','FULL') THEN 'xp_backup_database'
          WHEN @CurrentBackupType = 'LOG' THEN 'xp_backup_log'
          END

          SELECT @CurrentCommand03 = CASE
          WHEN @CurrentBackupType IN('DIFF','FULL') THEN 'DECLARE @ReturnCode int EXECUTE @ReturnCode = [master].dbo.xp_backup_database @database = N''' + REPLACE(@CurrentDatabaseName,'''','''''') + ''''
          WHEN @CurrentBackupType = 'LOG' THEN 'DECLARE @ReturnCode int EXECUTE @ReturnCode = [master].dbo.xp_backup_log @database = N''' + REPLACE(@CurrentDatabaseName,'''','''''') + ''''
          END

          SELECT @CurrentCommand03 = @CurrentCommand03 + ', @filename = N''' + REPLACE(FilePath,'''','''''') + ''''
          FROM @CurrentFiles
          WHERE Mirror = 0
          ORDER BY FileNumber ASC

          SET @CurrentMirror = 1
          WHILE @CurrentMirror < 4 -- no check for Enterprise needed, because it will work with every SQL Edition in LiteSpeed (http://documents.software.dell.com/litespeed-for-sql-server/7.5/netvault-litespeed-for-sql-server-user-guide/back-up-databases/back-up-using-the-backup-wizard)
            AND @MirrorType = 'DEFAULT'
          BEGIN
              IF EXISTS(SELECT * FROM @CurrentFiles WHERE Mirror = @CurrentMirror)
              BEGIN
                SELECT @CurrentCommand03 = @CurrentCommand03 + ', @mirror = N''' + REPLACE(FilePath,'''','''''') + ''''
                FROM @CurrentFiles
                WHERE Mirror = @CurrentMirror
                ORDER BY FileNumber ASC
              END
              SET @CurrentMirror = @CurrentMirror + 1
          END

          SET @CurrentCommand03 = @CurrentCommand03 + ', @with = '''
          IF @CheckSum = 'Y' SET @CurrentCommand03 = @CurrentCommand03 + 'CHECKSUM'
          IF @CheckSum = 'N' SET @CurrentCommand03 = @CurrentCommand03 + 'NO_CHECKSUM'
          IF @CurrentBackupType = 'DIFF' SET @CurrentCommand03 = @CurrentCommand03 + ', DIFFERENTIAL'
          IF @CopyOnly = 'Y' SET @CurrentCommand03 = @CurrentCommand03 + ', COPY_ONLY'
          IF @NoRecovery = 'Y' AND @CurrentBackupType = 'LOG' SET @CurrentCommand03 = @CurrentCommand03 + ', NORECOVERY'
          IF @BlockSize IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', BLOCKSIZE = ' + CAST(@BlockSize AS nvarchar)
          SET @CurrentCommand03 = @CurrentCommand03 + ''''
          IF @ReadWriteFileGroups = 'Y' AND @CurrentDatabaseName <> 'master' SET @CurrentCommand03 = @CurrentCommand03 + ', @read_write_filegroups = 1'
          IF @CompressionLevel IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', @compressionlevel = ' + CAST(@CompressionLevel AS nvarchar)
          IF @AdaptiveCompression IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', @adaptivecompression = ''' + CASE WHEN @AdaptiveCompression = 'SIZE' THEN 'Size' WHEN @AdaptiveCompression = 'SPEED' THEN 'Speed' END + ''''
          IF @BufferCount IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', @buffercount = ' + CAST(@BufferCount AS nvarchar)
          IF @MaxTransferSize IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', @maxtransfersize = ' + CAST(@MaxTransferSize AS nvarchar)
          IF @Threads IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', @threads = ' + CAST(@Threads AS nvarchar)
          IF @Init = 'Y' SET @CurrentCommand03 = @CurrentCommand03 + ', @init = 1'
          IF @Throttle IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', @throttle = ' + CAST(@Throttle AS nvarchar)
          IF @Description IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', @desc = N''' + REPLACE(@Description,'''','''''') + ''''

          IF @EncryptionAlgorithm IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', @cryptlevel = ' + CASE
          WHEN @EncryptionAlgorithm = 'RC2_40' THEN '0'
          WHEN @EncryptionAlgorithm = 'RC2_56' THEN '1'
          WHEN @EncryptionAlgorithm = 'RC2_112' THEN '2'
          WHEN @EncryptionAlgorithm = 'RC2_128' THEN '3'
          WHEN @EncryptionAlgorithm = 'TRIPLE_DES_3KEY' THEN '4'
          WHEN @EncryptionAlgorithm = 'RC4_128' THEN '5'
          WHEN @EncryptionAlgorithm = 'AES_128' THEN '6'
          WHEN @EncryptionAlgorithm = 'AES_192' THEN '7'
          WHEN @EncryptionAlgorithm = 'AES_256' THEN '8'
          END

          IF @EncryptionKey IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', @encryptionkey = N''' + REPLACE(@EncryptionKey,'''','''''') + ''''
          SET @CurrentCommand03 = @CurrentCommand03 + ' IF @ReturnCode <> 0 RAISERROR(''Error performing LiteSpeed backup.'', 16, 1)'
        END

        IF @BackupSoftware = 'SQLBACKUP'
        BEGIN
          SET @CurrentCommandType03 = 'sqlbackup'

          SELECT @CurrentCommand03 = CASE
          WHEN @CurrentBackupType IN('DIFF','FULL') THEN 'BACKUP DATABASE ' + QUOTENAME(@CurrentDatabaseName)
          WHEN @CurrentBackupType = 'LOG' THEN 'BACKUP LOG ' + QUOTENAME(@CurrentDatabaseName)
          END

          IF @ReadWriteFileGroups = 'Y' AND @CurrentDatabaseName <> 'master' SET @CurrentCommand03 = @CurrentCommand03 + ' READ_WRITE_FILEGROUPS'

          SET @CurrentCommand03 = @CurrentCommand03 + ' TO'

          SELECT @CurrentCommand03 = @CurrentCommand03 + ' DISK = N''' + REPLACE(FilePath,'''','''''') + '''' + CASE WHEN ROW_NUMBER() OVER (ORDER BY FileNumber ASC) <> @NumberOfFiles THEN ',' ELSE '' END
          FROM @CurrentFiles
          WHERE Mirror = 0
          ORDER BY FileNumber ASC

          SET @CurrentCommand03 = @CurrentCommand03 + ' WITH '
          -- WHILE loop for mirrors (Redgate SQL Backup support up to 32 mirrors, but you can specify only 3 as parameter in this script; no check for Enterprise necessary (works for every edition))
          SET @CurrentMirror = 1
          WHILE @CurrentMirror < 4
            AND @MirrorType = 'DEFAULT'
          BEGIN
              IF EXISTS(SELECT * FROM @CurrentFiles WHERE Mirror = @CurrentMirror)
              BEGIN
                SET @CurrentCommand03 = @CurrentCommand03 + ISNULL( ' MIRRORFILE' + ' = N''' + REPLACE((SELECT FilePath FROM @CurrentFiles WHERE Mirror = @CurrentMirror),'''','''''') + ''', ', '')
              END
              SET @CurrentMirror = @CurrentMirror + 1
          END

          IF @CheckSum = 'Y' SET @CurrentCommand03 = @CurrentCommand03 + 'CHECKSUM'
          IF @CheckSum = 'N' SET @CurrentCommand03 = @CurrentCommand03 + 'NO_CHECKSUM'
          IF @CurrentBackupType = 'DIFF' SET @CurrentCommand03 = @CurrentCommand03 + ', DIFFERENTIAL'
          IF @CopyOnly = 'Y' SET @CurrentCommand03 = @CurrentCommand03 + ', COPY_ONLY'
          IF @NoRecovery = 'Y' AND @CurrentBackupType = 'LOG' SET @CurrentCommand03 = @CurrentCommand03 + ', NORECOVERY'
          IF @Init = 'Y' SET @CurrentCommand03 = @CurrentCommand03 + ', INIT'
          IF @CompressionLevel IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', COMPRESSION = ' + CAST(@CompressionLevel AS nvarchar)
          IF @Threads IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', THREADCOUNT = ' + CAST(@Threads AS nvarchar)
          IF @MaxTransferSize IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', MAXTRANSFERSIZE = ' + CAST(@MaxTransferSize AS nvarchar)
          IF @Description IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', DESCRIPTION = N''' + REPLACE(@Description,'''','''''') + ''''

          IF @EncryptionAlgorithm IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', KEYSIZE = ' + CASE
          WHEN @EncryptionAlgorithm = 'AES_128' THEN '128'
          WHEN @EncryptionAlgorithm = 'AES_256' THEN '256'
          END

          IF @EncryptionKey IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', PASSWORD = N''' + REPLACE(@EncryptionKey,'''','''''') + ''''
          SET @CurrentCommand03 = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = [master].dbo.sqlbackup N''-SQL "' + REPLACE(@CurrentCommand03,'''','''''') + '"''' + ' IF @ReturnCode <> 0 RAISERROR(''Error performing SQLBackup backup.'', 16, 1)'
        END

        IF @BackupSoftware = 'SQLSAFE'
        BEGIN
          SET @CurrentCommandType03 = 'xp_ss_backup'

          SET @CurrentCommand03 = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = [master].dbo.xp_ss_backup @database = N''' + REPLACE(@CurrentDatabaseName,'''','''''') + ''''

          SELECT @CurrentCommand03 = @CurrentCommand03 + ', ' + CASE WHEN ROW_NUMBER() OVER (ORDER BY FileNumber ASC) = 1 THEN '@filename' ELSE '@backupfile' END + ' = N''' + REPLACE(FilePath,'''','''''') + ''''
          FROM @CurrentFiles
          WHERE Mirror = 0
          ORDER BY FileNumber ASC

          -- Idera SQL Safe supports only two mirror targets (http://community.idera.com/forums/topic/backup-mirroring/)
          SET @CurrentMirror = 1
          WHILE @CurrentMirror < 3
            AND @MirrorType = 'DEFAULT'
          BEGIN
              SELECT @CurrentCommand03 = @CurrentCommand03 + ', @mirrorfile = N''' + REPLACE(FilePath,'''','''''') + ''''
                FROM @CurrentFiles
               WHERE Mirror = @CurrentMirror
               ORDER BY FileNumber ASC;
              SET @CurrentMirror = @CurrentMirror + 1
          END

          SET @CurrentCommand03 = @CurrentCommand03 + ', @backuptype = ' + CASE WHEN @CurrentBackupType = 'FULL' THEN '''Full''' WHEN @CurrentBackupType = 'DIFF' THEN '''Differential''' WHEN @CurrentBackupType = 'LOG' THEN '''Log''' END
          IF @ReadWriteFileGroups = 'Y' AND @CurrentDatabaseName <> 'master' SET @CurrentCommand03 = @CurrentCommand03 + ', @readwritefilegroups = 1'
          SET @CurrentCommand03 = @CurrentCommand03 + ', @checksum = ' + CASE WHEN @CheckSum = 'Y' THEN '1' WHEN @CheckSum = 'N' THEN '0' END
          SET @CurrentCommand03 = @CurrentCommand03 + ', @copyonly = ' + CASE WHEN @CopyOnly = 'Y' THEN '1' WHEN @CopyOnly = 'N' THEN '0' END
          IF @CompressionLevel IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', @compressionlevel = ' + CAST(@CompressionLevel AS nvarchar)
          IF @Threads IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', @threads = ' + CAST(@Threads AS nvarchar)
          IF @Init = 'Y' SET @CurrentCommand03 = @CurrentCommand03 + ', @overwrite = 1'
          IF @Description IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', @desc = N''' + REPLACE(@Description,'''','''''') + ''''

          IF @EncryptionAlgorithm IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', @encryptiontype = N''' + CASE
          WHEN @EncryptionAlgorithm = 'AES_128' THEN 'AES128'
          WHEN @EncryptionAlgorithm = 'AES_256' THEN 'AES256'
          END + ''''

          IF @EncryptionKey IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ', @encryptedbackuppassword = N''' + REPLACE(@EncryptionKey,'''','''''') + ''''
          SET @CurrentCommand03 = @CurrentCommand03 + ' IF @ReturnCode <> 0 RAISERROR(''Error performing SQLsafe backup.'', 16, 1)'
        END

        IF @BackupSoftware = 'DATA_DOMAIN_BOOST'
        BEGIN
          SET @CurrentCommandType03 = 'emc_run_backup'

          SET @CurrentCommand03 = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = [master].dbo.emc_run_backup '''

          SET @CurrentCommand03 = @CurrentCommand03 + ' -c ' + CASE WHEN @CurrentAvailabilityGroup IS NOT NULL THEN @Cluster ELSE CAST(SERVERPROPERTY('MachineName') AS nvarchar) END

          SET @CurrentCommand03 = @CurrentCommand03 + ' -l ' + CASE
          WHEN @CurrentBackupType = 'FULL' THEN 'full'
          WHEN @CurrentBackupType = 'DIFF' THEN 'diff'
          WHEN @CurrentBackupType = 'LOG' THEN 'incr'
          END

          IF @NoRecovery = 'Y' SET @CurrentCommand03 = @CurrentCommand03 + ' -H'

          IF @CleanupTime IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ' -y +' + CAST(@CleanupTime/24 + CASE WHEN @CleanupTime%24 > 0 THEN 1 ELSE 0 END AS nvarchar) + 'd'

          IF @CheckSum = 'Y' SET @CurrentCommand03 = @CurrentCommand03 + ' -k'

          SET @CurrentCommand03 = @CurrentCommand03 + ' -S ' + CAST(@NumberOfFiles AS nvarchar)

          IF @Description IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ' -b "' + REPLACE(@Description,'''','''''') + '"'

          IF @BufferCount IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ' -O "BUFFERCOUNT=' + CAST(@BufferCount AS nvarchar) + '"'

          IF @ReadWriteFileGroups = 'Y' AND @CurrentDatabaseName <> 'master' SET @CurrentCommand03 = @CurrentCommand03 + ' -O "READ_WRITE_FILEGROUPS"'

          SET @CurrentCommand03 = @CurrentCommand03 + ' -a "NSR_DFA_SI=TRUE"'
          SET @CurrentCommand03 = @CurrentCommand03 + ' -a "NSR_DFA_SI_USE_DD=TRUE"'
          IF @DataDomainBoostHost IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ' -a "NSR_DFA_SI_DD_HOST=' + REPLACE(@DataDomainBoostHost,'''','''''') + '"'
          IF @DataDomainBoostUser IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ' -a "NSR_DFA_SI_DD_USER=' + REPLACE(@DataDomainBoostUser,'''','''''') + '"'
          IF @DataDomainBoostDevicePath IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ' -a "NSR_DFA_SI_DEVICE_PATH=' + REPLACE(@DataDomainBoostDevicePath,'''','''''') + '"'
          IF @DataDomainBoostLockboxPath IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ' -a "NSR_DFA_SI_DD_LOCKBOX_PATH=' + REPLACE(@DataDomainBoostLockboxPath,'''','''''') + '"'
          SET @CurrentCommand03 = @CurrentCommand03 + ' -a "NSR_SKIP_NON_BACKUPABLE_STATE_DB=TRUE"'
          IF @CopyOnly = 'Y' SET @CurrentCommand03 = @CurrentCommand03 + ' -a "NSR_COPY_ONLY=TRUE"'

          IF SERVERPROPERTY('InstanceName') IS NULL SET @CurrentCommand03 = @CurrentCommand03 + ' "MSSQL' + ':' + REPLACE(REPLACE(@CurrentDatabaseName,'''',''''''),'.','\.') + '"'
          IF SERVERPROPERTY('InstanceName') IS NOT NULL SET @CurrentCommand03 = @CurrentCommand03 + ' "MSSQL$' + CAST(SERVERPROPERTY('InstanceName') AS nvarchar) + ':' + REPLACE(REPLACE(@CurrentDatabaseName,'''',''''''),'.','\.') + '"'

          SET @CurrentCommand03 = @CurrentCommand03 + ''''

          SET @CurrentCommand03 = @CurrentCommand03 + ' IF @ReturnCode <> 0 RAISERROR(''Error performing Data Domain Boost backup.'', 16, 1)'
        END

        EXECUTE @CurrentCommandOutput03 = [dbo].[CommandExecute] @Command = @CurrentCommand03, @CommandType = @CurrentCommandType03, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
        SET @Error = @@ERROR
        IF @Error <> 0 SET @CurrentCommandOutput03 = @Error
        IF @CurrentCommandOutput03 <> 0 SET @ReturnCode = @CurrentCommandOutput03
      END

      --Fake-Mirroring for native SQL Backups on non-Enterprise servers (or special @MirrorType) by copying the backup files
      IF @CurrentCommandOutput03 = 0            -- backup was ok
      AND (    @MirrorType           IN ('COPY', 'COPY_LATER') 
            OR EXISTS (SELECT 1 FROM @CurrentFiles WHERE Mirror < 0)) -- a mirror directory is not available and @MirrorWhenNotAvailable is set to COPY_LATER
      AND (@MirrorDirectory IS NOT NULL OR @MirrorDirectory2 IS NOT NULL OR @MirrorDirectory3 IS NOT NULL)
      BEGIN
          SET @CurrentCommandType09 = 'MIRROR-COPY'
          DECLARE CopyCur CURSOR LOCAL FORWARD_ONLY STATIC READ_ONLY FOR
                  WITH source AS (SELECT FileNumber,
                                          LEFT(c.filepath, calc.pos_backslash) FilePath,
                                          SUBSTRING(c.filepath, pos_backslash + 2, 4000) FileName,
                                          FilePath AS FullFilePath
                                    FROM @CurrentFiles c
                                    CROSS APPLY (SELECT LEN(c.FilePath) - CHARINDEX('\', REVERSE(c.FilePath)) pos_backslash) calc
                                   WHERE Mirror = 0),
                       dest   AS  (SELECT FileNumber,
                                          LEFT(c.filepath, calc.pos_backslash) FilePath,
                                          SUBSTRING(c.filepath, pos_backslash + 2, 4000) FileName,
                                          FilePath AS FullFilePath,
                                          Mirror
                                    FROM @CurrentFiles c
                                    CROSS APPLY (SELECT LEN(c.FilePath) - CHARINDEX('\', REVERSE(c.FilePath)) pos_backslash) calc
                                   WHERE (Mirror > 0 AND @MirrorType IN ('COPY', 'COPY_LATER')) -- Mirror 1 to 3  = regular mirrors for COPY;
                                      OR  Mirror < 0   -- Mirror -1 to -3 = COPY_LATER-Mirrors (because of @MirrorType or Server not availabe)
                                  )
                  SELECT 'DECLARE @cmd NVARCHAR(4000);' + CHAR(13) + CHAR(10)
                       + 'DECLARE @ReturnCode int' + CHAR(13) + CHAR(10)
                       + 'SET @cmd = ''' + CASE WHEN @MirrorCopyCommand = 'ROBOCOPY'
                                                -- syntax: ROBOCOPY /R:3 /W:30 /NP "z:\SRV01\test\FULL\" "\\MyNAS\SRV01\test\FULL\" "SRV01_test_FULL_20170222_135724.bak"
                                                -- do not use /Z (use a mode that allows restart), since it is VERY slow (30 MB/s vs. > 600 MB/s). Its only benefit would be, 
                                                -- that it does not need copy the whole 100 GB backup again, when it was canceled at 95%
                                                -- do not use /MT = use multiple threads, since it would be slower on big files (only ~half speed)
                                                -- /NP = no percentage (would be look ridiculus in log files / select output
                                                -- /R:3 /W:30 = up to 3 retrys after 30 seconds
                                                THEN 'ROBOCOPY /R:3 /W:30 /NP "'
                                                   + REPLACE(source.FilePath, '''', '''''') + '" "' 
                                                   + REPLACE(dest.FilePath, '''', '''''') + '" "'
                                                   + REPLACE(source.FileName, '''', '''''') + '"''' + CHAR(13) + CHAR(10)
                                                WHEN @MirrorCopyCommand = 'XCOPY'
                                                THEN 'echo ' + @XCOPYFileLetter + ' | xcopy /Y /Z ' -- echo command necessary because xcopy always asks if the target is a file or a directory
                                                   + REPLACE(source.FullFilePath, '''', '''''') + '" "' 
                                                   + REPLACE(dest.FullFilePath, '''', '''''') + '"''' + CHAR(13) + CHAR(10)
                                                ELSE 'COPY /Y /Z' 
                                                   + REPLACE(source.FullFilePath, '''', '''''') + '" "' 
                                                   + REPLACE(dest.FullFilePath, '''', '''''') + '"''' + CHAR(13) + CHAR(10)
                                           END 
                       + 'EXEC @ReturnCode = xp_cmdshell @cmd' + CHAR(13) + CHAR(10)
                       + 'IF @ReturnCode <> ' + CASE WHEN @MirrorCopyCommand = 'ROBOCOPY' THEN '1' ELSE '0' END 
                       +    ' RAISERROR(''Error manually copying the backup file "' +source.FullFilePath + '" to the mirror path "' + dest.FilePath + '"'', 16, 1)'
                             AS cmd,
                         dest.Mirror,
                         (SELECT source.FullFilePath source, dest.FullFilePath target
                             FOR XML PATH('Files'), root('PendingMirrorCopy'), type
                         ) ExtendedInfo
                    FROM dest
                   INNER JOIN source
                      ON source.FileNumber = dest.FileNumber
                   ORDER BY dest.Mirror, dest.FileNumber
          OPEN CopyCur

          WHILE 1 = 1
          BEGIN
              FETCH NEXT FROM CopyCur INTO @CurrentCommand09, @CurrentMirror, @CurrentExtendedInfo
              IF @@fetch_status <> 0 BREAK
          
              if @CurrentMirror > 0
                 BEGIN -- execute
                   EXECUTE @CurrentCommandOutput09 = [dbo].[CommandExecute] @Command = @CurrentCommand09, @CommandType = @CurrentCommandType09, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
                   SET @Error = @@ERROR
                   IF @Error <> 0 SET @CurrentCommandOutput09 = @Error
                   IF @CurrentCommandOutput09 <> 0 SET @ReturnCode = @CurrentCommandOutput09
                 END
              
              IF @CurrentMirror < 0 -- COPY_LATER-Mirror targets -> save to write it later to the master.dbo.CommandLog
              BEGIN
                 INSERT INTO dbo.CommandLog (DatabaseName, CommandType, IndexType, Command, ExtendedInfo,
                             StartTime, EndTime, ErrorNumber, ErrorMessage)
                 VALUES (@CurrentDatabaseName, 'PENDING_COPY', ABS(@CurrentMirror), @CurrentCommand09,  @CurrentExtendedInfo,
                         GETDATE(),
                         DATEADD(HOUR, CASE ABS(@CurrentMirror)
                                            WHEN 1 THEN @MirrorCleanupTime
                                            WHEN 2 THEN @MirrorCleanupTime2
                                            WHEN 3 THEN @MirrorCleanupTime3
                                       END, GETDATE()),
                         CASE WHEN @Execute = 'Y' THEN -1 ELSE 0 END,
                         CASE WHEN @Execute = 'Y' THEN 'Not started' ELSE 'not executed because @Execute was set to N' END
                        )
              END
          END
          
          CLOSE CopyCur
          DEALLOCATE CopyCur
      END

      -- Verify the backup
      IF @Verify = 'SKIP_MIRRORS'
      BEGIN -- set the Mirror-Backups to already verified 
          UPDATE @CurrentBackupSet SET VerifyCompleted = 1 WHERE Mirror <> 0
          SET @Verify = 'Y'
      END
        
      IF @CurrentCommandOutput03 = 0 AND @Verify = 'Y'
      BEGIN
        WHILE (1 = 1)
        BEGIN
          SELECT TOP 1 @CurrentBackupSetID = ID,
                       @CurrentMirror = Mirror
          FROM @CurrentBackupSet
          WHERE VerifyCompleted = 0
          ORDER BY ID ASC

          IF @@ROWCOUNT = 0
          BEGIN
            BREAK
          END

          IF @BackupSoftware IS NULL
          BEGIN
            SET @CurrentCommandType04 = 'RESTORE_VERIFYONLY'

            SET @CurrentCommand04 = 'RESTORE VERIFYONLY FROM'

            SELECT @CurrentCommand04 = @CurrentCommand04 + ' ' + [Type] + ' = N''' + REPLACE(FilePath,'''','''''') + '''' + CASE WHEN ROW_NUMBER() OVER (ORDER BY FilePath ASC) <> @NumberOfFiles THEN ',' ELSE '' END
            FROM @CurrentFiles
            WHERE Mirror = @CurrentMirror
            ORDER BY FilePath ASC

            SET @CurrentCommand04 = @CurrentCommand04 + ' WITH '
            IF @CheckSum = 'Y' SET @CurrentCommand04 = @CurrentCommand04 + 'CHECKSUM'
            IF @CheckSum = 'N' SET @CurrentCommand04 = @CurrentCommand04 + 'NO_CHECKSUM'
            IF @URL IS NOT NULL AND @Credential IS NOT NULL SET @CurrentCommand04 = @CurrentCommand04 + ', CREDENTIAL = N''' + REPLACE(@Credential,'''','''''') + ''''
          END

          IF @BackupSoftware = 'LITESPEED'
          BEGIN
            SET @CurrentCommandType04 = 'xp_restore_verifyonly'

            SET @CurrentCommand04 = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = [master].dbo.xp_restore_verifyonly'

            SELECT @CurrentCommand04 = @CurrentCommand04 + ' @filename = N''' + REPLACE(FilePath,'''','''''') + '''' + CASE WHEN ROW_NUMBER() OVER (ORDER BY FilePath ASC) <> @NumberOfFiles THEN ',' ELSE '' END
            FROM @CurrentFiles
            WHERE Mirror = @CurrentMirror
            ORDER BY FilePath ASC

            SET @CurrentCommand04 = @CurrentCommand04 + ', @with = '''
            IF @CheckSum = 'Y' SET @CurrentCommand04 = @CurrentCommand04 + 'CHECKSUM'
            IF @CheckSum = 'N' SET @CurrentCommand04 = @CurrentCommand04 + 'NO_CHECKSUM'
            SET @CurrentCommand04 = @CurrentCommand04 + ''''
            IF @EncryptionKey IS NOT NULL SET @CurrentCommand04 = @CurrentCommand04 + ', @encryptionkey = N''' + REPLACE(@EncryptionKey,'''','''''') + ''''

            SET @CurrentCommand04 = @CurrentCommand04 + ' IF @ReturnCode <> 0 RAISERROR(''Error verifying LiteSpeed backup.'', 16, 1)'
          END

          IF @BackupSoftware = 'SQLBACKUP'
          BEGIN
            SET @CurrentCommandType04 = 'sqlbackup'

            SET @CurrentCommand04 = 'RESTORE VERIFYONLY FROM'

            SELECT @CurrentCommand04 = @CurrentCommand04 + ' DISK = N''' + REPLACE(FilePath,'''','''''') + '''' + CASE WHEN ROW_NUMBER() OVER (ORDER BY FilePath ASC) <> @NumberOfFiles THEN ',' ELSE '' END
            FROM @CurrentFiles
            WHERE Mirror = @CurrentMirror
            ORDER BY FilePath ASC

            SET @CurrentCommand04 = @CurrentCommand04 + ' WITH '
            IF @CheckSum = 'Y' SET @CurrentCommand04 = @CurrentCommand04 + 'CHECKSUM'
            IF @CheckSum = 'N' SET @CurrentCommand04 = @CurrentCommand04 + 'NO_CHECKSUM'
            IF @EncryptionKey IS NOT NULL SET @CurrentCommand04 = @CurrentCommand04 + ', PASSWORD = N''' + REPLACE(@EncryptionKey,'''','''''') + ''''

            SET @CurrentCommand04 = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = [master].dbo.sqlbackup N''-SQL "' + REPLACE(@CurrentCommand04,'''','''''') + '"''' + ' IF @ReturnCode <> 0 RAISERROR(''Error verifying SQLBackup backup.'', 16, 1)'
          END

          IF @BackupSoftware = 'SQLSAFE'
          BEGIN
            SET @CurrentCommandType04 = 'xp_ss_verify'

            SET @CurrentCommand04 = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = [master].dbo.xp_ss_verify @database = N''' + REPLACE(@CurrentDatabaseName,'''','''''') + ''''

            SELECT @CurrentCommand04 = @CurrentCommand04 + ', ' + CASE WHEN ROW_NUMBER() OVER (ORDER BY FilePath ASC) = 1 THEN '@filename' ELSE '@backupfile' END + ' = N''' + REPLACE(FilePath,'''','''''') + ''''
            FROM @CurrentFiles
            WHERE Mirror = @CurrentMirror
            ORDER BY FilePath ASC

            SET @CurrentCommand04 = @CurrentCommand04 + ' IF @ReturnCode <> 0 RAISERROR(''Error verifying SQLsafe backup.'', 16, 1)'
          END

          IF @CurrentMirror >= 0
              EXECUTE @CurrentCommandOutput04 = [dbo].[CommandExecute] @Command = @CurrentCommand04, @CommandType = @CurrentCommandType04, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
          ELSE  -- @CurrentMirror  < 0
          BEGIN -- pending mirrors: insert into CommandLog
             INSERT INTO dbo.CommandLog (DatabaseName, CommandType, IndexType, Command, ExtendedInfo,
                         StartTime, EndTime, ErrorNumber, ErrorMessage)
             VALUES (@CurrentDatabaseName, 'PENDING_VERIFY', ABS(@CurrentMirror), @CurrentCommand04,
                     (SELECT @NumberOfFiles NumberOfFiles,  -- List of all target files for this mirror
                             cf.Mirror,                     -- necessary, because all files has to be present before VERIFY can run
                             cf.FilePath,
                             cf.FileNumber
                        FROM @CurrentFiles AS cf
                       WHERE cf.Mirror = @CurrentMirror
                       ORDER BY cf.FileNumber
                         FOR XML PATH('Files'), root('TargetFiles'), type
                      ),
                     GETDATE(),
                     DATEADD(HOUR, CASE ABS(@CurrentMirror)
                                        WHEN 1 THEN @MirrorCleanupTime
                                        WHEN 2 THEN @MirrorCleanupTime2
                                        WHEN 3 THEN @MirrorCleanupTime3
                                   END, GETDATE()),
                     CASE WHEN @Execute = 'Y' THEN -1 ELSE 0 END,
                     CASE WHEN @Execute = 'Y' THEN 'Not started' ELSE 'not executed because @Execute was set to N' END
                    )
             SET @CurrentCommandOutput04 = @@ERROR
          END 

          SET @Error = @@ERROR
          IF @Error <> 0 SET @CurrentCommandOutput04 = @Error
          IF @CurrentCommandOutput04 <> 0 SET @ReturnCode = @CurrentCommandOutput04

          UPDATE @CurrentBackupSet
          SET VerifyCompleted = 1,
              VerifyOutput = @CurrentCommandOutput04
          WHERE ID = @CurrentBackupSetID

          SET @CurrentBackupSetID = NULL
          SET @CurrentMirror = NULL

          SET @CurrentCommand04 = NULL

          SET @CurrentCommandOutput04 = NULL

          SET @CurrentCommandType04 = NULL
        END
      END

      IF @CleanupMode = 'AFTER_BACKUP'
      BEGIN
        INSERT INTO @CurrentCleanupDates (CleanupDate, Mirror)
        SELECT DATEADD(hh,-(@CleanupTime),GETDATE()), 0

        IF NOT EXISTS(SELECT * FROM @CurrentCleanupDates WHERE (Mirror = 0 OR Mirror IS NULL) AND CleanupDate IS NULL)
        BEGIN
          UPDATE @CurrentDirectories
          SET CleanupDate = (SELECT MIN(CleanupDate)
                             FROM @CurrentCleanupDates
                             WHERE (Mirror = 0 OR Mirror IS NULL)),
              CleanupMode = 'AFTER_BACKUP'
          WHERE Mirror = 0
        END
      END

      SET @CurrentMirror = 1
      WHILE @CurrentMirror < 4
      BEGIN
          IF (@CurrentMirror = 1 AND @MirrorCleanupMode  = 'AFTER_BACKUP')
          OR (@CurrentMirror = 2 AND @MirrorCleanupMode2 = 'AFTER_BACKUP')
          OR (@CurrentMirror = 3 AND @MirrorCleanupMode3 = 'AFTER_BACKUP')
          BEGIN
            INSERT INTO @CurrentCleanupDates (CleanupDate, Mirror)
            SELECT DATEADD(HOUR, -(CASE WHEN @CurrentMirror = 1 THEN @MirrorCleanupTime
                                        WHEN @CurrentMirror = 2 THEN @MirrorCleanupTime2
                                        WHEN @CurrentMirror = 3 THEN @MirrorCleanupTime3
                                   END
                                  ),GETDATE()), @CurrentMirror

            IF NOT EXISTS(SELECT * FROM @CurrentCleanupDates WHERE (Mirror = @CurrentMirror OR Mirror IS NULL) AND CleanupDate IS NULL)
            BEGIN
              UPDATE @CurrentDirectories
                 SET CleanupDate = (SELECT MIN(CleanupDate)
                                      FROM @CurrentCleanupDates
                                     WHERE (Mirror IN (@CurrentMirror, @CurrentMirror * -1) OR Mirror IS NULL)),
                     CleanupMode = 'AFTER_BACKUP'
              WHERE Mirror IN (@CurrentMirror, @CurrentMirror * -1)
            END
          END
          SET @CurrentMirror = @CurrentMirror + 1
      END

      -- Delete old backup files, after backup
      IF ((@CurrentCommandOutput03 = 0 AND @Verify = 'N')
      OR (@CurrentCommandOutput03 = 0 AND @Verify = 'Y' AND NOT EXISTS (SELECT * FROM @CurrentBackupSet WHERE VerifyOutput <> 0 OR VerifyOutput IS NULL)))
      AND @HostPlatform = 'Windows'
      AND (@BackupSoftware <> 'DATA_DOMAIN_BOOST' OR @BackupSoftware IS NULL)
      AND @CurrentBackupType = @BackupType
      AND CAST(@StartTime AS TIME) BETWEEN ISNULL(@CleanUpStartTime, CAST('00:00:00.000' AS TIME))  -- only when in the specified timeframe
                                       AND ISNULL(@CleanUpEndTime  , CAST('23:59:59.999' AS TIME))
      BEGIN
        WHILE (1 = 1)
        BEGIN
          SELECT TOP 1 @CurrentDirectoryID = ID,
                       @CurrentDirectoryPath = DirectoryPath,
                       @CurrentCleanupDate = CleanupDate,
                       @CurrentMirror       = Mirror
          FROM @CurrentDirectories
          WHERE CleanupDate IS NOT NULL
          AND CleanupMode = 'AFTER_BACKUP'
          AND CleanupCompleted = 0
          ORDER BY ID ASC

          IF @@ROWCOUNT = 0
          BEGIN
            BREAK
          END

          IF @BackupSoftware IS NULL
          BEGIN
            SET @CurrentCommandType05 = 'xp_delete_file'

            IF @CleanupUsesCMD = 'N' 
               SET @CurrentCommand05 = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = [master].dbo.xp_delete_file 0, N''' + REPLACE(@CurrentDirectoryPath,'''','''''') + ''', ''' + @CurrentFileExtension + ''', ''' + CONVERT(nvarchar(19),@CurrentCleanupDate,126) + ''' IF @ReturnCode <> 0 RAISERROR(''Error deleting files.'', 16, 1)'
            ELSE -- ELSE added; for compatibility reasons the CommandType will not be changed
               BEGIN
                   -- get list of all backup files in the current folder (only for the current database)
                   SET @cmd = 'DIR /b /s /oN /a-d "'+ REPLACE(@CurrentDirectoryPath,N'''',N'''''') + '\*_' + @CurrentDatabaseName + '_*.' + @CurrentFileExtension  + '"'
                   PRINT @cmd
                   
                   DELETE FROM #CleanUpFiles WHERE 1 = 1

                   INSERT INTO #CleanUpFiles(FileWithPath)
                   EXEC master.sys.xp_cmdshell @cmd
                   ;
                   DELETE FROM #CleanUpFiles 
                    WHERE FileWithPath IS NULL                     -- there will always at least one empty line
                       OR LEN(CharBackupTime) <> 15                -- wrong formated timestamp (e.g. some manually created / renamed files)
                       OR ISNUMERIC(LEFT(CharBackupTime, 8))  = 0
                       OR ISNUMERIC(RIGHT(CharBackupTime, 6)) = 0
                   ;
                   UPDATE #CleanUpFiles
                      SET ShouldBeDeleted = 1
                    WHERE CharBackupTime < REPLACE(REPLACE(REPLACE(CONVERT(VARCHAR(50), @CurrentCleanupDate, 120), '-', ''), ' ', '_'), ':', '')
                   ;
                   
                   SET @Template = (SELECT TOP 1 REPLACE(cuf.FileWithPath, cuf.CharBackupTime, '*?*') template -- returns e.g. \\msdb01\z$\MSDB01\ProdDB\LOG\MSDB01_ProdDB_LOG_*?*.trn
                                      FROM #CleanUpFiles AS cuf)
                   ;
                   WITH groups AS -- Step 1: group the cleanup files by year / year + month / year + month + day / year + month + day + hour where ALL files should be deleted
                              (
                               SELECT LEFT(cuf.CharBackupTime, cte.endpos) grouppart, cte.endpos
                                 FROM #CleanUpFiles AS cuf
                                CROSS APPLY (VALUES (4), (6), (8), (11)) cte(endpos) -- end position of year, month, day and hour in CharBackupTime
                                GROUP BY LEFT(cuf.CharBackupTime, cte.endpos), cte.endpos
                                HAVING MIN(cuf.ShouldBeDeleted) = MAX(cuf.ShouldBeDeleted)
                                   AND MIN(cuf.ShouldBeDeleted) = 1
                              )
                   SELECT @CurrentCommand05 = 
                          N'DECLARE @ReturnCode int; '
                        + N'EXECUTE @ReturnCode = master.sys.xp_cmdshell '''
                        + STUFF((
                                 -- Step 2: limit to the top level group (e.g. month instead month + day + hour), if all files for this month should be deleted
                                SELECT ' & DEL "'+ REPLACE(@Template, '?', g2.grouppart) + '"' AS cmd -- -> e.g. DEL "\\msdb01\z$\MSDB01\ProdDB\LOG\MSDB01_ProdDB_LOG_*2015*.trn"
                                  FROM groups g
                                 RIGHT JOIN groups g2
                                    ON g2.grouppart LIKE g.grouppart + '%'
                                   AND g2.endpos > g.endpos
                                 WHERE g.grouppart IS NULL
                                   AND g2.grouppart IS NOT NULL
                                 ORDER BY g2.grouppart
                                   FOR XML PATH('i'), root('c'), type
                                ).query('/c/i').value('.', 'nvarchar(4000)')
                                , 1, 3, '' )
                        + N'''; '
                        + N'IF @ReturnCode <> 0 RAISERROR(''Error deleting files.'', 16, 1)'
                   ;
                   IF @CurrentCommand05 IS NULL SET @CurrentCommand05 = N'PRINT ''Cleanup: Nothing to do - no older backup files found''';
              END -- ELSE (@CleanupUsesCMD = 'Y')
          END -- @BackupSoftware IS NULL

          IF @BackupSoftware = 'LITESPEED'
          BEGIN
            SET @CurrentCommandType05 = 'xp_slssqlmaint'

            SET @CurrentCommand05 = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = [master].dbo.xp_slssqlmaint N''-MAINTDEL -DELFOLDER "' + REPLACE(@CurrentDirectoryPath,'''','''''') + '" -DELEXTENSION "' + @CurrentFileExtension + '" -DELUNIT "' + CAST(DATEDIFF(mi,@CurrentCleanupDate,GETDATE()) + 1 AS nvarchar) + '" -DELUNITTYPE "minutes" -DELUSEAGE'' IF @ReturnCode <> 0 RAISERROR(''Error deleting LiteSpeed backup files.'', 16, 1)'
          END

          IF @BackupSoftware = 'SQLBACKUP'
          BEGIN
            SET @CurrentCommandType05 = 'sqbutility'

            SET @CurrentCommand05 = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = [master].dbo.sqbutility 1032, N''' + REPLACE(@CurrentDatabaseName,'''','''''') + ''', N''' + REPLACE(@CurrentDirectoryPath,'''','''''') + ''', ''' + CASE WHEN @CurrentBackupType = 'FULL' THEN 'D' WHEN @CurrentBackupType = 'DIFF' THEN 'I' WHEN @CurrentBackupType = 'LOG' THEN 'L' END + ''', ''' + CAST(DATEDIFF(hh,@CurrentCleanupDate,GETDATE()) + 1 AS nvarchar) + 'h'', ' + ISNULL('''' + REPLACE(@EncryptionKey,'''','''''') + '''','NULL') + ' IF @ReturnCode <> 0 RAISERROR(''Error deleting SQLBackup backup files.'', 16, 1)'
          END

          IF @BackupSoftware = 'SQLSAFE'
          BEGIN
            SET @CurrentCommandType05 = 'xp_ss_delete'

            SET @CurrentCommand05 = 'DECLARE @ReturnCode int EXECUTE @ReturnCode = [master].dbo.xp_ss_delete @filename = N''' + REPLACE(@CurrentDirectoryPath,'''','''''') + '\*.' + @CurrentFileExtension + ''', @age = ''' + CAST(DATEDIFF(mi,@CurrentCleanupDate,GETDATE()) + 1 AS nvarchar) + 'Minutes'' IF @ReturnCode <> 0 RAISERROR(''Error deleting SQLsafe backup files.'', 16, 1)'
          END

          IF @CurrentMirror >= 0 -- do not execute for pending mirrors (only if mirror is available and of course for the main @Directory)
             EXECUTE @CurrentCommandOutput05 = [dbo].[CommandExecute] @Command = @CurrentCommand05, @CommandType = @CurrentCommandType05, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
          ELSE  -- @CurrentMirror  < 0
          BEGIN -- pending mirrors: insert into CommandLog
             INSERT INTO dbo.CommandLog (DatabaseName, CommandType, IndexType, Command, ExtendedInfo,
                         StartTime, EndTime, ErrorNumber, ErrorMessage)
             VALUES (@CurrentDatabaseName, 'PENDING_CLEANUP_AFTER', ABS(@CurrentMirror), @CurrentCommand05, CAST(@CurrentDirectoryPath AS XML),
                     GETDATE(),
                     DATEADD(HOUR, CASE ABS(@CurrentMirror)
                                        WHEN 1 THEN @MirrorCleanupTime
                                        WHEN 2 THEN @MirrorCleanupTime2
                                        WHEN 3 THEN @MirrorCleanupTime3
                                   END, GETDATE()),
                     CASE WHEN @Execute = 'Y' THEN -1 ELSE 0 END,
                     CASE WHEN @Execute = 'Y' THEN 'Not started' ELSE 'not executed because @Execute was set to N' END
                    )
             SET @CurrentCommandOutput05 = @@ERROR
          END 

          SET @Error = @@ERROR
          IF @Error <> 0 SET @CurrentCommandOutput05 = @Error
          IF @CurrentCommandOutput05 <> 0 SET @ReturnCode = @CurrentCommandOutput05

          UPDATE @CurrentDirectories
          SET CleanupCompleted = 1,
              CleanupOutput = @CurrentCommandOutput05
          WHERE ID = @CurrentDirectoryID

          SET @CurrentDirectoryID = NULL
          SET @CurrentDirectoryPath = NULL
          SET @CurrentCleanupDate = NULL

          SET @CurrentCommand05 = NULL

          SET @CurrentCommandOutput05 = NULL

          SET @CurrentCommandType05 = NULL
        END
      END
    END

    IF DATABASEPROPERTYEX(@CurrentDatabaseName,'Status') = 'SUSPECT'
    BEGIN
      SET @ErrorMessage = 'The database ' + QUOTENAME(@CurrentDatabaseName) + ' is in a SUSPECT state.'
      RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
      SET @Error = @@ERROR
      RAISERROR(@EmptyLine,10,1) WITH NOWAIT
    END

    -- Update that the database is completed
    IF @DatabasesInParallel = 'Y'
    BEGIN
      UPDATE dbo.QueueDatabase
      SET DatabaseEndTime = GETDATE()
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
    SET @CurrentDatabaseID = NULL
    SET @CurrentDatabaseName = NULL
    SET @CurrentBackupType = NULL
    SET @CurrentFileExtension = NULL
    SET @CurrentFileNumber = NULL
    SET @CurrentDifferentialBaseLSN = NULL
    SET @CurrentDifferentialBaseIsSnapshot = NULL
    SET @CurrentLogLSN = NULL
    SET @CurrentLatestBackup = NULL
    SET @CurrentDatabaseNameFS = NULL
    SET @CurrentDirectoryStructure = NULL
    SET @CurrentDatabaseFileName = NULL
    SET @CurrentMaxFilePathLength = NULL
    SET @CurrentDate = NULL
    SET @CurrentCleanupDate = NULL
    SET @CurrentIsDatabaseAccessible = NULL
    SET @CurrentAvailabilityGroup = NULL
    SET @CurrentAvailabilityGroupRole = NULL
    SET @CurrentAvailabilityGroupBackupPreference = NULL
    SET @CurrentIsPreferredBackupReplica = NULL
    SET @CurrentDatabaseMirroringRole = NULL
    SET @CurrentLogShippingRole = NULL
    SET @CurrentIsEncrypted = NULL
    SET @CurrentIsReadOnly = NULL
    SET @CurrentLastLogBackup = NULL
    SET @CurrentLogSizeSinceLastLogBackup = NULL
    SET @CurrentAllocatedExtentPageCount = NULL
    SET @CurrentModifiedExtentPageCount = NULL
    SET @CurrentMirror = NULL      

    SET @CurrentCommand03 = NULL
    SET @CurrentCommand06 = NULL
    SET @CurrentCommand07 = NULL

    SET @CurrentCommandOutput03 = NULL

    SET @CurrentCommandType03 = NULL

    DELETE FROM @CurrentDirectories
    DELETE FROM @CurrentURLs
    DELETE FROM @CurrentFiles
    DELETE FROM @CurrentCleanupDates
    DELETE FROM @CurrentBackupSet

  END -- END of loop: WHILE EXISTS (SELECT * FROM @tmpDatabases WHERE Selected = 1 AND Completed = 0) AND @BackupType <> 'COPY_PENDING_MIRRORS'

  ----------------------------------------------------------------------------------------------------
  --// copy pending backups to mirror                                                             //--
  ----------------------------------------------------------------------------------------------------
  IF OBJECT_ID('master.dbo.CommandLog') IS NOT NULL
  BEGIN
      UPDATE master.dbo.CommandLog WITH (ROWLOCK)
         SET ErrorNumber    = 99,
             ErrorMessage   = 'Not copied / verified because cleanup time reached'
       WHERE CommandType   IN ('PENDING_COPY', 'PENDING_VERIFY') -- Missed Cleanups could be skipped
         AND ErrorNumber    = -1        -- not (yet) started
         AND EndTime       <= GETDATE() -- EndTime = Original BackupTime + CleanUpTime -> skip when to old
         AND ID             < @CommandLogIdAtStart -- otherwise it would change the entries created in this procedure call 
                                                   -- -> stupid idea, because either the mirror is not available or @MirrorType is set to COPY_LATER
      ;

      WHILE @SkipPendingMirrorCopies = 'N'
      BEGIN
         SET @CurrentCommandLogId = NULL;
         SET @CurrentCommandOutput08 = NULL;

         SELECT TOP 1 
                @CurrentCommandLogId    = cl.id, 
                @CurrentDatabaseName    = cl.DatabaseName, 
                @CurrentCommand08       = cl.Command, 
                @CurrentMirror          = cl.IndexType,
                @CurrentCommandType08   = cl.CommandType,
                @CurrentExtendedInfo    = cl.ExtendedInfo
           FROM master.dbo.CommandLog  AS cl WITH (NOLOCK)
          WHERE cl.CommandType         IN ('PENDING_CLEANUP_BEFORE', 'PENDING_COPY', 'PENDING_VERIFY', 'PENDING_CLEANUP_AFTER')
            AND cl.ErrorNumber          = -1                   -- not (yet) started
            AND cl.ID                  <= @CommandLogIdAtStart -- otherwise it would read the entries created in this procedure call 
                                                                  -- -> stupid idea, because either the mirror is not available or @MirrorType is set to COPY_LATER
          ORDER BY cl.ID
          ;
          IF @CurrentCommandLogId IS NULL BREAK -- leave the WHILE loop when no (more) records are found

          UPDATE dbo.CommandLog SET ErrorNumber = -2, ErrorMessage = 'work in progress' WHERE id = @CurrentCommandLogId
      
          IF @CurrentCommandType08 IN ('PENDING_CLEANUP_BEFORE', 'PENDING_CLEANUP_AFTER')
          AND ISNULL(@CleanUpStartTime, CAST('00:00:00.000' AS TIME)) <= CAST(@StartTime AS TIME)-- only when in the specified timeframe
          AND ISNULL(@CleanUpEndTime  , CAST('23:59:59.999' AS TIME)) >= CAST(@StartTime AS TIME)
          BEGIN 
               -- Execute pending CLEANUP commands in the mirror directories, if the Directory is accessible
               -- since the select above is ordered by ID it should be ensured, that the pending backup file was copied to the server first
               SET @CurrentDirectoryPath = CAST(@CurrentExtendedInfo AS NVARCHAR(max))
               IF NOT EXISTS (SELECT * FROM @Directories AS d WHERE DirectoryPath = @CurrentDirectoryPath)
                  INSERT INTO @Directories (ID, DirectoryPath, Mirror, Completed, Available                       )
                         SELECT @CurrentCommandLogId, @CurrentDirectoryPath, 0, 0, 1

               IF EXISTS (SELECT * FROM @Directories AS d WHERE DirectoryPath = @CurrentDirectoryPath AND d.Available = 1)
               BEGIN
                   DELETE FROM @DirectoryInfo
                   INSERT INTO @DirectoryInfo (FileExists, FileIsADirectory, ParentDirectoryExists)
                   EXECUTE [master].dbo.xp_fileexist @CurrentDirectoryPath

                   IF EXISTS (SELECT * FROM @DirectoryInfo WHERE FileExists = 0 AND FileIsADirectory = 1 AND ParentDirectoryExists = 1)
                   BEGIN
                      EXECUTE @CurrentCommandOutput08 = [dbo].[CommandExecute] @Command = @CurrentCommand08, @CommandType = @CurrentCommandType08, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
                      SET @Error = @@ERROR
                   END

                   -- skip it at the next run
                   IF EXISTS (SELECT * FROM @DirectoryInfo WHERE FileExists = 0 AND FileIsADirectory = 0 AND ParentDirectoryExists = 0)
                      UPDATE @Directories SET Available = 0 WHERE DirectoryPath = @CurrentDirectoryPath 
               END
          END

          IF @CurrentCommandType08 = 'PENDING_COPY'
          BEGIN 
               -- Execute pending COPY command, when the sourcefile and target directory are available
               SELECT @CurrentDirectoryPath = x.node.value('source[1]', 'nvarchar(1024)')
                 FROM @CurrentExtendedInfo.nodes('PendingMirrorCopy/Files') as x(node)

               IF @CurrentDirectoryPath LIKE '_:\%' SET @CurrentRootDirectoryPath = LEFT(@CurrentDirectoryPath, 3)
               ELSE SET @CurrentRootDirectoryPath = SUBSTRING(@CurrentDirectoryPath, 1, CHARINDEX('\', @CurrentDirectoryPath, CHARINDEX('\', @CurrentDirectoryPath, 3)+1)) -- returns 

               IF NOT EXISTS (SELECT * FROM @Directories AS d WHERE DirectoryPath = @CurrentRootDirectoryPath)
                  INSERT INTO @Directories (ID, DirectoryPath, Mirror, Completed, Available)
                         SELECT @CurrentCommandLogId * -1, @CurrentRootDirectoryPath, 0, 0, 1

               IF EXISTS (SELECT * FROM @Directories AS d WHERE DirectoryPath = @CurrentRootDirectoryPath  AND d.Available = 1)
               BEGIN
                   DELETE FROM @DirectoryInfo;
                   INSERT INTO @DirectoryInfo (FileExists, FileIsADirectory, ParentDirectoryExists)
                   EXECUTE [master].dbo.xp_fileexist @CurrentDirectoryPath;

                   IF NOT EXISTS (SELECT * FROM @DirectoryInfo WHERE FileExists = 1 AND FileIsADirectory = 0 AND ParentDirectoryExists = 1)
                   BEGIN
                      SET @Error = 2
                      SET @ErrorMessage = 'Error while copying pending backup to the mirror directory - source file ' + ISNULL(@CurrentDirectoryPath, '<NULL>') + ' does not (longer) exists.'
                      UPDATE dbo.CommandLog SET ErrorNumber = @Error, ErrorMessage = @ErrorMessage WHERE id = @CurrentCommandLogId
                      RAISERROR(@ErrorMessage,15,1) WITH NOWAIT

                      IF EXISTS (SELECT * FROM @DirectoryInfo WHERE FileExists = 0 AND FileIsADirectory = 0 AND ParentDirectoryExists = 0) 
                         UPDATE @Directories SET Available = 0 WHERE DirectoryPath = @CurrentRootDirectoryPath

                   END
               END

               SELECT @CurrentDirectoryPath = x.node.value('target[1]', 'nvarchar(1024)')
                 FROM @CurrentExtendedInfo.nodes('PendingMirrorCopy/Files') as x(node)

               IF @CurrentDirectoryPath LIKE '_:\%' SET @CurrentRootDirectoryPath = LEFT(@CurrentDirectoryPath, 3) -- returns e.g. z:\
               ELSE SET @CurrentRootDirectoryPath = SUBSTRING(@CurrentDirectoryPath, 1, CHARINDEX('\', @CurrentDirectoryPath, CHARINDEX('\', @CurrentDirectoryPath, 3)+1)) -- returns e.g. \\backup_nas\my_server\

               IF NOT EXISTS (SELECT * FROM @Directories AS d WHERE DirectoryPath = @CurrentRootDirectoryPath)
                  INSERT INTO @Directories (ID, DirectoryPath, Mirror, Completed, Available)
                         SELECT @CurrentCommandLogId, @CurrentRootDirectoryPath, 0, 0, 1

               IF EXISTS (SELECT * FROM @Directories AS d WHERE DirectoryPath = @CurrentRootDirectoryPath AND d.Available = 1)
               BEGIN
                   DELETE FROM @DirectoryInfo
                   INSERT INTO @DirectoryInfo (FileExists, FileIsADirectory, ParentDirectoryExists)
                   EXECUTE [master].dbo.xp_fileexist @CurrentDirectoryPath

                   -- commented out - when the copy was not successful the target fail may exists but is not complete 
                   --                 in this case there would remain a unusable copy of the backup file on the target server, when it would set just to 'done'
                   --                 When launching the copy command again, it would be overwritten. When someone had successful manually copied the file ROBOCOPY would do nothing, 
                   --                 while COPY / XCOPY would execute the copy again (overwrite).
                   --IF EXISTS (SELECT * FROM @DirectoryInfo WHERE FileExists = 1 AND FileIsADirectory = 0 AND ParentDirectoryExists = 1)
                   --BEGIN
                   --    UPDATE dbo.CommandLog SET ErrorNumber = 0, ErrorMessage = 'Target file already exists - not copied again',  EndTime = GETDATE() WHERE id = @CurrentCommandLogId
                   --END 
                   IF EXISTS (SELECT * FROM @DirectoryInfo WHERE /*FileExists = 0 AND */ FileIsADirectory = 0 AND ParentDirectoryExists = 1)
                   BEGIN -- Target directory (but not file) exists -> copy
                       EXECUTE @CurrentCommandOutput08 = [dbo].[CommandExecute] @Command = @CurrentCommand08, @CommandType = @CurrentCommandType08, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
                       SET @Error = @@ERROR
                   END;
                   -- Target directory is still missing
                   IF EXISTS (SELECT * FROM @DirectoryInfo WHERE FileExists = 0 AND FileIsADirectory = 0 AND ParentDirectoryExists = 0) 
                   BEGIN
                        -- try to create it
                        SET @CurrentFilePath = SUBSTRING(@CurrentDirectoryPath, 1, LEN(@CurrentDirectoryPath) - CHARINDEX('\', REVERSE(@CurrentDirectoryPath)))
                        SET @CurrentCommandType01 = 'xp_create_subdir'
                        SET @CurrentCommand01 = 'DECLARE @ReturnCode int;' + CHAR(13) + CHAR(10)
                                              + 'EXECUTE @ReturnCode = [master].dbo.xp_create_subdir N''' 
                                              +             REPLACE(@CurrentFilePath,
                                                                    CHAR(39), CHAR(39) + CHAR(39)) + ''';' + CHAR(13) + CHAR(10)
                                              + 'IF @ReturnCode <> 0 RAISERROR(''Error creating directory.'', 16, 1);'
                        EXECUTE @CurrentCommandOutput01 = [dbo].[CommandExecute] @Command = @CurrentCommand01, @CommandType = @CurrentCommandType01, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
                    
                        IF @CurrentCommandOutput01 = 0
                        BEGIN -- when the target directory could be created, then copy the file
                             EXECUTE @CurrentCommandOutput08 = [dbo].[CommandExecute] @Command = @CurrentCommand08, @CommandType = @CurrentCommandType08, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
                             SET @Error = @@ERROR
                        END
                        ELSE 
                        BEGIN
                           UPDATE @Directories SET Available = 0 WHERE DirectoryPath = @CurrentRootDirectoryPath -- otherwise mark the root as not available (e.g. network error)
                           SET @Error = -1 -- skip and retry in the next run 
                        END
                   END 
               END 
               ELSE 
                   SET @Error = -1 -- skip, since the directory was for the previous PENDING_COPY command (with the same target directory) not available 

          END -- IF @CurrentCommandType08 = 'PENDING_COPY'


          IF @CurrentCommandType08 = 'PENDING_VERIFY'
          BEGIN 
               -- first check, if all target files are available, since you can not verify a backup if one or more files are still missing (e.g. because of a pending copy)
               DECLARE CurTargetFiles CURSOR LOCAL FORWARD_ONLY STATIC READ_ONLY FOR
                       SELECT FilePath = x.node.value('FilePath[1]', 'nvarchar(1024)')
                         FROM @CurrentExtendedInfo.nodes('TargetFiles/Files') as x(node)
               OPEN CurTargetFiles
           
               SET @CurrentFilePath = NULL

               WHILE 1 = 1
               BEGIN
                   FETCH NEXT FROM CurTargetFiles INTO @CurrentFilePath
                   IF @@fetch_status <> 0 BREAK
               
                   IF @CurrentFilePath LIKE '_:\%' SET @CurrentRootDirectoryPath = LEFT(@CurrentFilePath, 3)
                   ELSE SET @CurrentRootDirectoryPath = SUBSTRING(@CurrentFilePath, 1, CHARINDEX('\', @CurrentFilePath, CHARINDEX('\', @CurrentFilePath, 3)+1)) -- returns 

                   IF NOT EXISTS (SELECT * FROM @Directories AS d WHERE DirectoryPath = @CurrentRootDirectoryPath)
                      INSERT INTO @Directories (ID, DirectoryPath, Mirror, Completed, Available)
                             SELECT @CurrentCommandLogId, @CurrentRootDirectoryPath, 0, 0, 1

                   IF EXISTS (SELECT * FROM @Directories AS d WHERE DirectoryPath = @CurrentRootDirectoryPath  AND d.Available = 1)
                   BEGIN
                       DELETE FROM @DirectoryInfo
                       INSERT INTO @DirectoryInfo (FileExists, FileIsADirectory, ParentDirectoryExists)
                       EXECUTE [master].dbo.xp_fileexist @CurrentFilePath

                       IF NOT EXISTS (SELECT * FROM @DirectoryInfo WHERE FileExists = 1 AND FileIsADirectory = 0 AND ParentDirectoryExists = 1)
                       BEGIN
                          SET @CurrentFilePath = NULL -- target file does not exists
                          SET @Error           = -1
                          BREAK
                       END 

                       IF EXISTS (SELECT * FROM @DirectoryInfo WHERE FileExists = 0 AND FileIsADirectory = 0 AND ParentDirectoryExists = 0)
                          UPDATE @Directories SET Available = 0 WHERE DirectoryPath = @CurrentRootDirectoryPath
                  END
                  ELSE 
                      BEGIN
                          SET @CurrentFilePath = NULL;
                          SET @Error = -1; -- Directory not available
                      END 
               END -- WHILE loop (cursor)
               CLOSE CurTargetFiles
               DEALLOCATE CurTargetFiles
           
               IF @CurrentFilePath IS NOT NULL -- when all target files exists, execute the verify command
               BEGIN
                  EXECUTE @CurrentCommandOutput08 = [dbo].[CommandExecute] @Command = @CurrentCommand08, @CommandType = @CurrentCommandType08, @Mode = 1, @DatabaseName = @CurrentDatabaseName, @LogToTable = @LogToTable, @Execute = @Execute
                  SET @Error = @@ERROR
               END
          END -- IF @CurrentCommandType08 = 'PENDING_VERIFY'

          IF ISNULL(@Error, 0)                  <> 0 SET @CurrentCommandOutput08 = @Error
          IF ISNULL(@CurrentCommandOutput08, 0) <> 0 SET @ReturnCode             = @CurrentCommandOutput08


          IF @CurrentCommandOutput08 = 0
                UPDATE dbo.CommandLog SET ErrorNumber = 0, ErrorMessage = 'successfull executed', EndTime = GETDATE() WHERE id = @CurrentCommandLogId;
      END -- WHILE-Loop for pending actions
  
      -- set unfinished pendings from "work in progress" to "not started" again (e.g. because target is still not available)
      UPDATE dbo.CommandLog WITH(ROWLOCK)
         SET ErrorNumber = -1, ErrorMessage = 'not started'
       WHERE ErrorNumber = -2
         AND CommandType         IN ('PENDING_CLEANUP_BEFORE', 'PENDING_COPY', 'PENDING_VERIFY', 'PENDING_CLEANUP_AFTER')
         AND ID                  <= @CommandLogIdAtStart
      ;
  END; -- commandlog table exists
  
  ----------------------------------------------------------------------------------------------------
  --// Log completing information                                                                 //--
  ----------------------------------------------------------------------------------------------------

  Logging:
  SET @EndMessage = 'Date and time: ' + CONVERT(nvarchar,GETDATE(),120)
  RAISERROR('%s',10,1,@EndMessage) WITH NOWAIT

  IF @ReturnCode <> 0
  BEGIN
    RETURN @ReturnCode
  END

  ----------------------------------------------------------------------------------------------------
END
GO
