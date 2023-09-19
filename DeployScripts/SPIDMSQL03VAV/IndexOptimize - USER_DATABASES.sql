USE [msdb]
GO
DECLARE @operator_name SYSNAME = 'dbamail'
DECLARE @cmd varchar(max) = CONCAT('sqlcmd -E -S $(ESCAPE_SQUOTE(SRVR)) -d DBAUtility -Q "'
,'EXECUTE [dbo].[IndexOptimize] @Databases = ''USER_DATABASES'''
,',@Indexes = ''ALL_INDEXES'' '
,',@LogToTable = ''Y'' '
,',@SortInTempdb = ''Y'' '
--,',@MaxDop = 2 '
,',@FragmentationLow = NULL '
,',@FragmentationMedium = NULL '
,',@FragmentationHigh=''INDEX_REBUILD_ONLINE,INDEX_REORGANIZE'' '
,',@FragmentationLevel2=10 '
,',@StatisticsResample = ''N'' '
,',@OnlyModifiedStatistics = ''Y'' '
,',@TimeLimit = 7200 '
,',@PartitionLevel = ''Y'' '
,',@UpdateStatistics = ''ALL'' '
,',@LOBCompaction = ''Y'' '
,'" -b')

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0

IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'IndexOptimize - USER_DATABASES', 
		@enabled=1, 
		@notify_level_eventlog=2, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Source: https://ola.hallengren.com', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=@operator_name,
		@job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [SET RECOVERY MODEL TO BULK_LOGGED]    Script Date: 9/18/2023 9:36:55 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'SET RECOVERY MODEL TO BULK_LOGGED', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'ALTER DATABASE [ID_GEN] SET RECOVERY BULK_LOGGED WITH NO_WAIT

ALTER DATABASE [ID_GENPools] SET RECOVERY BULK_LOGGED WITH NO_WAIT', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [IndexOptimize - USER_DATABASES]    Script Date: 9/18/2023 9:36:55 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'IndexOptimize - USER_DATABASES', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'CmdExec', 
		@command=@cmd,
		@output_file_name=N'$(ESCAPE_SQUOTE(SQLLOGDIR))\IndexOptimize_$(ESCAPE_SQUOTE(JOBID))_$(ESCAPE_SQUOTE(STEPID))_$(ESCAPE_SQUOTE(STRTDT))_$(ESCAPE_SQUOTE(STRTTM)).txt', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [SET RECOVERY MODEL TO  FULL]    Script Date: 9/18/2023 9:36:55 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'SET RECOVERY MODEL TO  FULL', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'ALTER DATABASE [ID_GEN] SET RECOVERY Full WITH NO_WAIT

ALTER DATABASE [ID_GENPools] SET RECOVERY Full WITH NO_WAIT', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Fail Job if any steps failed]    Script Date: 9/18/2023 9:36:55 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Fail Job if any steps failed', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SELECT  step_name, message
FROM    msdb.dbo.sysjobhistory
WHERE   instance_id > COALESCE((SELECT MAX(instance_id) FROM msdb.dbo.sysjobhistory
                                WHERE job_id = $(ESCAPE_SQUOTE(JOBID)) AND step_id = 0), 0)
        AND job_id = $(ESCAPE_SQUOTE(JOBID))
        AND run_status <> 1 -- success

IF      @@ROWCOUNT <> 0
        RAISERROR(''One or more steps failed'', 16, 1)', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Daily at 12:04AM', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20230911, 
		@active_end_date=99991231, 
		@active_start_time=400, 
		@active_end_time=235959, 
		@schedule_uid=N'ba5f4a5c-626b-4ec9-8a39-429c9a99cdc5'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


