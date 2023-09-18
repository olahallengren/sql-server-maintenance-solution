USE [msdb]
IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = 'IndexOptimize - USER_DATABASES')
	EXEC msdb.dbo.sp_delete_job @job_name = 'IndexOptimize - USER_DATABASES'

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
/****** Object:  Step [IndexOptimize - USER_DATABASES]    Script Date: 9/7/2023 10:08:18 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'IndexOptimize - USER_DATABASES', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'CmdExec', 
		@command=@cmd,
		@output_file_name=N'$(ESCAPE_SQUOTE(SQLLOGDIR))\IndexOptimize_$(ESCAPE_SQUOTE(JOBID))_$(ESCAPE_SQUOTE(STEPID))_$(ESCAPE_SQUOTE(STRTDT))_$(ESCAPE_SQUOTE(STRTTM)).txt', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


