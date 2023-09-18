USE [msdb]
GO
EXEC msdb.dbo.sp_update_job @job_name=N'DBA_Index_Maintenance_new', 
		@enabled=0
GO
EXEC msdb.dbo.sp_update_job @job_name=N'DBA_Update_Stats', 
		@enabled=0
GO

USE [msdb]
GO
DECLARE @schedule_id int
EXEC msdb.dbo.sp_add_jobschedule  @job_name=N'IndexOptimize - USER_DATABASES',  @name=N'Daily at 12:04AM', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20230911, 
		@active_end_date=99991231, 
		@active_start_time=400, 
		@active_end_time=235959, @schedule_id = @schedule_id OUTPUT
select @schedule_id
GO
