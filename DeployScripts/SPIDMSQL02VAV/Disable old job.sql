USE [msdb]
GO
EXEC msdb.dbo.sp_update_job @job_name='DBA_Index_Maintenance', 
		@enabled=0
GO
EXEC msdb.dbo.sp_update_job @job_name='DBA_Update_Stats', 
		@enabled=0
GO
