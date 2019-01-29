IF (SCHEMA_ID('sqlservermaint') IS NULL) 
BEGIN
    EXEC ('CREATE SCHEMA [sqlservermaint] AUTHORIZATION [dbo]')
END

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[sqlservermaint].[QueueDatabase]') AND type in (N'U'))
BEGIN
CREATE TABLE [sqlservermaint].[QueueDatabase](
  [QueueID] [int] NOT NULL,
  [DatabaseName] [sysname] NOT NULL,
  [DatabaseOrder] [int] NULL,
  [DatabaseStartTime] [datetime] NULL,
  [DatabaseEndTime] [datetime] NULL,
  [SessionID] [smallint] NULL,
  [RequestID] [int] NULL,
  [RequestStartTime] [datetime] NULL,
 CONSTRAINT [PK_QueueDatabase] PRIMARY KEY CLUSTERED
(
  [QueueID] ASC,
  [DatabaseName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
)
END
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[sqlservermaint].[FK_QueueDatabase_Queue]') AND parent_object_id = OBJECT_ID(N'[sqlservermaint].[QueueDatabase]'))
ALTER TABLE [sqlservermaint].[QueueDatabase]  WITH CHECK ADD  CONSTRAINT [FK_QueueDatabase_Queue] FOREIGN KEY([QueueID])
REFERENCES [sqlservermaint].[Queue] ([QueueID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[sqlservermaint].[FK_QueueDatabase_Queue]') AND parent_object_id = OBJECT_ID(N'[sqlservermaint].[QueueDatabase]'))
ALTER TABLE [sqlservermaint].[QueueDatabase] CHECK CONSTRAINT [FK_QueueDatabase_Queue]
GO

