SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[QueueDatabase]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[QueueDatabase](
  [QueueID] [int] NOT NULL,
  [DatabaseName] [sysname] NOT NULL,
  [DatabaseOrder] [int] NULL,
  [DatabaseStartTime] [datetime2](7) NULL,
  [DatabaseEndTime] [datetime2](7) NULL,
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
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_QueueDatabase_Queue]') AND parent_object_id = OBJECT_ID(N'[dbo].[QueueDatabase]'))
ALTER TABLE [dbo].[QueueDatabase]  WITH CHECK ADD  CONSTRAINT [FK_QueueDatabase_Queue] FOREIGN KEY([QueueID])
REFERENCES [dbo].[Queue] ([QueueID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_QueueDatabase_Queue]') AND parent_object_id = OBJECT_ID(N'[dbo].[QueueDatabase]'))
ALTER TABLE [dbo].[QueueDatabase] CHECK CONSTRAINT [FK_QueueDatabase_Queue]
GO

