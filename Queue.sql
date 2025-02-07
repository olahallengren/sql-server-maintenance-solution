SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

-- Check if the table [dbo].[Queue] already exists
IF NOT EXISTS (
    SELECT 1
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[Queue]')
      AND type = N'U'
)
BEGIN
    -- Create the [Queue] table
    CREATE TABLE [dbo].[Queue] (
        [QueueID] INT IDENTITY(1,1) NOT NULL, -- Unique identifier for each queue item
        [SchemaName] SYSNAME NOT NULL,         -- Schema name of the object
        [ObjectName] SYSNAME NOT NULL,         -- Object name (e.g., table, procedure)
        [Parameters] NVARCHAR(MAX) NOT NULL,   -- Parameters for the task
        [QueueStartTime] DATETIME2(7) NULL,    -- Time when the task was queued
        [SessionID] SMALLINT NULL,             -- Session ID for the task
        [RequestID] INT NULL,                  -- Identifier for the request
        [RequestStartTime] DATETIME NULL,      -- Time when the request started processing
        CONSTRAINT [PK_Queue] PRIMARY KEY CLUSTERED ([QueueID] ASC) 
            WITH (
                PAD_INDEX = OFF,               -- Do not pad the pages of the index
                STATISTICS_NORECOMPUTE = OFF,  -- Automatically recompute statistics
                IGNORE_DUP_KEY = OFF,          -- Do not allow duplicate keys
                ALLOW_ROW_LOCKS = ON,          -- Allow row-level locking
                ALLOW_PAGE_LOCKS = ON          -- Allow page-level locking
            )
    );
END;
GO
