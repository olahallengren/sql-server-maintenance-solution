SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

-- Check if the table 'CommandLog' does not exist
IF NOT EXISTS (
    SELECT 1
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[CommandLog]')
      AND type = N'U'
)
BEGIN
    -- Create the 'CommandLog' table
    CREATE TABLE [dbo].[CommandLog] (
        [ID] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED (
            [ID] ASC
        ) WITH (
            PAD_INDEX = OFF,
            STATISTICS_NORECOMPUTE = OFF,
            IGNORE_DUP_KEY = OFF,
            ALLOW_ROW_LOCKS = ON,
            ALLOW_PAGE_LOCKS = ON
        ),
        [DatabaseName] [sysname] NULL,
        [SchemaName] [sysname] NULL,
        [ObjectName] [sysname] NULL,
        Name] [sysname] NULL,
        [IndexType] [tinyint] NULL,
        [StatisticsName] [sysname] NULL,
        [PartitionNumber] [int] NULL,
        [ExtendedInfo] [xml] NULL,
        [Command] [nvarchar](max) NOT NULL,
          NOT NULL,
                [ErrorNumberorMessage] [nvarcha
GO
