SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

-- Create the procedure [CommandExecute] if it does not already exist
IF NOT EXISTS (
    SELECT * 
    FROM sys.objects 
    WHERE object_id = OBJECT_ID(N'[dbo].[CommandExecute]') 
      AND type IN (N'P', N'PC')
)
BEGIN
    EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[CommandExecute] AS';
END;
GO

-- Alter the procedure to define its functionality
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
    @ExtendedInfo xml = NULL,
    @LockMessageSeverity int = 16,
    @ExecuteAsUser nvarchar(max) = NULL,
    @LogToTable nvarchar(max),
    @Execute nvarchar(max)
AS
BEGIN
    ----------------------------------------------------------------------------------------------------
    -- Initialize basic settings and declare variables
    ----------------------------------------------------------------------------------------------------
    SET NOCOUNT ON;

    DECLARE @StartMessage nvarchar(max),
            @EndMessage nvarchar(max),
            @ErrorMessage nvarchar(max),
            @ErrorMessageOriginal nvarchar(max),
            @Severity int;

    DECLARE @Errors TABLE (
        ID int IDENTITY PRIMARY KEY,
        [Message] nvarchar(max) NOT NULL,
        Severity int NOT NULL,
        [State] int
    );

    DECLARE @CurrentMessage nvarchar(max),
            @CurrentSeverity int,
            @CurrentState int;

    DECLARE @sp_executesql nvarchar(max) = QUOTENAME(@DatabaseContext) + '.sys.sp_executesql';
    DECLARE @StartTime datetime2,
            @EndTime datetime2,
            @ID int,
            @Error int = 0,
            @ReturnCode int = 0,
            @EmptyLine nvarchar(max) = CHAR(9),
            @RevertCommand nvarchar(max);

    ----------------------------------------------------------------------------------------------------
    -- Core requirements validation
    ----------------------------------------------------------------------------------------------------
    IF NOT (SELECT compatibility_level FROM sys.databases WHERE database_id = DB_ID()) >= 90
    BEGIN
        INSERT INTO @Errors ([Message], Severity, [State])
        SELECT 'The database must have a compatibility level of 90 or higher.', 16, 1;
    END;

    IF NOT (SELECT uses_ansi_nulls FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
    BEGIN
        INSERT INTO @Errors ([Message], Severity, [State])
        SELECT 'ANSI_NULLS must be set to ON for the stored procedure.', 16, 1;
    END;

    IF NOT (SELECT uses_quoted_identifier FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
    BEGIN
        INSERT INTO @Errors ([Message], Severity, [State])
        SELECT 'QUOTED_IDENTIFIER must be set to ON for the stored procedure.', 16, 1;
    END;

    IF @LogToTable = 'Y' 
       AND NOT EXISTS (
           SELECT * 
           FROM sys.objects o
           JOIN sys.schemas s ON o.[schema_id] = s.[schema_id]
           WHERE o.[type] = 'U' AND s.[name] = 'dbo' AND o.[name] = 'CommandLog'
       )
    BEGIN
        INSERT INTO @Errors ([Message], Severity, [State])
        SELECT 'The table "CommandLog" is missing. Download the script from the provided link.', 16, 1;
    END;

    ----------------------------------------------------------------------------------------------------
    -- Input parameters validation
    ----------------------------------------------------------------------------------------------------
    IF @DatabaseContext IS NULL OR NOT EXISTS (SELECT * FROM sys.databases WHERE name = @DatabaseContext)
    BEGIN
        INSERT INTO @Errors ([Message], Severity, [State])
        SELECT 'Invalid value for @DatabaseContext.', 16, 1;
    END;

    IF @Command IS NULL OR @Command = ''
    BEGIN
        INSERT INTO @Errors ([Message], Severity, [State])
        SELECT 'Invalid value for @Command.', 16, 1;
    END;

    IF @CommandType IS NULL OR LEN(@CommandType) > 60
    BEGIN
        INSERT INTO @Errors ([Message], Severity, [State])
        SELECT 'Invalid value for @CommandType.', 16, 1;
    END;

    IF @Mode NOT IN (1, 2)
    BEGIN
        INSERT INTO @Errors ([Message], Severity, [State])
        SELECT 'Invalid value for @Mode.', 16, 1;
    END;

    -- Additional validations...

    ----------------------------------------------------------------------------------------------------
    -- Raise errors if any validation fails
    ----------------------------------------------------------------------------------------------------
    DECLARE ErrorCursor CURSOR FAST_FORWARD FOR 
    SELECT [Message], Severity, [State] 
    FROM @Errors 
    ORDER BY [ID];

    OPEN ErrorCursor;
    FETCH NEXT FROM ErrorCursor INTO @CurrentMessage, @CurrentSeverity, @CurrentState;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        RAISERROR('%s', @CurrentSeverity, @CurrentState, @CurrentMessage) WITH NOWAIT;
        FETCH NEXT FROM ErrorCursor INTO @CurrentMessage, @CurrentSeverity, @CurrentState;
    END;

    CLOSE ErrorCursor;
    DEALLOCATE ErrorCursor;

    IF EXISTS (SELECT * FROM @Errors WHERE Severity >= 16)
    BEGIN
        SET @ReturnCode = 50000;
        RETURN @ReturnCode;
    END;

    ----------------------------------------------------------------------------------------------------
    -- Additional implementation logic goes here
    ----------------------------------------------------------------------------------------------------

    -- Complete logging and finalize the procedure
    RETURN @ReturnCode;
END;
GO
