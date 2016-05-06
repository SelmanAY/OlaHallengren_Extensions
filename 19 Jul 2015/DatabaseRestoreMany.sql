SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Selman AY
-- Create date: 2015-12-22
-- Description:	Intended to automatically restore transaction log backup taken by Ola Hallengren's DatabaseBackup Solution
-- =============================================
ALTER PROCEDURE [dbo].[DatabaseRestoreMany] 
	@Databases nvarchar(max),
	@BackupRoot varchar(1000) = NULL, 
	@BackupTypes nvarchar(max) = NULL,
	@DataFileDirectory nvarchar(max) = NULL,
	@LogFileDirectory nvarchar(max) = NULL,
	@DirectoryPerDatabase char(1) = 'Y',
	@RecoveryState varchar(10) = 'NORECOVERY', -- 'RECOVERY, NORECOVERY, STANDBY'

	@ReturnBackupList char(1) = 'N',
	@ReturnTaskList char(1) = 'N',
	@Execute char(1) = 'Y'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SelectedDatabases TABLE (DatabaseName nvarchar(max),
							 DatabaseType nvarchar(max),
							 Selected bit)

    DECLARE @tmpDatabases TABLE (ID int IDENTITY,
						  DatabaseName nvarchar(max),
						  DatabaseType nvarchar(max),
						  [Snapshot] bit,
						  Selected bit,
						  Completed bit,
						  PRIMARY KEY(Selected, Completed, ID))

    DECLARE @CurrentDatabase nvarchar(max) 

    DECLARE @ErrorMessage nvarchar(max)
    DECLARE @Error int
    -------------------------------------------------------------------
    SET @Databases = REPLACE(@Databases, ', ', ',');

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
    Databases2 (DatabaseItem, Selected) AS
    (
    SELECT CASE WHEN DatabaseItem LIKE '-%' THEN RIGHT(DatabaseItem,LEN(DatabaseItem) - 1) ELSE DatabaseItem END AS DatabaseItem,
		  CASE WHEN DatabaseItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
    FROM Databases1
    ),
    Databases3 (DatabaseItem, DatabaseType, Selected) AS
    (
    SELECT CASE WHEN DatabaseItem IN('ALL_DATABASES','SYSTEM_DATABASES','USER_DATABASES') THEN '%' ELSE DatabaseItem END AS DatabaseItem,
		  CASE WHEN DatabaseItem = 'SYSTEM_DATABASES' THEN 'S' WHEN DatabaseItem = 'USER_DATABASES' THEN 'U' ELSE NULL END AS DatabaseType,
		  Selected
    FROM Databases2
    ),
    Databases4 (DatabaseName, DatabaseType, Selected) AS
    (
    SELECT CASE WHEN LEFT(DatabaseItem,1) = '[' AND RIGHT(DatabaseItem,1) = ']' THEN PARSENAME(DatabaseItem,1) ELSE DatabaseItem END AS DatabaseItem,
		  DatabaseType,
		  Selected
    FROM Databases3
    )
    INSERT INTO @SelectedDatabases (DatabaseName, DatabaseType, Selected)
    SELECT DatabaseName,
		  DatabaseType,
		  Selected
    FROM Databases4
    OPTION (MAXRECURSION 0)

    INSERT INTO @tmpDatabases (DatabaseName, DatabaseType, [Snapshot], Selected, Completed)
    SELECT [name] AS DatabaseName,
		  CASE WHEN name IN('master','msdb','model') THEN 'S' ELSE 'U' END AS DatabaseType,
		  CASE WHEN source_database_id IS NOT NULL THEN 1 ELSE 0 END AS [Snapshot],
		  0 AS Selected,
		  0 AS Completed
    FROM sys.databases
    ORDER BY [name] ASC

    UPDATE tmpDatabases
    SET tmpDatabases.Selected = SelectedDatabases.Selected
    FROM @tmpDatabases tmpDatabases
    INNER JOIN @SelectedDatabases SelectedDatabases
    ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
    AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
    AND NOT ((tmpDatabases.DatabaseName = 'tempdb' OR tmpDatabases.[Snapshot] = 1) AND tmpDatabases.DatabaseName <> SelectedDatabases.DatabaseName)
    WHERE SelectedDatabases.Selected = 1

    UPDATE tmpDatabases
    SET tmpDatabases.Selected = SelectedDatabases.Selected
    FROM @tmpDatabases tmpDatabases
    INNER JOIN @SelectedDatabases SelectedDatabases
    ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
    AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
    AND NOT ((tmpDatabases.DatabaseName = 'tempdb' OR tmpDatabases.[Snapshot] = 1) AND tmpDatabases.DatabaseName <> SelectedDatabases.DatabaseName)
    WHERE SelectedDatabases.Selected = 0

	INSERT INTO @tmpDatabases
	SELECT s.DatabaseName, NULL AS DatabaseType, 0 as Snapshot, 1 as Selected, 0 as Completed
	FROM @SelectedDatabases as s
	WHERE s.DatabaseType IS NULL and s.Selected = 1
	
    WHILE EXISTS(SELECT DatabaseName FROM @tmpDatabases t WHERE t.Completed = 0 AND Selected = 1)
    BEGIN
	   SELECT TOP 1 @CurrentDatabase =  DatabaseName FROM @tmpDatabases t WHERE t.Completed = 0 AND Selected = 1
	   
	   EXEC DatabaseRestore 
		  @DatabaseName = @CurrentDatabase, 
		  @BackupRoot = @BackupRoot, 
		  @BackupTypes = @BackupTypes,
		  @DataFileDirectory  = @DataFileDirectory,
		  @LogFileDirectory = @LogFileDirectory,
		  @DirectoryPerDatabase = @DirectoryPerDatabase,
		  @RecoveryState = @RecoveryState, -- 'RECOVERY, NORECOVERY, STANDBY'

		  @ReturnBackupList = @ReturnBackupList,
		  @ReturnTaskList = @ReturnTaskList,
		  @Execute = @Execute

	   UPDATE @tmpDatabases SET Completed = 1 WHERE DatabaseName = @CurrentDatabase
    END

    IF @Databases IS NULL OR NOT EXISTS(SELECT * FROM @SelectedDatabases) OR EXISTS(SELECT * FROM @SelectedDatabases WHERE DatabaseName IS NULL OR DatabaseName = '')
    BEGIN
	   SET @ErrorMessage = 'The value for the parameter @Databases is not supported.' + CHAR(13) + CHAR(10) + ' '
	   RAISERROR(@ErrorMessage,16,1) WITH NOWAIT
	   SET @Error = @@ERROR
    END
END