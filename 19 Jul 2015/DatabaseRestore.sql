SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  StoredProcedure [dbo].[DatabaseRestore]    Script Date: 30.12.2015 14:03:17 ******/
-- =============================================
-- Author:		Selman AY
-- Create date:	2015-12-22
-- Description:	Intended to automatically restore transaction log backup taken by Ola Hallengren's DatabaseBackup Solution
-- =============================================
ALTER PROCEDURE [dbo].[DatabaseRestore] 
	@DatabaseName nvarchar(max),
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
/*
========== Whole Flow Described ==========
1. Step Name : DECLARE Variables => Declare variable which are global for all steps (local variables like cursors or temporary variables used in them will be declared before each step)
2. Step Name : Get Backup Filters => Process @BackupTypes parameter and fill appropriate filters (that will be used in DIR command of console) to a resultset.
3. Step Name : Get Backup Files => Full Paths and filenames of backup files are filled to a resultset using xp_cmdshell in combination with dir command
4. Step Name : Restore Headers => Backup information like server, databasename, backuptime and LSNs are extracted using RESTORE HEADERONLY command for all backup files
    4.1 TODO : 5th step is for transaction log restores define a logic like @BackupTypes and provide a solution for FULL restores. (Also define a couple of different directory structures) 
5. Step Name : Do we have the database in server => if not terminate the execution. (for transaction and diff restores not for full restores)
6. Step Name : Find last restored backup info => current restore state of the database 
========== Whole Flow Described ==========
*/

-- ========== Step 1: DECLARE Variables ==========
DECLARE @Backups TABLE (BackupPathFile varchar(2000), BackupFile varchar(2000), ServerName varchar(100), DatabaseName varchar(100), BackupType nvarchar(60), BackupDateTime datetime, IsRestored bit DEFAULT 0, FirstLSN numeric(25,0), LastLSN numeric(25,0), CheckpointLSN numeric(25,0), DatabaseBackupLSN numeric(25,0))
DECLARE @LastRestoredBackup TABLE (BackupPathFile varchar(2000), BackupFile varchar(2000), ServerName varchar(100), DatabaseName varchar(100), BackupType nvarchar(60), BackupDateTime datetime, IsRestored bit DEFAULT 0, FirstLSN numeric(25,0), LastLSN numeric(25,0), CheckpointLSN numeric(25,0), DatabaseBackupLSN numeric(25,0))
DECLARE @Tasks TABLE (OrderId int NOT NULL identity(1,1), BackupPathFile varchar(2000), SqlCommand nvarchar(max), IsExecuted bit default 0)
DECLARE @Filters TABLE (Filter varchar(100))
DECLARE @List nvarchar(max)
-- ========== DECLARE Variables ==========

-- ========== Step 2: Get Backup Filters ==========
SET @List = REPLACE(@BackupTypes, ', ', ','); 
;WITH ListItems (StartPosition, EndPosition, ListItem) AS
(
SELECT 1 AS StartPosition,
	   ISNULL(NULLIF(CHARINDEX(',', @List, 1), 0), LEN(@List) + 1) AS EndPosition,
	   SUBSTRING(@List, 1, ISNULL(NULLIF(CHARINDEX(',', @List, 1), 0), LEN(@List) + 1) - 1) AS ListItem
WHERE @List IS NOT NULL
UNION ALL
SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
	   ISNULL(NULLIF(CHARINDEX(',', @List, EndPosition + 1), 0), LEN(@List) + 1) AS EndPosition,
	   SUBSTRING(@List, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(',', @List, EndPosition + 1), 0), LEN(@List) + 1) - EndPosition - 1) AS ListItem
FROM ListItems
WHERE EndPosition < LEN(@List) + 1
)
INSERT INTO @Filters (Filter)
select 
    CASE ListItem 
	   WHEN 'FULL' THEN '*' + @DatabaseName + '*FULL*.bak'
	   WHEN 'DIFF' THEN '*' + @DatabaseName + '*DIFF*.bak'
	   WHEN 'LOG' THEN '*' + @DatabaseName + '*LOG*.trn'
    END
from ListItems
-- ========== Get Backup Filters ==========

-- ========== Step 3: Get Backup Files ==========
-- DECLARE CURSOR variables
DECLARE @_filter varchar(100)
DECLARE FilterCursor CURSOR LOCAL FORWARD_ONLY STATIC READ_ONLY TYPE_WARNING
FOR SELECT Filter FROM @Filters

DECLARE @cmdshell varchar(2000)
-- DECLARE CURSOR variables

OPEN FilterCursor 
FETCH NEXT FROM FilterCursor INTO @_filter

WHILE @@FETCH_STATUS = 0
BEGIN;
  -- Do the job
  SET @cmdshell = 'DIR /b /s /a ' + @backupRoot + @_filter
  INSERT INTO @Backups(BackupPathFile) 
  EXEC master.sys.xp_cmdshell @cmdshell
   
  print @cmdshell

  -- Fetch next row
  FETCH NEXT FROM FilterCursor INTO @_filter
END;
 
CLOSE FilterCursor;
DEALLOCATE FilterCursor;

DELETE FROM @Backups WHERE BackupPathFile IS NULL -- Delete emtpy lines from output
DELETE FROM @Backups WHERE BackupPathFile = 'File Not Found' -- if any filter not returns any files this line will be included in resultset by xp_cmdshell
UPDATE @Backups SET BackupFile = RIGHT(BackupPathFile, CHARINDEX('\',REVERSE(BackupPathFile))-1) -- we need only filenames for some operations or outputs
-- ========== Get Backup Files ==========

-- ========== Step 4: Restore Headers ==========
-- DECLARE CURSOR variables
DECLARE @sqltext_header nvarchar(2000)
DECLARE @sqltext_fileList nvarchar(2000)
DECLARE @BackupPathFile varchar(2000)
DECLARE @Header table (BackupPathFile varchar(2000), ServerName nvarchar(128), DatabaseName nvarchar(128), BackupTypeDescription nvarchar(60), BackupStartDate datetime, FirstLSN numeric(25,0), LastLSN numeric(25,0), CheckpointLSN numeric(25,0), DatabaseBackupLSN numeric(25,0))
DECLARE @FileList table (BackupPathFile varchar(2000), LogicalName nvarchar(128), PhysicalName nvarchar(260), Type char(1), FileID bigint, DbFileName varchar(2000), Directory varchar(2000))


-- Output of RESTORE HEADERONLY FROM DISK = '...' command can between sql server versions we need to declare different variables for different versions
DECLARE @header2014 table (BackupName nvarchar(128), BackupDescription nvarchar(255), BackupType smallint, ExpirationDate datetime, Compressed bit, Position smallint, DeviceType tinyint, UserName nvarchar(128), ServerName nvarchar(128), DatabaseName nvarchar(128), DatabaseVersion int, DatabaseCreationDate datetime, BackupSize numeric(20,0), FirstLSN numeric(25,0), LastLSN numeric(25,0), CheckpointLSN numeric(25,0), DatabaseBackupLSN numeric(25,0), BackupStartDate datetime, BackupFinishDate datetime, SortOrder smallint, CodePage smallint, UnicodeLocaleId int, UnicodeComparisonStyle int, CompatibilityLevel tinyint, SoftwareVendorId int, SoftwareVersionMajor int, SoftwareVersionMinor int, SoftwareVersionBuild int, MachineName nvarchar(128), Flags int, BindingID uniqueidentifier, RecoveryForkID uniqueidentifier, Collation nvarchar(128), FamilyGUID uniqueidentifier, HasBulkLoggedData bit, IsSnapshot bit, IsReadOnly bit, IsSingleUser bit, HasBackupChecksums bit, IsDamaged bit, BeginsLogChain bit, HasIncompleteMetaData bit, IsForceOffline bit, IsCopyOnly bit, FirstRecoveryForkID uniqueidentifier, ForkPointLSN numeric(25,0) NULL, RecoveryModel nvarchar(60), DifferentialBaseLSN numeric(25,0), DifferentialBaseGUID uniqueidentifier, BackupTypeDescription nvarchar(60), BackupSetGUID uniqueidentifier NULL, CompressedBackupSize bigint, Containment tinyint, KeyAlgorithm nvarchar(32), EncryptorThumbprint varbinary(20), EncryptorType nvarchar(32))
DECLARE @header2012 table (BackupName nvarchar(128), BackupDescription nvarchar(255), BackupType smallint, ExpirationDate datetime, Compressed bit, Position smallint, DeviceType tinyint, UserName nvarchar(128), ServerName nvarchar(128), DatabaseName nvarchar(128), DatabaseVersion int, DatabaseCreationDate datetime, BackupSize numeric(20,0), FirstLSN numeric(25,0), LastLSN numeric(25,0), CheckpointLSN numeric(25,0), DatabaseBackupLSN numeric(25,0), BackupStartDate datetime, BackupFinishDate datetime, SortOrder smallint, CodePage smallint, UnicodeLocaleId int, UnicodeComparisonStyle int, CompatibilityLevel tinyint, SoftwareVendorId int, SoftwareVersionMajor int, SoftwareVersionMinor int, SoftwareVersionBuild int, MachineName nvarchar(128), Flags int, BindingID uniqueidentifier, RecoveryForkID uniqueidentifier, Collation nvarchar(128), FamilyGUID uniqueidentifier, HasBulkLoggedData bit, IsSnapshot bit, IsReadOnly bit, IsSingleUser bit, HasBackupChecksums bit, IsDamaged bit, BeginsLogChain bit, HasIncompleteMetaData bit, IsForceOffline bit, IsCopyOnly bit, FirstRecoveryForkID uniqueidentifier, ForkPointLSN numeric(25,0) NULL, RecoveryModel nvarchar(60), DifferentialBaseLSN numeric(25,0), DifferentialBaseGUID uniqueidentifier, BackupTypeDescription nvarchar(60), BackupSetGUID uniqueidentifier NULL, CompressedBackupSize bigint, Containment tinyint)
DECLARE @header2008 table (BackupName nvarchar(128), BackupDescription nvarchar(255), BackupType smallint, ExpirationDate datetime, Compressed bit, Position smallint, DeviceType tinyint, UserName nvarchar(128), ServerName nvarchar(128), DatabaseName nvarchar(128), DatabaseVersion int, DatabaseCreationDate datetime, BackupSize numeric(20,0), FirstLSN numeric(25,0), LastLSN numeric(25,0), CheckpointLSN numeric(25,0), DatabaseBackupLSN numeric(25,0), BackupStartDate datetime, BackupFinishDate datetime, SortOrder smallint, CodePage smallint, UnicodeLocaleId int, UnicodeComparisonStyle int, CompatibilityLevel tinyint, SoftwareVendorId int, SoftwareVersionMajor int, SoftwareVersionMinor int, SoftwareVersionBuild int, MachineName nvarchar(128), Flags int, BindingID uniqueidentifier, RecoveryForkID uniqueidentifier, Collation nvarchar(128), FamilyGUID uniqueidentifier, HasBulkLoggedData bit, IsSnapshot bit, IsReadOnly bit, IsSingleUser bit, HasBackupChecksums bit, IsDamaged bit, BeginsLogChain bit, HasIncompleteMetaData bit, IsForceOffline bit, IsCopyOnly bit, FirstRecoveryForkID uniqueidentifier, ForkPointLSN numeric(25,0) NULL, RecoveryModel nvarchar(60), DifferentialBaseLSN numeric(25,0), DifferentialBaseGUID uniqueidentifier, BackupTypeDescription nvarchar(60), BackupSetGUID uniqueidentifier NULL, CompressedBackupSize bigint)

DECLARE @fileList2014 table (LogicalName nvarchar(128), PhysicalName nvarchar(260), Type char(1), FileGroupName nvarchar(128), Size numeric(20,0), MaxSize numeric(20,0), FileID bigint, CreateLSN numeric(25,0), DropLSN numeric(25,0)NULL, UniqueID uniqueidentifier, ReadOnlyLSN numeric(25,0) NULL, ReadWriteLSN numeric(25,0)NULL, BackupSizeInBytes bigint, SourceBlockSize int, FileGroupID int, LogGroupGUID uniqueidentifier NULL, DifferentialBaseLSN numeric(25,0)NULL, DifferentialBaseGUID uniqueidentifier, IsReadOnly bit, IsPresent bit, TDEThumbprint varbinary(32))
DECLARE @fileList2012 table (LogicalName nvarchar(128), PhysicalName nvarchar(260), Type char(1), FileGroupName nvarchar(128), Size numeric(20,0), MaxSize numeric(20,0), FileID bigint, CreateLSN numeric(25,0), DropLSN numeric(25,0)NULL, UniqueID uniqueidentifier, ReadOnlyLSN numeric(25,0) NULL, ReadWriteLSN numeric(25,0)NULL, BackupSizeInBytes bigint, SourceBlockSize int, FileGroupID int, LogGroupGUID uniqueidentifier NULL, DifferentialBaseLSN numeric(25,0)NULL, DifferentialBaseGUID uniqueidentifier, IsReadOnly bit, IsPresent bit, TDEThumbprint varbinary(32))
DECLARE @fileList2008 table (LogicalName nvarchar(128), PhysicalName nvarchar(260), Type char(1), FileGroupName nvarchar(128), Size numeric(20,0), MaxSize numeric(20,0), FileID bigint, CreateLSN numeric(25,0), DropLSN numeric(25,0)NULL, UniqueID uniqueidentifier, ReadOnlyLSN numeric(25,0) NULL, ReadWriteLSN numeric(25,0)NULL, BackupSizeInBytes bigint, SourceBlockSize int, FileGroupID int, LogGroupGUID uniqueidentifier NULL, DifferentialBaseLSN numeric(25,0)NULL, DifferentialBaseGUID uniqueidentifier, IsReadOnly bit, IsPresent bit, TDEThumbprint varbinary(32))


DECLARE HeaderCursor CURSOR LOCAL FORWARD_ONLY STATIC READ_ONLY TYPE_WARNING
FOR SELECT BackupPathFile FROM @Backups as b
-- DECLARE CURSOR variables

OPEN HeaderCursor 
FETCH NEXT FROM HeaderCursor INTO @BackupPathFile

WHILE @@FETCH_STATUS = 0
BEGIN
  DELETE FROM @header2014
  DELETE FROM @header2012
  DELETE FROM @header2008
  DELETE FROM @fileList2014
  DELETE FROM @fileList2012
  DELETE FROM @fileList2008

  -- Do the job
  SET @sqltext_header = 'restore headeronly from disk = ''' + @BackupPathFile + ''''
  SET @sqltext_fileList = 'restore filelistonly from disk = ''' + @BackupPathFile + ''''

  -- Output of RESTORE HEADERONLY FROM DISK = '...' command can between sql server versions
  IF CAST(SERVERPROPERTY('productversion') as varchar(50)) LIKE '12%'
  BEGIN
	   PRINT 'restore header for Sql Server 2014 : ' +  @sqltext_header
	   INSERT INTO @header2014
	   EXEC sp_executesql @sqltext_header

	   INSERT INTO @Header 
	   SELECT @BackupPathFile as BackupPathFile, ServerName, DatabaseName, BackupTypeDescription, BackupFinishDate, FirstLSN, LastLSN, CheckpointLSN, DatabaseBackupLSN
	   FROM @header2014

	   PRINT 'restore filelist for Sql Server 2014 : ' +  @sqltext_fileList
	   INSERT INTO @fileList2014
	   EXEC sp_executesql @sqltext_fileList

	   INSERT INTO @FileList (BackupPathFile, LogicalName, PhysicalName, [Type], FileID)
	   SELECT @BackupPathFile as BackupPathFile, LogicalName, PhysicalName, [Type], FileID 
	   FROM @fileList2014
  END
  ELSE IF CAST(SERVERPROPERTY('productversion') as varchar(50)) LIKE '11%'
  BEGIN
	   PRINT 'restore header for Sql Server 2012 : ' +  @sqltext_header
	   INSERT INTO @header2012 
	   EXEC sp_executesql @sqltext_header
	   
	   INSERT INTO @Header 
	   SELECT @BackupPathFile as BackupPathFile, ServerName, DatabaseName, BackupTypeDescription, BackupStartDate, FirstLSN, LastLSN, CheckpointLSN, DatabaseBackupLSN
	   FROM @header2012

	   PRINT 'restore filelist for Sql Server 2012 : ' +  @sqltext_fileList
	   INSERT INTO @fileList2012
	   EXEC sp_executesql @sqltext_fileList

	   INSERT INTO @FileList (BackupPathFile, LogicalName, PhysicalName, [Type], FileID)
	   SELECT @BackupPathFile as BackupPathFile, LogicalName, PhysicalName, Type, FileID 
	   FROM @fileList2012
  END
  ELSE IF CAST(SERVERPROPERTY('productversion') as varchar(50)) LIKE '10%'
  BEGIN
	   PRINT 'restore header for Sql Server 2008 : ' +  @sqltext_header
	   INSERT INTO @header2008
	   EXEC sp_executesql @sqltext_header
	   
	   INSERT INTO @Header 
	   SELECT @BackupPathFile as BackupPathFile, ServerName, DatabaseName, BackupTypeDescription, BackupStartDate, FirstLSN, LastLSN, CheckpointLSN, DatabaseBackupLSN
	   FROM @header2008

	   PRINT 'restore filelist for Sql Server 2008 : ' +  @sqltext_fileList
	   INSERT INTO @fileList2008
	   EXEC sp_executesql @sqltext_fileList

	   INSERT INTO @FileList (BackupPathFile, LogicalName, PhysicalName, [Type], FileID)
	   SELECT @BackupPathFile as BackupPathFile, LogicalName, PhysicalName, Type, FileID 
	   FROM @fileList2008
  END

  UPDATE b
  SET 
    b.FirstLSN = l.FirstLSN
    , b.LastLSN = l.LastLSN
    , b.CheckpointLSN = l.CheckpointLSN
    , b.DatabaseBackupLSN = l.DatabaseBackupLSN
    , b.ServerName = l.ServerName
    , b.DatabaseName = l.DatabaseName
    , b.BackupType = l.BackupTypeDescription
    , b.BackupDateTime = l.BackupStartDate
  FROM @Backups as b
    INNER JOIN @Header as l on b.BackupPathFile = l.BackupPathFile
    
  -- Fetch next row
  DELETE FROM @Header 
  DELETE FROM @header2014 
  DELETE FROM @header2012 
  DELETE FROM @header2008 

  FETCH NEXT FROM HeaderCursor INTO @BackupPathFile
END;
 
CLOSE HeaderCursor;
DEALLOCATE HeaderCursor;
-- ========== Restore Headers ==========

-- if you have AdventureWorks2008R2 and AdventureWorks2008R2_DW on the same server and wanted to restore for AdventureWorks2008R2 others are included because of filename pattern used in 'dir' command
DELETE FROM @Backups WHERE DatabaseName != @DatabaseName

-- ========== Step 5: Do we have the database in server ==========
DECLARE @DatabaseExist bit
IF EXISTS (SELECT s.name FROM sys.databases s WHERE s.name = @DatabaseName) SET @DatabaseExist = 1 ELSE SET @DatabaseExist = 0

IF (@BackupTypes like '%FULL%') -- we are allowed to make full restore, yeyyy 
BEGIN
	DECLARE @sqlcmd_full nvarchar(max) 
	DECLARE @sqlcmd_files nvarchar(max)

	DECLARE @FullBackupFilePath varchar(4000)

	select top 1 @FullBackupFilePath = BackupPathFile from @Backups order by BackupDateTime desc
	IF (RIGHT(@DataFileDirectory, 1) != '\') SET @DataFileDirectory = @DataFileDirectory + '\'
	IF (RIGHT(@LogFileDirectory, 1) != '\') SET @LogFileDirectory = @LogFileDirectory + '\'

	PRINT '@DirectoryPerDatabase = ' + @DirectoryPerDatabase

	IF (@DirectoryPerDatabase = 'Y')
	BEGIN
		UPDATE @FileList SET Directory = @DataFileDirectory + @DatabaseName + '\' WHERE [Type] = 'D'
		UPDATE @FileList SET Directory = @LogFileDirectory + @DatabaseName + '\' WHERE [Type] = 'L'

		UPDATE @FileList SET DbFileName = RIGHT(PhysicalName, CHARINDEX('\',REVERSE(PhysicalName))-1)
		
		INSERT INTO @Tasks (BackupPathFile, SqlCommand, IsExecuted) VALUES (@FullBackupFilePath, 'exec master.dbo.xp_create_subdir ''' + @DataFileDirectory + @DatabaseName + '''', 0)
		INSERT INTO @Tasks (BackupPathFile, SqlCommand, IsExecuted) VALUES (@FullBackupFilePath, 'exec master.dbo.xp_create_subdir ''' + @LogFileDirectory + @DatabaseName + '''', 0)
	END
	ELSE
	BEGIN
		UPDATE @FileList SET Directory = @DataFileDirectory WHERE [Type] = 'D'
		UPDATE @FileList SET Directory = @LogFileDirectory WHERE [Type] = 'L'

		UPDATE @FileList SET DbFileName = RIGHT(PhysicalName, CHARINDEX('\',REVERSE(PhysicalName))-1)
		
		INSERT INTO @Tasks (BackupPathFile, SqlCommand, IsExecuted) VALUES (@FullBackupFilePath, 'exec master.dbo.xp_create_subdir ''' + @DataFileDirectory + '''', 0)
		INSERT INTO @Tasks (BackupPathFile, SqlCommand, IsExecuted) VALUES (@FullBackupFilePath, 'exec master.dbo.xp_create_subdir ''' + @LogFileDirectory + '''', 0)
	END

	SET @sqlcmd_full = 'RESTORE DATABASE [' + @DatabaseName + '] FROM  DISK = N''' + @FullBackupFilePath + ''''
	SET @sqlcmd_full = @sqlcmd_full + ' WITH  FILE = 1, ' -- it is safe to make this hard coded since we are working on OLA's backups

	SELECT @sqlcmd_files = COALESCE(@sqlcmd_files + ', ', '') + a.Pattern
	FROM 
	( 
		SELECT 
			' MOVE N''' + fl.LogicalName + ''' TO N''' + fl.Directory + fl.DbFileName + ''' ' as Pattern 
			, * 
		FROM @FileList as fl WHERE fl.BackupPathFile = @FullBackupFilePath
	) as a

	print @sqlcmd_files

	SET @sqlcmd_full = @sqlcmd_full + N' ' + @sqlcmd_files
	SET @sqlcmd_full = @sqlcmd_full + N' ,  NORECOVERY,  STATS = 1'

	print @sqlcmd_full

	INSERT INTO @Tasks (BackupPathFile, SqlCommand, IsExecuted) VALUES (@FullBackupFilePath, @sqlcmd_full, 0)
END
ELSE IF @DatabaseExist = 0 -- we are not allowed to make full restore and database is not exists on server
BEGIN
    RAISERROR ('Database not found in the server. A full restore is required to continue', 16, -1);
    RETURN;
END
-- ========== Restore Headers ==========

-- ========== Step 6: Find last restored backup info ==========
-- Q1: Do we have last restored backup file in backup repository?
UPDATE b
SET 
    b.IsRestored = 1
FROM    
    @Backups as b 
    INNER JOIN msdb..restorehistory rs on b.DatabaseName = rs.destination_database_name
    INNER JOIN msdb..backupset bs ON [rs].[backup_set_id] = [bs].[backup_set_id]
    INNER JOIN msdb..backupmediafamily bmf ON [bs].[media_set_id] = [bmf].[media_set_id] AND b.BackupFile = RIGHT([bmf].[physical_device_name], CHARINDEX('\',REVERSE([bmf].[physical_device_name]))-1)
WHERE 
    [bmf].[physical_device_name] IS NOT NULL

-- A1: if no warn about the situation and find that info from msdb..restorehistory
IF @@ROWCOUNT = 0 -- Last restored backup not found in resultset
BEGIN
    RAISERROR ('Last Restored Backup file could not be found in backup repository given. Not a catastrophe but you should revise your retention policies.', -1, -1);

    INSERT INTO @LastRestoredBackup
    SELECT  TOP 1 
		  bmf.physical_device_name AS BackupPathFile
		  , RIGHT(bmf.physical_device_name, CHARINDEX('\',REVERSE(bmf.physical_device_name))-1) AS BackupFile
		  , bs.server_name AS ServerName
		  , bs.database_name AS DatabaseName
		  , CASE bs.[type] 
			 WHEN 'D' THEN 'Database'
			 WHEN 'I' THEN 'Differential database'
			 WHEN 'L' THEN 'Log'
			 WHEN 'F' THEN 'File or filegroup'
			 WHEN 'G' THEN 'Differential file'
			 WHEN 'P' THEN 'Partial'
			 WHEN 'Q' THEN 'Differential partial'
		    END AS BackupType
		  , bs.backup_start_date as BackupDateTime
		  , 0 AS IsRestored
		  , bs.first_lsn AS FirstLSN
		  , bs.last_lsn AS LastLSN
		  , bs.checkpoint_lsn AS CheckpointLSN
		  , bs.database_backup_lsn AS DatabaseBackupLSN
    FROM    msdb..restorehistory rs 
		  INNER JOIN msdb..backupset bs ON [rs].[backup_set_id] = [bs].[backup_set_id]
		  INNER JOIN msdb..backupmediafamily bmf ON [bs].[media_set_id] = [bmf].[media_set_id] 
    WHERE 
		  [bmf].[physical_device_name] IS NOT NULL
		  AND rs.destination_database_name = @DatabaseName
    ORDER BY
		  rs.restore_date DESC
END
ELSE -- Last restored backup found in resultset A1: if yes use the latest one as @LastRestoredBackup
BEGIN
    INSERT INTO @LastRestoredBackup
    SELECT TOP 1 * FROM @Backups WHERE IsRestored = 1 ORDER BY BackupDateTime DESC
END
-- ========== Find last restored backup info ==========

IF (@ReturnBackupList = 'Y')
BEGIN
    SELECT * FROM @Backups ORDER BY BackupDateTime
END

-- ========== Step 7: Do we have a broken chain of log backups ==========
;with OrderedBackups AS
(
    select *, ROW_NUMBER() OVER (order by backupdatetime) as o from @backups where backuptype = 'Transaction Log'
)
select 
    CASE WHEN b1.LastLsn = b2.FirstLsn THEN 0 ELSE 1 END AS IsBroken
    , b1.*
    , b2.BackupFile as NextBackupFile
INTO #LogBackupChain
from OrderedBackups b1
    inner join OrderedBackups b2 on b2.o = b1.o + 1

IF EXISTS (SELECT * FROM #LogBackupChain WHERE IsBroken = 1)
BEGIN
    DECLARE @err_backupfilename varchar(128), @err_nextbackupfilename varchar(128)
    SELECT TOP 1 @err_backupfilename = BackupFile, @err_nextbackupfilename = NextBackupFile FROM #LogBackupChain WHERE IsBroken = 1
    DECLARE @msg nvarchar(4000) = 'Log backup chain is broken.' + char(10)
							 + 'One or more backup files are missing between ' + @err_backupfilename + ' and ' +  @err_nextbackupfilename + char(10)
							 + 'Only the first occurance of break is listed.'
    RAISERROR (@msg, 16, -1);
END
-- ========== Do we have a broken chain of log backups ==========

-- ========== Step 8: Generate Tasks ==========
-- DECLARE CURSOR variables (LR = Last Restore State)
DECLARE @LOG_BackupPathFile varchar(2000)
DECLARE @LOG_FirstLSN numeric(25,0)
DECLARE @LOG_LastLSN numeric(25,0)
DECLARE @LR_FirstLSN numeric(25,0)
DECLARE @LR_LastLSN numeric(25,0)
DECLARE @sqlcmd nvarchar(max)

DECLARE @order int = 0

DECLARE TaskCursor CURSOR LOCAL FORWARD_ONLY STATIC READ_ONLY TYPE_WARNING
FOR SELECT BackupPathFile, FirstLSN, LastLSN FROM @Backups WHERE BackupType = 'Transaction Log' AND IsRestored = 0 ORDER BY BackupDateTime
-- DECLARE CURSOR variables

SELECT TOP 1 @LR_FirstLSN = lr.FirstLSN, @LR_LastLSN = lr.LastLSN FROM @LastRestoredBackup as lr ORDER BY BackupDateTime DESC

OPEN TaskCursor 
FETCH NEXT FROM TaskCursor INTO @LOG_BackupPathFile, @LOG_FirstLSN, @LOG_LastLSN

WHILE @@FETCH_STATUS = 0
BEGIN;

  PRINT '@LOG_BackupPathFile : ' + @LOG_BackupPathFile
  PRINT '@LOG_FirstLSN : ' + CAST(@LOG_FirstLSN as varchar(50))
  PRINT '@LOG_LastLSN : ' + CAST(@LOG_LastLSN as varchar(50))
  PRINT '@@LR_FirstLSN : ' + CAST(@LR_FirstLSN as varchar(50))
  PRINT '@@LR_LastLSN : ' + CAST(@LR_LastLSN as varchar(50))

  -- Do the job
  IF (@LOG_LastLSN < @LR_LastLSN) -- TODO : Test for equality
  BEGIN
    -- if lastlsn of current backup is less than last restored backup (log, diff or full) means there is no need to restore that file
    -- in other words log backup is too early to apply.
    PRINT 'Skipping that file. It is to early to apply : ' + @LOG_BackupPathFile
  END
  ELSE IF (@LR_LastLSN < @LOG_FirstLSN AND @order = 0) -- TODO 1 : Test for equality, TODO 2 : this should be checked for only first log backup to restore. prevent this check for latter
  BEGIN
    -- if lastlsn of last restored backup (log, diff or full) is less than firstlsn of current backup means we have lost transactions chain is broken.
    -- in other words log backup is too late to apply.

    DECLARE @msg1 nvarchar(4000) = 'Log backup chain is broken.' + char(10)
							 + 'Log backup is too late to apply : ' + @LOG_BackupPathFile 
    RAISERROR (@msg1, 16, -1);
    RETURN
  END
  ELSE 
  BEGIN
    PRINT Cast(@order as varchar(10)) + '. add task : ' + @LOG_BackupPathFile

    SET @sqlcmd = 'RESTORE LOG [' + @DatabaseName + '] FROM  DISK = N'''+ @LOG_BackupPathFile +'''  WITH NORECOVERY'
    INSERT INTO @Tasks (BackupPathFile, SqlCommand, IsExecuted) VALUES (@LOG_BackupPathFile, @sqlcmd, 0)

    SET @order = @order + 1
  END

  -- Fetch next row
  FETCH NEXT FROM TaskCursor INTO @LOG_BackupPathFile, @LOG_FirstLSN, @LOG_LastLSN
END;
 
CLOSE TaskCursor;
DEALLOCATE TaskCursor;
-- ========== Generate Tasks ==========

-- ========== Step 9: Recovery State ==========
IF(@RecoveryState = 'RECOVERY')
BEGIN
    SET @sqlcmd = 'RESTORE DATABASE [' + @DatabaseName + '] WITH RECOVERY'
    INSERT INTO @Tasks (BackupPathFile, SqlCommand, IsExecuted) VALUES (@LOG_BackupPathFile, @sqlcmd, 0)
END
ELSE IF(@RecoveryState = 'STANDBY')
BEGIN
    DECLARE @fileLocation varchar(1000)
    SELECT @fileLocation = physical_name FROM sys.master_files where database_id = DB_ID(@DatabaseName) AND file_id = 1
    SET @fileLocation = REPLACE(@fileLocation, RIGHT(@fileLocation, CHARINDEX('\',REVERSE(@fileLocation))-1), '')
    SET @fileLocation = @fileLocation + @DatabaseName + '_undo.bak'

    SET @sqlcmd = 'RESTORE LOG [' + @DatabaseName + '] WITH STANDBY = ''' + @fileLocation + ''''
    INSERT INTO @Tasks (BackupPathFile, SqlCommand, IsExecuted) VALUES (@LOG_BackupPathFile, @sqlcmd, 0)
END
-- ========== Recovery State ==========

-- ========== Step 10: Execute ==========
IF(@ReturnTaskList = 'Y')
BEGIN
    SELECT * FROM @Tasks ORDER BY OrderId
END

DECLARE @OrderId INT

IF(@Execute = 'Y')
BEGIN
WHILE EXISTS (SELECT SqlCommand FROM @Tasks WHERE IsExecuted = 0)
    BEGIN
	   SELECT TOP 1 @OrderId = OrderId, @sqlcmd = SqlCommand FROM @Tasks WHERE IsExecuted = 0 ORDER BY OrderId
    
	   EXEC sp_executesql @sqlcmd

	   PRINT 'Executed : ' + @sqlcmd

	   UPDATE @Tasks SET IsExecuted = 1 WHERE OrderId = @OrderId
    END
END
-- ========== Execute ==========
--




/* --- Buradan devam et
1. Eğer yeni bir dosya eklendiyse bunu farkedip dosyaları ilgili yere taşıyacak şekilde script üret. 
*/
END