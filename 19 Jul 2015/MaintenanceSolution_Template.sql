/*

SQL Server Maintenance Solution - SQL Server 2005, SQL Server 2008, SQL Server 2008 R2, SQL Server 2012, and SQL Server 2014

Backup: https://ola.hallengren.com/sql-server-backup.html
Integrity Check: https://ola.hallengren.com/sql-server-integrity-check.html
Index and Statistics Maintenance: https://ola.hallengren.com/sql-server-index-and-statistics-maintenance.html

The solution is free: https://ola.hallengren.com/license.html

You can contact me by e-mail at ola@hallengren.com.

Last updated 19 July, 2015.

Ola Hallengren
https://ola.hallengren.com

*/

USE [master] -- Specify the database in which the objects will be created.

SET NOCOUNT ON

DECLARE @CreateJobs nvarchar(max)
DECLARE @BackupDirectory nvarchar(max)
DECLARE @CleanupTime int
DECLARE @OutputFileDirectory nvarchar(max)
DECLARE @LogToTable nvarchar(max)
DECLARE @Version numeric(18,10)
DECLARE @Error int

SET @CreateJobs          = 'Y'          -- Specify whether jobs should be created.
SET @BackupDirectory     = N'C:\Backup' -- Specify the backup root directory.
SET @CleanupTime         = NULL         -- Time in hours, after which backup files are deleted. If no time is specified, then no backup files are deleted.
SET @OutputFileDirectory = NULL         -- Specify the output file directory. If no directory is specified, then the SQL Server error log directory is used.
SET @LogToTable          = 'Y'          -- Log commands to a table.

SET @Error = 0

SET @Version = CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)),CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - 1) + '.' + REPLACE(RIGHT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)), LEN(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)))),'.','') AS numeric(18,10))

IF IS_SRVROLEMEMBER('sysadmin') = 0
BEGIN
  RAISERROR('You need to be a member of the SysAdmin server role to install the solution.',16,1)
  SET @Error = @@ERROR
END

IF OBJECT_ID('tempdb..#Config') IS NOT NULL DROP TABLE #Config

CREATE TABLE #Config ([Name] nvarchar(max),
                      [Value] nvarchar(max))

IF @CreateJobs = 'Y' AND @OutputFileDirectory IS NULL AND SERVERPROPERTY('EngineEdition') <> 4 AND @Version < 12
BEGIN
  IF @Version >= 11
  BEGIN
    SELECT @OutputFileDirectory = [path]
    FROM sys.dm_os_server_diagnostics_log_configurations
  END
  ELSE
  BEGIN
    SELECT @OutputFileDirectory = LEFT(CAST(SERVERPROPERTY('ErrorLogFileName') AS nvarchar(max)),LEN(CAST(SERVERPROPERTY('ErrorLogFileName') AS nvarchar(max))) - CHARINDEX('\',REVERSE(CAST(SERVERPROPERTY('ErrorLogFileName') AS nvarchar(max)))))
  END
END

IF @CreateJobs = 'Y' AND RIGHT(@OutputFileDirectory,1) = '\' AND SERVERPROPERTY('EngineEdition') <> 4
BEGIN
  SET @OutputFileDirectory = LEFT(@OutputFileDirectory, LEN(@OutputFileDirectory) - 1)
END

INSERT INTO #Config ([Name], [Value])
VALUES('CreateJobs', @CreateJobs)

INSERT INTO #Config ([Name], [Value])
VALUES('BackupDirectory', @BackupDirectory)

INSERT INTO #Config ([Name], [Value])
VALUES('CleanupTime', @CleanupTime)

INSERT INTO #Config ([Name], [Value])
VALUES('OutputFileDirectory', @OutputFileDirectory)

INSERT INTO #Config ([Name], [Value])
VALUES('LogToTable', @LogToTable)

INSERT INTO #Config ([Name], [Value])
VALUES('DatabaseName', DB_NAME(DB_ID()))

INSERT INTO #Config ([Name], [Value])
VALUES('Error', CAST(@Error AS nvarchar))

IF OBJECT_ID('[dbo].[DatabaseBackup]') IS NOT NULL DROP PROCEDURE [dbo].[DatabaseBackup]
IF OBJECT_ID('[dbo].[DatabaseIntegrityCheck]') IS NOT NULL DROP PROCEDURE [dbo].[DatabaseIntegrityCheck]
IF OBJECT_ID('[dbo].[IndexOptimize]') IS NOT NULL DROP PROCEDURE [dbo].[IndexOptimize]
IF OBJECT_ID('[dbo].[CommandExecute]') IS NOT NULL DROP PROCEDURE [dbo].[CommandExecute]

IF OBJECT_ID('[dbo].[CommandLog]') IS NULL AND OBJECT_ID('[dbo].[PK_CommandLog]') IS NULL
BEGIN

	--------------------------- START : CommandLog_TableCreation_Script --------------------------- 
	PRINT '[[CommandLog_TableCreation_Script]]'
	--------------------------- END : CommandLog_TableCreation_Script --------------------------- 

END
GO

--------------------------- START : CommandExecute_SPCreation_Script --------------------------- 
PRINT '[[CommandExecute_SPCreation_Script]]'
--------------------------- END : CommandExecute_SPCreation_Script --------------------------- 

GO

--------------------------- START : DatabaseBackup_SPCreation_Script --------------------------- 
PRINT '[[DatabaseBackup_SPCreation_Script]]'
--------------------------- END : DatabaseBackup_SPCreation_Script --------------------------- 

GO

--------------------------- START : DatabaseIntegrityCheck_SPCreation_Script --------------------------- 
PRINT '[[DatabaseIntegrityCheck_SPCreation_Script]]'
--------------------------- END : DatabaseIntegrityCheck_SPCreation_Script ---------------------------

GO

--------------------------- START : IndexOptimize_SPCreation_Script ---------------------------
PRINT '[[IndexOptimize_SPCreation_Script]]'
--------------------------- END : IndexOptimize_SPCreation_Script ---------------------------

GO

--------------------------- START : DatabaseRestore_SPCreation_Script ---------------------------
PRINT '[[DatabaseRestore_SPCreation_Script]]'
--------------------------- END : DatabaseRestore_SPCreation_Script ---------------------------

GO

--------------------------- START : DatabaseRestoreMany_SPCreation_Script ---------------------------
PRINT '[[DatabaseRestoreMany_SPCreation_Script]]'
--------------------------- END : DatabaseRestoreMany_SPCreation_Script ---------------------------

GO

IF (SELECT CAST([Value] AS int) FROM #Config WHERE Name = 'Error') = 0
AND (SELECT [Value] FROM #Config WHERE Name = 'CreateJobs') = 'Y'
AND SERVERPROPERTY('EngineEdition') <> 4
BEGIN

  DECLARE @BackupDirectory nvarchar(max)
  DECLARE @CleanupTime int
  DECLARE @OutputFileDirectory nvarchar(max)
  DECLARE @LogToTable nvarchar(max)
  DECLARE @DatabaseName nvarchar(max)

  DECLARE @Version numeric(18,10)

  DECLARE @TokenServer nvarchar(max)
  DECLARE @TokenJobID nvarchar(max)
  DECLARE @TokenStepID nvarchar(max)
  DECLARE @TokenDate nvarchar(max)
  DECLARE @TokenTime nvarchar(max)
  DECLARE @TokenLogDirectory nvarchar(max)

  DECLARE @ExecutionKey nvarchar(4000)

  DECLARE @JobDescription nvarchar(max)
  DECLARE @JobCategory nvarchar(max)
  DECLARE @JobOwner nvarchar(max)

  DECLARE @JobName01 nvarchar(max)
  DECLARE @JobName02 nvarchar(max)
  DECLARE @JobName03 nvarchar(max)
  DECLARE @JobName04 nvarchar(max)
  DECLARE @JobName05 nvarchar(max)
  DECLARE @JobName06 nvarchar(max)
  DECLARE @JobName07 nvarchar(max)
  DECLARE @JobName08 nvarchar(max)
  DECLARE @JobName09 nvarchar(max)
  DECLARE @JobName10 nvarchar(max)
  DECLARE @JobName11 nvarchar(max)

  DECLARE @JobCommand01 nvarchar(max)
  DECLARE @JobCommand02 nvarchar(max)
  DECLARE @JobCommand03 nvarchar(max)
  DECLARE @JobCommand04 nvarchar(max)
  DECLARE @JobCommand05 nvarchar(max)
  DECLARE @JobCommand06 nvarchar(max)
  DECLARE @JobCommand07 nvarchar(max)
  DECLARE @JobCommand08 nvarchar(max)
  DECLARE @JobCommand09 nvarchar(max)
  DECLARE @JobCommand10 nvarchar(max)
  DECLARE @JobCommand11 nvarchar(max)

  DECLARE @OutputFile01 nvarchar(max)
  DECLARE @OutputFile02 nvarchar(max)
  DECLARE @OutputFile03 nvarchar(max)
  DECLARE @OutputFile04 nvarchar(max)
  DECLARE @OutputFile05 nvarchar(max)
  DECLARE @OutputFile06 nvarchar(max)
  DECLARE @OutputFile07 nvarchar(max)
  DECLARE @OutputFile08 nvarchar(max)
  DECLARE @OutputFile09 nvarchar(max)
  DECLARE @OutputFile10 nvarchar(max)
  DECLARE @OutputFile11 nvarchar(max)

  SET @Version = CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)),CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - 1) + '.' + REPLACE(RIGHT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)), LEN(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)))),'.','') AS numeric(18,10))

  IF @Version >= 9.002047
  BEGIN
    SET @TokenServer = '$' + '(ESCAPE_SQUOTE(SRVR))'
    SET @TokenJobID = '$' + '(ESCAPE_SQUOTE(JOBID))'
    SET @TokenStepID = '$' + '(ESCAPE_SQUOTE(STEPID))'
    SET @TokenDate = '$' + '(ESCAPE_SQUOTE(STRTDT))'
    SET @TokenTime = '$' + '(ESCAPE_SQUOTE(STRTTM))'
  END
  ELSE
  BEGIN
    SET @TokenServer = '$' + '(SRVR)'
    SET @TokenJobID = '$' + '(JOBID)'
    SET @TokenStepID = '$' + '(STEPID)'
    SET @TokenDate = '$' + '(STRTDT)'
    SET @TokenTime = '$' + '(STRTTM)'
  END

  SET @ExecutionKey = @TokenJobID + '#' + @TokenDate + '#' + @TokenTime

  IF @Version >= 12
  BEGIN
    SET @TokenLogDirectory = '$' + '(ESCAPE_SQUOTE(SQLLOGDIR))'
  END

  SELECT @BackupDirectory = Value
  FROM #Config
  WHERE [Name] = 'BackupDirectory'

  SELECT @CleanupTime = Value
  FROM #Config
  WHERE [Name] = 'CleanupTime'

  SELECT @OutputFileDirectory = Value
  FROM #Config
  WHERE [Name] = 'OutputFileDirectory'

  SELECT @LogToTable = Value
  FROM #Config
  WHERE [Name] = 'LogToTable'

  SELECT @DatabaseName = Value
  FROM #Config
  WHERE [Name] = 'DatabaseName'

  SET @JobDescription = 'Source: https://ola.hallengren.com'
  SET @JobCategory = 'Database Maintenance'
  SET @JobOwner = SUSER_SNAME(0x01)

  SET @JobName01 = 'DatabaseBackup - SYSTEM_DATABASES - FULL'
  SET @JobCommand01 = 'sqlcmd -E -S ' + @TokenServer + ' -d ' + @DatabaseName + ' -Q "EXECUTE [dbo].[DatabaseBackup] @Databases = ''SYSTEM_DATABASES'', @Directory = ' + ISNULL('N''' + REPLACE(@BackupDirectory,'''','''''') + '''','NULL') + ', @BackupType = ''FULL'', @Verify = ''Y'', @CleanupTime = ' + ISNULL(CAST(@CleanupTime AS nvarchar),'NULL') + ', @CheckSum = ''Y''' + CASE WHEN @LogToTable = 'Y' THEN ',  @ExecutionKey = ''' + @ExecutionKey  + ''' ,@LogToTable = ''Y''' ELSE '' END + '" -b'
  SET @OutputFile01 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + 'DatabaseBackup_' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile01) > 200 SET @OutputFile01 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile01) > 200 SET @OutputFile01 = NULL

  SET @JobName02 = 'DatabaseBackup - USER_DATABASES - DIFF'
  SET @JobCommand02 = 'sqlcmd -E -S ' + @TokenServer + ' -d ' + @DatabaseName + ' -Q "EXECUTE [dbo].[DatabaseBackup] @Databases = ''USER_DATABASES'', @Directory = ' + ISNULL('N''' + REPLACE(@BackupDirectory,'''','''''') + '''','NULL') + ', @BackupType = ''DIFF'', @Verify = ''Y'', @CleanupTime = ' + ISNULL(CAST(@CleanupTime AS nvarchar),'NULL') + ', @CheckSum = ''Y''' + CASE WHEN @LogToTable = 'Y' THEN ', @ExecutionKey = ''' + @ExecutionKey  + ''' ,@LogToTable = ''Y''' ELSE '' END + '" -b'
  SET @OutputFile02 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + 'DatabaseBackup_' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile02) > 200 SET @OutputFile02 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile02) > 200 SET @OutputFile02 = NULL

  SET @JobName03 = 'DatabaseBackup - USER_DATABASES - FULL'
  SET @JobCommand03 = 'sqlcmd -E -S ' + @TokenServer + ' -d ' + @DatabaseName + ' -Q "EXECUTE [dbo].[DatabaseBackup] @Databases = ''USER_DATABASES'', @Directory = ' + ISNULL('N''' + REPLACE(@BackupDirectory,'''','''''') + '''','NULL') + ', @BackupType = ''FULL'', @Verify = ''Y'', @CleanupTime = ' + ISNULL(CAST(@CleanupTime AS nvarchar),'NULL') + ', @CheckSum = ''Y''' + CASE WHEN @LogToTable = 'Y' THEN ', @ExecutionKey = ''' + @ExecutionKey  + ''' ,@LogToTable = ''Y''' ELSE '' END + '" -b'
  SET @OutputFile03 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + 'DatabaseBackup_' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile03) > 200 SET @OutputFile03 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile03) > 200 SET @OutputFile03 = NULL

  SET @JobName04 = 'DatabaseBackup - USER_DATABASES - LOG'
  SET @JobCommand04 = 'sqlcmd -E -S ' + @TokenServer + ' -d ' + @DatabaseName + ' -Q "EXECUTE [dbo].[DatabaseBackup] @Databases = ''USER_DATABASES'', @Directory = ' + ISNULL('N''' + REPLACE(@BackupDirectory,'''','''''') + '''','NULL') + ', @BackupType = ''LOG'', @Verify = ''Y'', @CleanupTime = ' + ISNULL(CAST(@CleanupTime AS nvarchar),'NULL') + ', @CheckSum = ''Y''' + CASE WHEN @LogToTable = 'Y' THEN ', @ExecutionKey = ''' + @ExecutionKey  + ''' ,@LogToTable = ''Y''' ELSE '' END + '" -b'
  SET @OutputFile04 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + 'DatabaseBackup_' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile04) > 200 SET @OutputFile04 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile04) > 200 SET @OutputFile04 = NULL

  SET @JobName05 = 'DatabaseIntegrityCheck - SYSTEM_DATABASES'
  SET @JobCommand05 = 'sqlcmd -E -S ' + @TokenServer + ' -d ' + @DatabaseName + ' -Q "EXECUTE [dbo].[DatabaseIntegrityCheck] @Databases = ''SYSTEM_DATABASES''' + CASE WHEN @LogToTable = 'Y' THEN ', @ExecutionKey = ''' + @ExecutionKey  + ''' ,@LogToTable = ''Y''' ELSE '' END + '" -b'
  SET @OutputFile05 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + 'DatabaseIntegrityCheck_' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile05) > 200 SET @OutputFile05 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile05) > 200 SET @OutputFile05 = NULL

  SET @JobName06 = 'DatabaseIntegrityCheck - USER_DATABASES'
  SET @JobCommand06 = 'sqlcmd -E -S ' + @TokenServer + ' -d ' + @DatabaseName + ' -Q "EXECUTE [dbo].[DatabaseIntegrityCheck] @Databases = ''USER_DATABASES''' + CASE WHEN @LogToTable = 'Y' THEN ', @ExecutionKey = ''' + @ExecutionKey  + ''' ,@LogToTable = ''Y''' ELSE '' END + '" -b'
  SET @OutputFile06 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + 'DatabaseIntegrityCheck_' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile06) > 200 SET @OutputFile06 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile06) > 200 SET @OutputFile06 = NULL

  SET @JobName07 = 'IndexOptimize - USER_DATABASES'
  SET @JobCommand07 = 'sqlcmd -E -S ' + @TokenServer + ' -d ' + @DatabaseName + ' -Q "EXECUTE [dbo].[IndexOptimize] @Databases = ''USER_DATABASES''' + CASE WHEN @LogToTable = 'Y' THEN ', @ExecutionKey = ''' + @ExecutionKey  + ''' ,@LogToTable = ''Y''' ELSE '' END + '" -b'
  SET @OutputFile07 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + 'IndexOptimize_' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile07) > 200 SET @OutputFile07 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile07) > 200 SET @OutputFile07 = NULL

  SET @JobName08 = 'sp_delete_backuphistory'
  SET @JobCommand08 = 'sqlcmd -E -S ' + @TokenServer + ' -d ' + 'msdb' + ' -Q "DECLARE @CleanupDate datetime SET @CleanupDate = DATEADD(dd,-30,GETDATE()) EXECUTE dbo.sp_delete_backuphistory @oldest_date = @CleanupDate" -b'
  SET @OutputFile08 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + 'sp_delete_backuphistory_' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile08) > 200 SET @OutputFile08 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile08) > 200 SET @OutputFile08 = NULL

  SET @JobName09 = 'sp_purge_jobhistory'
  SET @JobCommand09 = 'sqlcmd -E -S ' + @TokenServer + ' -d ' + 'msdb' + ' -Q "DECLARE @CleanupDate datetime SET @CleanupDate = DATEADD(dd,-30,GETDATE()) EXECUTE dbo.sp_purge_jobhistory @oldest_date = @CleanupDate" -b'
  SET @OutputFile09 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + 'sp_purge_jobhistory_' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile09) > 200 SET @OutputFile09 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile09) > 200 SET @OutputFile09 = NULL

  SET @JobName10 = 'Output File Cleanup'
  SET @JobCommand10 = 'cmd /q /c "For /F "tokens=1 delims=" %v In (''ForFiles /P "' + COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '" /m *_*_*_*.txt /d -30 2^>^&1'') do if EXIST "' + COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '"\%v echo del "' + COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '"\%v& del "' + COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '"\%v"'
  SET @OutputFile10 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + 'OutputFileCleanup_' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile10) > 200 SET @OutputFile10 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile10) > 200 SET @OutputFile10 = NULL

  SET @JobName11 = 'CommandLog Cleanup'
  SET @JobCommand11 = 'sqlcmd -E -S ' + @TokenServer + ' -d ' + @DatabaseName + ' -Q "DELETE FROM [dbo].[CommandLog] WHERE StartTime < DATEADD(dd,-30,GETDATE())" -b'
  SET @OutputFile11 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + 'CommandLogCleanup_' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile11) > 200 SET @OutputFile11 = COALESCE(@OutputFileDirectory,@TokenLogDirectory) + '\' + @TokenJobID + '_' + @TokenStepID + '_' + @TokenDate + '_' + @TokenTime + '.txt'
  IF LEN(@OutputFile11) > 200 SET @OutputFile11 = NULL

  IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @JobName01)
  BEGIN
    EXECUTE msdb.dbo.sp_add_job @job_name = @JobName01, @description = @JobDescription, @category_name = @JobCategory, @owner_login_name = @JobOwner
    EXECUTE msdb.dbo.sp_add_jobstep @job_name = @JobName01, @step_name = @JobName01, @subsystem = 'CMDEXEC', @command = @JobCommand01, @output_file_name = @OutputFile01
    EXECUTE msdb.dbo.sp_add_jobserver @job_name = @JobName01
  END

  IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @JobName02)
  BEGIN
    EXECUTE msdb.dbo.sp_add_job @job_name = @JobName02, @description = @JobDescription, @category_name = @JobCategory, @owner_login_name = @JobOwner
    EXECUTE msdb.dbo.sp_add_jobstep @job_name = @JobName02, @step_name = @JobName02, @subsystem = 'CMDEXEC', @command = @JobCommand02, @output_file_name = @OutputFile02
    EXECUTE msdb.dbo.sp_add_jobserver @job_name = @JobName02
  END

  IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @JobName03)
  BEGIN
    EXECUTE msdb.dbo.sp_add_job @job_name = @JobName03, @description = @JobDescription, @category_name = @JobCategory, @owner_login_name = @JobOwner
    EXECUTE msdb.dbo.sp_add_jobstep @job_name = @JobName03, @step_name = @JobName03, @subsystem = 'CMDEXEC', @command = @JobCommand03, @output_file_name = @OutputFile03
    EXECUTE msdb.dbo.sp_add_jobserver @job_name = @JobName03
  END

  IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @JobName04)
  BEGIN
    EXECUTE msdb.dbo.sp_add_job @job_name = @JobName04, @description = @JobDescription, @category_name = @JobCategory, @owner_login_name = @JobOwner
    EXECUTE msdb.dbo.sp_add_jobstep @job_name = @JobName04, @step_name = @JobName04, @subsystem = 'CMDEXEC', @command = @JobCommand04, @output_file_name = @OutputFile04
    EXECUTE msdb.dbo.sp_add_jobserver @job_name = @JobName04
  END

  IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @JobName05)
  BEGIN
    EXECUTE msdb.dbo.sp_add_job @job_name = @JobName05, @description = @JobDescription, @category_name = @JobCategory, @owner_login_name = @JobOwner
    EXECUTE msdb.dbo.sp_add_jobstep @job_name = @JobName05, @step_name = @JobName05, @subsystem = 'CMDEXEC', @command = @JobCommand05, @output_file_name = @OutputFile05
    EXECUTE msdb.dbo.sp_add_jobserver @job_name = @JobName05
  END

  IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @JobName06)
  BEGIN
    EXECUTE msdb.dbo.sp_add_job @job_name = @JobName06, @description = @JobDescription, @category_name = @JobCategory, @owner_login_name = @JobOwner
    EXECUTE msdb.dbo.sp_add_jobstep @job_name = @JobName06, @step_name = @JobName06, @subsystem = 'CMDEXEC', @command = @JobCommand06, @output_file_name = @OutputFile06
    EXECUTE msdb.dbo.sp_add_jobserver @job_name = @JobName06
  END

  IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @JobName07)
  BEGIN
    EXECUTE msdb.dbo.sp_add_job @job_name = @JobName07, @description = @JobDescription, @category_name = @JobCategory, @owner_login_name = @JobOwner
    EXECUTE msdb.dbo.sp_add_jobstep @job_name = @JobName07, @step_name = @JobName07, @subsystem = 'CMDEXEC', @command = @JobCommand07, @output_file_name = @OutputFile07
    EXECUTE msdb.dbo.sp_add_jobserver @job_name = @JobName07
  END

  IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @JobName08)
  BEGIN
    EXECUTE msdb.dbo.sp_add_job @job_name = @JobName08, @description = @JobDescription, @category_name = @JobCategory, @owner_login_name = @JobOwner
    EXECUTE msdb.dbo.sp_add_jobstep @job_name = @JobName08, @step_name = @JobName08, @subsystem = 'CMDEXEC', @command = @JobCommand08, @output_file_name = @OutputFile08
    EXECUTE msdb.dbo.sp_add_jobserver @job_name = @JobName08
  END

  IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @JobName09)
  BEGIN
    EXECUTE msdb.dbo.sp_add_job @job_name = @JobName09, @description = @JobDescription, @category_name = @JobCategory, @owner_login_name = @JobOwner
    EXECUTE msdb.dbo.sp_add_jobstep @job_name = @JobName09, @step_name = @JobName09, @subsystem = 'CMDEXEC', @command = @JobCommand09, @output_file_name = @OutputFile09
    EXECUTE msdb.dbo.sp_add_jobserver @job_name = @JobName09
  END

  IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @JobName10)
  BEGIN
    EXECUTE msdb.dbo.sp_add_job @job_name = @JobName10, @description = @JobDescription, @category_name = @JobCategory, @owner_login_name = @JobOwner
    EXECUTE msdb.dbo.sp_add_jobstep @job_name = @JobName10, @step_name = @JobName10, @subsystem = 'CMDEXEC', @command = @JobCommand10, @output_file_name = @OutputFile10
    EXECUTE msdb.dbo.sp_add_jobserver @job_name = @JobName10
  END

  IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @JobName11)
  BEGIN
    EXECUTE msdb.dbo.sp_add_job @job_name = @JobName11, @description = @JobDescription, @category_name = @JobCategory, @owner_login_name = @JobOwner
    EXECUTE msdb.dbo.sp_add_jobstep @job_name = @JobName11, @step_name = @JobName11, @subsystem = 'CMDEXEC', @command = @JobCommand11, @output_file_name = @OutputFile11
    EXECUTE msdb.dbo.sp_add_jobserver @job_name = @JobName11
  END

END
GO