$template = (Get-Content .\MaintenanceSolution_Template.sql)

$scriptFile = "MaintenanceSolution.sql"
if(Test-Path -Path $scriptFile)
{
    Clear-Content -Path $scriptFile 
}

foreach($t_line in $template)
{
    if($t_line -eq "	PRINT '[[CommandLog_TableCreation_Script]]'")
    {
        (Get-Content -Path CommandLog.sql) | ForEach-Object -Process { Add-Content -Path $scriptFile -Value $_ }
    }
    elseif($t_line -eq "PRINT '[[CommandExecute_SPCreation_Script]]'")
    {
        (Get-Content -Path CommandExecute.sql) | ForEach-Object -Process { Add-Content -Path $scriptFile -Value $_ }
    }
    elseif($t_line -eq "PRINT '[[DatabaseBackup_SPCreation_Script]]'")
    {
        (Get-Content -Path DatabaseBackup.sql) | ForEach-Object -Process { Add-Content -Path $scriptFile -Value $_ }
    }
    elseif($t_line -eq "PRINT '[[DatabaseIntegrityCheck_SPCreation_Script]]'")
    {
        (Get-Content -Path DatabaseIntegrityCheck.sql) | ForEach-Object -Process { Add-Content -Path $scriptFile -Value $_ }
    }
    elseif($t_line -eq "PRINT '[[IndexOptimize_SPCreation_Script]]'")
    {
        (Get-Content -Path IndexOptimize.sql) | ForEach-Object -Process { Add-Content -Path $scriptFile -Value $_ }
    }
    elseif($t_line -eq "PRINT '[[DatabaseRestore_SPCreation_Script]]'")
    {
        (Get-Content -Path DatabaseRestore.sql) | ForEach-Object -Process { Add-Content -Path $scriptFile -Value $_ }
    }
    elseif($t_line -eq "PRINT '[[DatabaseRestoreMany_SPCreation_Script]]'")
    {
        (Get-Content -Path DatabaseRestoreMany.sql) | ForEach-Object -Process { Add-Content -Path $scriptFile -Value $_ }
    }
    else
    {
        Add-Content -Path $scriptFile -Value $t_line
    }
}