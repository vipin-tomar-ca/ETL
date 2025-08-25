@echo off
REM SSIS Deployment Examples
REM This batch file demonstrates various ways to use the SSIS deployment scripts

echo ========================================
echo SSIS Deployment Script Examples
echo ========================================

echo.
echo Example 1: Basic deployment with Windows Authentication
echo ----------------------------------------
echo .\deploy_ssis.ps1 -SqlServerInstance "localhost" -DatabaseName "SSISDB"
echo.

echo Example 2: Deployment with SQL Server Authentication
echo ----------------------------------------
echo .\deploy_ssis.ps1 -SqlServerInstance "server\instance" -DatabaseName "SSISDB" -Username "sa" -Password "password" -UseWindowsAuthentication:$false
echo.

echo Example 3: Custom folder and project names
echo ----------------------------------------
echo .\deploy_ssis.ps1 -SqlServerInstance "localhost" -DatabaseName "SSISDB" -FolderName "MyETL" -ProjectName "DataWarehouse"
echo.

echo Example 4: Force deployment (overwrite existing)
echo ----------------------------------------
echo .\deploy_ssis.ps1 -SqlServerInstance "localhost" -DatabaseName "SSISDB" -Force
echo.

echo Example 5: Custom packages path
echo ----------------------------------------
echo .\deploy_ssis.ps1 -SqlServerInstance "localhost" -DatabaseName "SSISDB" -PackagesPath "C:\SSIS\Packages"
echo.

echo Example 6: Enhanced script with configuration file
echo ----------------------------------------
echo .\deploy_ssis_enhanced.ps1 -ConfigFile ".\deploy_ssis_config.json"
echo.

echo Example 7: Enhanced script with validation only
echo ----------------------------------------
echo .\deploy_ssis_enhanced.ps1 -ConfigFile ".\deploy_ssis_config.json" -ValidateOnly
echo.

echo Example 8: Enhanced script with backup before deploy
echo ----------------------------------------
echo .\deploy_ssis_enhanced.ps1 -ConfigFile ".\deploy_ssis_config.json" -BackupBeforeDeploy
echo.

echo Example 9: Override configuration with command line parameters
echo ----------------------------------------
echo .\deploy_ssis_enhanced.ps1 -ConfigFile ".\deploy_ssis_config.json" -SqlServerInstance "prod-server" -Force
echo.

echo ========================================
echo To run any example, copy the command and execute it in PowerShell
echo ========================================

pause
