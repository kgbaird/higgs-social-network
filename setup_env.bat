@echo off
rem ### CODE OWNERS: Kyle Baird
rem
rem ### OBJECTIVE:
rem   Script environment setup so it is reproducable and accessible by multiple systems
rem
rem ### DEVELOPER NOTES:
rem   Machine may or may not have networt access. Which pipeline_components_env is called depends on that

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Setting up environment

SET PATH_PIPELINE_COMPONENTS_ENV=S:\PRM\Pipeline_Components_Env\pipeline_components_env.bat
rem SET PATH_PIPELINE_COMPONENTS_ENV=%USERPROFILES%\scrap\pipeline_components_env.bat

rem ### LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Calling pipeline_components_env from %PATH_PIPELINE_COMPONENTS_ENV%
call "%PATH_PIPELINE_COMPONENTS_ENV%"

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Adding local python library to PYTHONPATH
set PYTHONPATH=%~dp0python;%PYTHONPATH%


echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Finished setting up environment
