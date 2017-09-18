@echo off
rem ### CODE OWNERS: Kyle Baird
rem
rem ### OBJECTIVE:
rem   Script environment setup so it is reproducable and accessible by multiple systems
rem
rem ### DEVELOPER NOTES:
rem   Machine may or may not have networt access. Which pipeline_components_env is called depends on that



rem ### LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Calling environment setup script
call "%~dp0setup_env.bat"

echo %~nx0 %DATE:~-4%-%DATE:~4,2%-%DATE:~7,2% %TIME%: Beginning pipeline execution
python -m higgs_twitter.pipeline
