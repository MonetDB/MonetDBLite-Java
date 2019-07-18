@echo off

if "%~3"=="" (
    echo Exactly 3 parameters must be given, build type, MonetDBLite source dir and MonetDBLite output dir
    goto :eof
) else if not "%~4"=="" (
    echo Exactly 3 parameters must be given, build type, MonetDBLite source dir and MonetDBLite output dir
    goto :eof
)

set result=false
if "%~1"=="Release" set result=true
if "%~1"=="Debug" set result=true
if "%result%" == "false" (
    echo The first parameter must be either 'Release' or 'Debug'
    goto :eof
)

cd /D "%~dp0" & :: Move into script directory
call versions.bat

if not exist "build/%~1/windows/x86x64" (
	md "build/%~1/windows/x86x64"
)
cd "build/%~1/windows/x86x64"

cmake -DCMAKE_BUILD_TYPE="%~1" -DMONETDBLITE_SOURCE_DIR="%~2" -DMONETDBLITE_OUTPUT_DIR="%~3" ../../../..
cmake --build . --target ALL_BUILD --config "%~1"

cd /D "%~dp0"

if not exist "monetdb-java-lite/src/main/resources/libs/windows/x86x64" (
	md "monetdb-java-lite/src/main/resources/libs/windows/x86x64"
)

move "build/%~1/windows/x86x64/%~1/libmonetdblitejava.dll" "monetdb-java-lite/src/main/resources/libs/windows/x86x64/libmonetdblitejava.dll"
copy "src/embeddedjava/windows/vcruntime140.dll" "monetdb-java-lite/src/main/resources/libs/windows/x86x64/vcruntime140.dll"

cd monetdb-java-lite
call gradle.bat build
