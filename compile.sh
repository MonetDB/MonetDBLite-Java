#!/bin/bash

set -e

case "$1" in
    windows)
        BUILDSYS=windows
        BUILDINPUTLIBRARY=Release/monetdb5.dll
        BUILDOUTPUTLIBRARY=monetdb5.dll
        ;;

    macosx)
        BUILDSYS=macosx
        BUILDINPUTLIBRARY=libmonetdb5.so
        BUILDOUTPUTLIBRARY=libmonetdb5.dylib
        ;;

    *)
        BUILDSYS=linux
        BUILDINPUTLIBRARY=libmonetdb5.so
        BUILDOUTPUTLIBRARY=libmonetdb5.so
esac

# Save the previous directory
PREVDIRECTORY=`pwd`
BASEDIR=$(realpath `dirname $0`)
cd $BASEDIR
mkdir -p build/$BUILDSYS
cd build/$BUILDSYS

# Time to compile
if [[ $1 == "windows" ]] ; then
    cmake -G "Visual Studio 15 2017 Win64" ../..
    cmake --build . --target ALL_BUILD --config Release
else
    OPT=true cmake -G "Unix Makefiles" ../..
    # For MacOS builds, change -Wl,-soname linker option
    if [[ $1 == "macosx" ]] ; then
        sed -i "s/-Wl,-soname,${BUILDINPUTLIBRARY}/-Wl,-install_name,${BUILDINPUTLIBRARY}/g" CMakeFiles/monetdb5.dir/link.txt
    fi
    make clean && make -j
fi

# Move the compiled library to the Gradle directory
cd $BASEDIR
mkdir -p monetdb-java-lite/src/main/resources/libs/$BUILDSYS

mv build/$BUILDSYS/$BUILDINPUTLIBRARY monetdb-java-lite/src/main/resources/libs/$BUILDSYS/$BUILDOUTPUTLIBRARY

# On Windows copy the runtime library
if [[ $1 == "windows" ]] ; then
    cp -rf src/embeddedjava/windows/vcruntime140.dll monetdb-java-lite/src/main/resources/libs/$BUILDSYS/vcruntime140.dll
fi

# If we are not on Travis then we perform the gradle build
if [[ -z $TRAVIS ]] ; then
    cd monetdb-java-lite
    ./gradlew build
else
    # For when compiling in a Docker container
    chmod -R 777 monetdb-java-lite
fi

cd $PREVDIRECTORY
