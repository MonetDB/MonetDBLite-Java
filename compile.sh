#!/bin/bash

set -e

if [[ ! -z $TRAVIS && $1 != "windows" ]]; then
    # Install MonetDB compilation dependencies
    apt-get -qq update && apt-get -qq -y install pkg-config pkgconf flex bison byacc
    if [[ $1 == "macosx" ]] ; then
        export OS=Darwin
    else
        export OS=Linux
    fi
fi

case "$1" in
    windows)
        BUILDSYS=windows
        BUILDLIBRARY=libmonetdb5.dll
        ;;

    macosx)
        BUILDSYS=macosx
        BUILDLIBRARY=libmonetdb5.dylib
        export CC=gcc
        ;;

    *)
        BUILDSYS=linux
        BUILDLIBRARY=libmonetdb5.so
        export CC=gcc
esac

# Save the previous directory
PREVDIRECTORY=`pwd`
BASEDIR=$(realpath `dirname $0`)
cd $BASEDIR

if [[ $1 == "windows" ]] ; then
    cmake -G "Visual Studio 2017 Win64"
    cmake --build . --target ALL_BUILD --config Release
else
    export OPT=true # Set the optimization flags
    make clean && make init && make -j
fi

# Move the compiled library to the Gradle directory
mkdir -p monetdb-java-lite/src/main/resources/libs/$BUILDSYS
mv build/$BUILDSYS/$BUILDLIBRARY monetdb-java-lite/src/main/resources/libs/$BUILDSYS/$BUILDLIBRARY

if [[ $1 == "windows" ]] ; then
    cp -rf src/embeddedjava/windows/vcruntime140.dll monetdb-java-lite/src/main/resources/libs/$BUILDSYS/vcruntime140.dll
fi

# If we are not on Travis then we perform the gradle build
if [ -z $TRAVIS ] ; then
    cd monetdb-java-lite
    ./gradlew build
else
    # For when compiling in a Docker container
    chmod -R 777 monetdb-java-lite
fi

cd $PREVDIRECTORY
