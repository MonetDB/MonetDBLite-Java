#!/bin/bash

set -e

function echo_and_exit {
	echo $1 >&2
	exit 1
}

if [[ "$#" != 2 ]]; then
    echo_and_exit "Exactly 2 parameters must be given, OS and build type"
fi

if [[ ! "$1" =~ ^(windows|macosx|linux)$ ]]; then
    echo_and_exit "The first parameter must either be 'windows', 'macosx' or 'linux'"
fi

if [[ ! "$2" =~ ^(Release|Debug)$ ]]; then
    echo_and_exit "The second parameter must either be 'Release' or 'Debug'"
fi

case "$1" in
    windows)
        BUILDSYS=windows
        BUILDINPUTLIBRARY="$2"/monetdb5.dll
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

# A bash 4.0 way of converting a string to lowercase...
BUILDTYPE="${2,,}"

echo "Performing ${BUILDTYPE} build on ${1}"

# Move into the script's directory
BASEDIR=$(realpath `dirname $0`)
cd $BASEDIR

# Set the proper versions
./versions.sh

mkdir -p build/$BUILDTYPE/$BUILDSYS
cd build/$BUILDTYPE/$BUILDSYS

# Time to compile
if [[ $1 == "windows" ]] ; then
    cmake -G "Visual Studio 15 2017 Win64" ../../..
    cmake --build . --target ALL_BUILD --config "$2"
else
    cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE="$2" ../../..
    # For MacOS builds, change -Wl,-soname linker option
    if [[ $1 == "macosx" ]] ; then
        sed -i "s/-Wl,-soname,${BUILDINPUTLIBRARY}/-Wl,-install_name,${BUILDINPUTLIBRARY}/g" CMakeFiles/monetdb5.dir/link.txt
    fi
    make clean && make -j
fi

# Move the compiled library to the Gradle directory
cd $BASEDIR
mkdir -p monetdb-java-lite/src/main/resources/libs/$BUILDSYS

mv build/$BUILDTYPE/$BUILDSYS/$BUILDINPUTLIBRARY monetdb-java-lite/src/main/resources/libs/$BUILDSYS/$BUILDOUTPUTLIBRARY

# On Windows copy the runtime library
if [[ $1 == "windows" ]] ; then
    cp -rf src/embeddedjava/windows/vcruntime140.dll monetdb-java-lite/src/main/resources/libs/$BUILDSYS/vcruntime140.dll
fi

# Sometimes is to desirable to compile the native library only
if [[ -z $NATIVE_LIBRARY_ONLY ]] ; then
    cd monetdb-java-lite
    ./gradlew build
fi
