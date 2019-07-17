#!/bin/bash

set -e

function echo_and_exit {
	echo $1 >&2
	exit 1
}

if [[ "$#" != 5 ]]; then
    echo_and_exit "Exactly 5 parameters must be given, OS, architecture, build type, MonetDBLite source dir and MonetDBLite output dir"
fi

if [[ ! "$1" =~ ^(windows|macosx|linux)$ ]]; then
    echo_and_exit "The first parameter must either be 'windows', 'macosx' or 'linux'"
fi

if [[ ! "$2" =~ ^(amd64|x86_64|arm64|aarch64)$ ]]; then
    echo_and_exit "The second parameter must either be 'arm64' or 'x86_64'"
fi

if [[ ! "$3" =~ ^(Release|Debug)$ ]]; then
    echo_and_exit "The third parameter must either be 'Release' or 'Debug'"
fi

if [[ "$2" =~ ^(amd64|x86_64|x8664|x64)$ ]]; then
    ARCH_DIR="x86x64"
elif [[ "$2" =~ ^(arm64|aarch64)$ ]]; then
    ARCH_DIR="arm64"
else
    echo_and_exit "Unknown architecture"
fi

case "$1" in
    windows)
        BUILDSYS=windows
        BUILDINPUTLIBRARY="$3"/libmonetdblitejava.dll
        BUILDOUTPUTLIBRARY=libmonetdblitejava.dll
        ;;

    macosx)
        BUILDSYS=macosx
        BUILDINPUTLIBRARY=libmonetdblitejava.so
        BUILDOUTPUTLIBRARY=libmonetdblitejava.so
        ;;

    linux)
        BUILDSYS=linux
        BUILDINPUTLIBRARY=libmonetdblitejava.so
        BUILDOUTPUTLIBRARY=libmonetdblitejava.so
        ;;

    *)
        echo_and_exit "Unknown OS"
esac

# A bash 4.0 way of converting a string to lowercase...
BUILDTYPE="${3,,}"

echo "Performing ${BUILDTYPE} build on ${1} on ${2}"

# Move into the script's directory
BASEDIR=$(realpath `dirname $0`)
cd "$BASEDIR"

# Set the proper versions
./versions.sh

mkdir -p build/"$BUILDTYPE"/"$BUILDSYS"/"$ARCH_DIR"
cd build/"$BUILDTYPE"/"$BUILDSYS"/"$ARCH_DIR"

# Time to compile
if [[ "$1" == "windows" ]] ; then
    cmake -G "Visual Studio 15 2017 Win64" -DCMAKE_BUILD_TYPE="$3" ../../../..
    cmake --build . --target ALL_BUILD --config "$3"
else
    cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE="$3" -DMONETDBLITE_SOURCE_DIR="$4" -DMONETDBLITE_OUTPUT_DIR="$5" ../../../..
    # For MacOS builds, change -Wl,-soname linker option
    if [[ "$1" == "macosx" ]] ; then
        sed -i "s/-Wl,-soname,${BUILDINPUTLIBRARY}/-Wl,-install_name,${BUILDINPUTLIBRARY}/g" CMakeFiles/monetdb5.dir/link.txt
    fi
    make clean && make -j$(getconf _NPROCESSORS_ONLN)
fi

# Move the compiled library to the Gradle directory
cd "$BASEDIR"
mkdir -p monetdb-java-lite/src/main/resources/libs/"$BUILDSYS"/"$ARCH_DIR"

mv build/"$BUILDTYPE"/"$BUILDSYS"/"$ARCH_DIR"/"$BUILDINPUTLIBRARY" monetdb-java-lite/src/main/resources/libs/"$BUILDSYS"/"$ARCH_DIR"/"$BUILDOUTPUTLIBRARY"

# On Windows copy the runtime library
if [[ "$1" == "windows" ]] ; then
    cp -rf src/embeddedjava/windows/vcruntime140.dll monetdb-java-lite/src/main/resources/libs/"$BUILDSYS"/"$ARCH_DIR"/vcruntime140.dll
fi

# Sometimes is to desirable to compile the native library only
if [[ -z "$NATIVE_LIBRARY_ONLY" ]] ; then
    cd monetdb-java-lite
    ./gradlew build
fi
