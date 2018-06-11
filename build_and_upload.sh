#!/bin/bash

set -e

function echo_and_exit {
	echo $1 >&2
	exit 1
}

if [[ -z $JAVA_HOME ]] ; then
    echo_and_exit "The JAVA_HOME directory must be set"
fi

# Docker builds
# docker run -it --rm -v $(pwd):/workdir:Z --privileged=true -e CROSS_TRIPLE=x86_64-linux-gnu -e NATIVE_LIBRARY_ONLY=1 pedrotadim/compilemonetdblite bash compile.sh linux Release
# docker run -it --rm -v $(pwd):/workdir:Z --privileged=true -e CROSS_TRIPLE=x86_64-apple-darwin -e NATIVE_LIBRARY_ONLY=1 -e CROSS_COMPILING=1 pedrotadim/compilemonetdblite bash compile.sh macosx Release

cd monetdb-java-lite

./gradlew -i build
./gradlew javadoc

# Prepare the directory to upload to our website
mkdir -p synchronizing/javadocs/embedded
# Move the header file for our Downloads section
mv ../HEADER.html synchronizing
# Move the javadocs
mv build/docs/javadoc/* synchronizing/javadocs/embedded/
# Move the monetdb-java-lite jar
mv build/libs/monetdb-java-lite-2.38.jar synchronizing
# Rsync the library files to the monet.org machine
rsync -aqz --ignore-times synchronizing/* ferreira@monetdb.org:/var/www/html/downloads/Java-Experimental/
# Remove it in the end
rm -rf synchronizing

# Load our credentials
MPASSWD=$(sed -n '2p' < gradle.properties | cut -d = -f 2)
# printf -v MPASSWD "%s\n" $MPASSWD

# Upload to Maven Central Repository
yes $MPASSWD | head -100 | /opt/maven/bin/mvn deploy
