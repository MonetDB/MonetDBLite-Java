#!/bin/bash

# Set the stable and next versions of MonetDBLite-Java
NEW_JDBC_STABLE_MAJOR_VERSION=2
NEW_JDBC_STABLE_MINOR_VERSION=37

NEW_JDBC_NEXT_MAJOR_VERSION=2
NEW_JDBC_NEXT_MINOR_VERSION=37

# Set this variable if the version to be build depends in a JDBC-new SNAPSHOT release
NEW_JDBC_NEXT_SNAPSHOT=false

# These variables define the current stable release numbers (to be shown in README.md)
MONETDBLITEJAVA_STABLE_MAJOR_VERSION=2
MONETDBLITEJAVA_STABLE_MINOR_VERSION=39

# These variables define the next SNAPSHOT release numbers
MONETDBLITEJAVA_NEXT_MAJOR_VERSION=2
MONETDBLITEJAVA_NEXT_MINOR_VERSION=40

# Set this variable if the version to be build is a SNAPSHOT release
MONETDBLITEJAVA_NEXT_SNAPSHOT=true

REPLACES_STR=( "NEW_JDBC_STABLE_MAJOR_VERSION" "NEW_JDBC_STABLE_MINOR_VERSION" "NEW_JDBC_NEXT_MAJOR_VERSION"
"NEW_JDBC_NEXT_MINOR_VERSION" "MONETDBLITEJAVA_STABLE_MAJOR_VERSION"
"MONETDBLITEJAVA_STABLE_MINOR_VERSION" "MONETDBLITEJAVA_NEXT_MAJOR_VERSION" "MONETDBLITEJAVA_NEXT_MINOR_VERSION" )

REPLACES_VAL=( $NEW_JDBC_STABLE_MAJOR_VERSION $NEW_JDBC_STABLE_MINOR_VERSION $NEW_JDBC_NEXT_MAJOR_VERSION
$NEW_JDBC_NEXT_MINOR_VERSION $MONETDBLITEJAVA_STABLE_MAJOR_VERSION
$MONETDBLITEJAVA_STABLE_MINOR_VERSION $MONETDBLITEJAVA_NEXT_MAJOR_VERSION $MONETDBLITEJAVA_NEXT_MINOR_VERSION )

if [ "$MONETDBLITEJAVA_NEXT_SNAPSHOT" = true ] ; then # Add snapshot string
    REPLACES_STR=("${REPLACES_STR[@]}" "MONETDBLITEJAVA_NEXT_SNAPSHOT")
    REPLACES_VAL=("${REPLACES_VAL[@]}" "-SNAPSHOT")
fi

if [ "$NEW_JDBC_NEXT_SNAPSHOT" = true ] ; then # Add snapshot string
    REPLACES_STR=("${REPLACES_STR[@]}" "NEW_JDBC_NEXT_SNAPSHOT")
    REPLACES_VAL=("${REPLACES_VAL[@]}" "-SNAPSHOT")
fi

SED_REGEX=""

for ((i=0; i < ${#REPLACES_VAL[@]} ; i+=1))
do
    SED_REGEX="${SED_REGEX} s/@${REPLACES_STR[i]}@/${REPLACES_VAL[i]}/g;"
done

SED_REGEX="${SED_REGEX%?}" # Bash way of remove last char

# This way is more secure
TO_UPDATE=( "build_and_upload.sh.in" "HEADER.html.in" "README.md.in" "monetdb-java-lite/pom.xml.in"
"monetdb-java-lite/build.gradle.in"
"monetdb-java-lite/src/main/java/nl/cwi/monetdb/embedded/env/MonetDBJavaLiteLoader.java.in" )

for entry in "${TO_UPDATE[@]}"
do
    sed "${SED_REGEX}" "${entry}" > "${entry::-3}" # Remove .in
done

# Remove the snapshot string
if [ "$MONETDBLITEJAVA_NEXT_SNAPSHOT" = false ] ; then
    OUT_FILE=`mktemp`
    for entry in "${TO_UPDATE[@]}"
    do
        sed 's/@MONETDBLITEJAVA_NEXT_SNAPSHOT@//' "${entry::-3}" > $OUT_FILE && mv $OUT_FILE "${entry::-3}"
    done
fi

if [ "$NEW_JDBC_NEXT_SNAPSHOT" = false ] ; then
    OUT_FILE=`mktemp`
    for entry in "${TO_UPDATE[@]}"
    do
        sed 's/@NEW_JDBC_NEXT_SNAPSHOT@//' "${entry::-3}" > $OUT_FILE && mv $OUT_FILE "${entry::-3}"
    done
fi
