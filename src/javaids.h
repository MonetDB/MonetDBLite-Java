/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2019 MonetDB B.V.
 */

#ifndef SRC_JAVAIDS_H
#define SRC_JAVAIDS_H

#include "monetdb_config.h"
#include "jni.h"

java_export int initializeIDS(JNIEnv *env);
java_export void releaseIDS(JNIEnv *env);

/* Embedded database environment Classes */

java_export jmethodID getMonetDBEmbeddedDatabaseConstructorID(void);
java_export jclass getMonetDBEmbeddedExceptionClassID(void);
java_export jclass getMonetDBEmbeddedConnectionClassID(void);
java_export jmethodID getMonetDBEmbeddedConnectionConstructorID(void);
java_export jclass getJDBCEmbeddedConnectionClassID(void);
java_export jmethodID getJDBCDBEmbeddedConnectionConstructorID(void);
java_export jclass getQueryResultSetID(void);
java_export jmethodID getQueryResultSetConstructorID(void);
java_export jclass getPreparedQueryResultSetClassID(void);
java_export jmethodID getPreparedQueryResultSetClassConstructorID(void);
java_export jclass getExecResultSetClassID(void);
java_export jmethodID getExecResultSetClassConstructorID(void);
java_export jclass getMonetDBTableClassID(void);
java_export jmethodID getMonetDBTableClassConstructorID(void);
java_export jfieldID getConnectionResultPointerID(void);

/* Java MonetDB mappings constructors */

java_export jclass getBooleanClassID(void);
java_export jmethodID getBooleanConstructorID(void);
java_export jclass getByteClassID(void);
java_export jmethodID getByteConstructorID(void);
java_export jclass getShortClassID(void);
java_export jmethodID getShortConstructorID(void);
java_export jclass getIntegerClassID(void);
java_export jmethodID getIntegerConstructorID(void);
java_export jclass getLongClassID(void);
java_export jmethodID getLongConstructorID(void);
java_export jclass getFloatClassID(void);
java_export jmethodID getFloatConstructorID(void);
java_export jclass getDoubleClassID(void);
java_export jmethodID getDoubleConstructorID(void);

java_export jclass getShortArrayClassID(void);
java_export jclass getIntegerArrayClassID(void);
java_export jclass getLongArrayClassID(void);
java_export jclass getFloatArrayClassID(void);
java_export jclass getDoubleArrayClassID(void);
java_export jclass getByteArrayClassID(void);
java_export jclass getByteMatrixClassID(void);
java_export jclass getBigDecimalClassID(void);

java_export jclass getBigDecimalArrayClassID(void);
java_export jmethodID getBigDecimalConstructorID(void);
java_export jclass getDateClassID(void);
java_export jclass getDateClassArrayID(void);
java_export jmethodID getDateConstructorID(void);
java_export jclass getTimeClassID(void);
java_export jclass getTimeArrayClassID(void);
java_export jmethodID getTimeConstructorID(void);
java_export jclass getTimestampClassID(void);
java_export jclass getTimestampArrayClassID(void);
java_export jmethodID getTimestampConstructorID(void);
java_export jclass getGregorianCalendarClassID(void);
java_export jmethodID getGregorianCalendarConstructorID(void);
java_export jmethodID getGregorianCalendarSetterID(void);
java_export jclass getStringClassID(void);
java_export jclass getStringArrayClassID(void);
java_export jmethodID getStringByteArrayConstructorID(void);

/* JDBC Embedded Connection */

java_export jfieldID getServerResponsesID(void);
java_export jfieldID getLastErrorID(void);
java_export jfieldID getLastResultSetPointerID(void);
java_export jfieldID getServerHeaderResponseID(void);
java_export jfieldID getLastServerResponseParametersID(void);
java_export jfieldID getLastServerResponseID(void);
java_export jclass getAutoCommitResponseClassID(void);
java_export jmethodID getAutoCommitResponseConstructorID(void);
java_export jclass getUpdateResponseClassID(void);
java_export jmethodID getUpdateResponseConstructorID(void);

/* MonetDB Table */

java_export jclass getMonetDBTableColumnClassID(void);
java_export jmethodID getMonetDBTableColumnConstructorID(void);
java_export jclass getMappingEnumID(void);
java_export jmethodID getGetEnumValueID(void);
java_export jfieldID getGetConnectionID(void);
java_export jfieldID getGetConnectionLongID(void);
java_export jfieldID getGetSchemaID(void);
java_export jfieldID getGetTableID(void);
java_export jfieldID getStructPointerID(void);

java_export jmethodID getBigDecimalToStringID(void);
java_export jmethodID getSetBigDecimalScaleID(void);
java_export jmethodID getDateToLongID(void);
java_export jmethodID getTimeToLongID(void);
java_export jmethodID getTimestampToLongID(void);

#endif //SRC_JAVAIDS_H
