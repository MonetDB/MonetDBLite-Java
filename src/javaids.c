/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2019 MonetDB B.V.
 */

#include "javaids.h"

#include "monetdb_config.h"
#include <jni.h>

/* Embedded database environment Classes */
static jmethodID monetDBEmbeddedDatabaseConstructorID = NULL;
static jclass monetDBEmbeddedExceptionClassID = NULL;
static jclass monetDBEmbeddedConnectionClassID = NULL;
static jmethodID monetDBEmbeddedConnectionConstructorID = NULL;
static jclass jDBCEmbeddedConnectionClassID = NULL;
static jmethodID jDBCDBEmbeddedConnectionConstructorID = NULL;

static jclass queryResultSetID = NULL;
static jmethodID queryResultSetConstructorID = NULL;
static jclass preparedQueryResultSetClassID = NULL;
static jmethodID preparedQueryResultSetClassConstructorID = NULL;
static jclass execResultSetClassID = NULL;
static jmethodID execResultSetClassConstructorID = NULL;
static jclass monetDBTableClassID = NULL;
static jmethodID monetDBTableClassConstructorID = NULL;

static jfieldID connectionResultPointerID = NULL;

/* Java MonetDB mappings constructors */
static jclass booleanClassID = NULL;
static jmethodID booleanConstructorID = NULL;
static jclass byteClassID = NULL;
static jmethodID byteConstructorID = NULL;
static jclass shortClassID = NULL;
static jmethodID shortConstructorID = NULL;
static jclass integerClassID = NULL;
static jmethodID integerConstructorID = NULL;
static jclass longClassID = NULL;
static jmethodID longConstructorID = NULL;
static jclass floatClassID = NULL;
static jmethodID floatConstructorID = NULL;
static jclass doubleClassID = NULL;
static jmethodID doubleConstructorID = NULL;

static jclass byteArrayClassID = NULL;
static jclass shortArrayClassID = NULL;
static jclass integerArrayClassID = NULL;
static jclass longArrayClassID = NULL;
static jclass floatArrayClassID = NULL;
static jclass doubleArrayClassID = NULL;
static jclass byteMatrixClassID = NULL;
static jclass bigDecimalClassID = NULL;
static jclass bigDecimalArrayClassID = NULL;
static jmethodID bigDecimalConstructorID = NULL;

static jclass dateClassID = NULL;
static jclass dateClassArrayID = NULL;
static jmethodID dateConstructorID = NULL;
static jclass timeClassID = NULL;
static jclass timeArrayClassID = NULL;
static jmethodID timeConstructorID = NULL;
static jclass timestampClassID = NULL;
static jclass timestampArrayClassID = NULL;
static jmethodID timestampConstructorID = NULL;
static jclass gregorianCalendarClassID = NULL;
static jmethodID gregorianCalendarConstructorID = NULL;
static jmethodID gregorianCalendarSetterID = NULL;

static jclass stringClassID = NULL;
static jclass stringArrayClassID = NULL;
static jmethodID stringByteArrayConstructorID = NULL;

/* JDBC Embedded Connection */

static jfieldID serverResponsesID = NULL;
static jfieldID lastErrorID = NULL;
static jfieldID lastResultSetPointerID = NULL;
static jfieldID serverHeaderResponseID = NULL;
static jfieldID lastServerResponseParametersID = NULL;
static jfieldID lastServerResponseID = NULL;
static jfieldID structPointerID = NULL;

static jclass autoCommitResponseClassID = NULL;
static jmethodID autoCommitResponseConstructorID = NULL;
static jclass updateResponseClassID = NULL;
static jmethodID updateResponseConstructorID = NULL;

/* MonetDB Table */

static jclass monetDBTableColumnClassID = NULL;
static jmethodID monetDBTableColumnConstructorID = NULL;
static jclass mappingEnumID = NULL;
static jmethodID getEnumValueID = NULL;

static jfieldID getConnectionID = NULL;
static jfieldID getConnectionLongID = NULL;
static jfieldID getSchemaID = NULL;
static jfieldID getTableID = NULL;

static jmethodID bigDecimalToStringID = NULL;
static jmethodID setBigDecimalScaleID = NULL;
static jmethodID dateToLongID = NULL;
static jmethodID timeToLongID = NULL;
static jmethodID timestampToLongID = NULL;

int initializeIDS(JNIEnv *env) {
	/* Embedded database environment Classes */

	jobject tempLocalRef;
	jclass embeddedDataBlockResponseClass, monetDBEmbeddedDatabaseClass, tableClass;

	monetDBEmbeddedDatabaseClass = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedDatabase");

	// private MonetDBEmbeddedDatabase(String dbDirectory, boolean silentFlag, boolean sequentialFlag)
	monetDBEmbeddedDatabaseConstructorID = (*env)->GetMethodID(env, monetDBEmbeddedDatabaseClass, "<init>", "(Ljava/lang/String;ZZ)V");
	(*env)->DeleteLocalRef(env, monetDBEmbeddedDatabaseClass);

	tempLocalRef = (jobject) (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedException");
	monetDBEmbeddedExceptionClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !monetDBEmbeddedExceptionClassID) {
		return 0;
	}

	tempLocalRef = (jobject) (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedConnection");
	monetDBEmbeddedConnectionClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !monetDBEmbeddedConnectionClassID) {
		return 0;
	}

	// protected MonetDBEmbeddedConnection(long connectionPointer)
	monetDBEmbeddedConnectionConstructorID = (*env)->GetMethodID(env, monetDBEmbeddedConnectionClassID, "<init>", "(J)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "nl/cwi/monetdb/embedded/jdbc/JDBCEmbeddedConnection");
	jDBCEmbeddedConnectionClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !jDBCEmbeddedConnectionClassID) {
		return 0;
	}

	// protected JDBCEmbeddedConnection(long connectionPointer)
	jDBCDBEmbeddedConnectionConstructorID = (*env)->GetMethodID(env, jDBCEmbeddedConnectionClassID, "<init>", "(J)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "nl/cwi/monetdb/embedded/resultset/QueryResultSet");
	queryResultSetID = (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !queryResultSetID) {
		return 0;
	}

	//QueryResultSetMonetDBEmbeddedConnection connection, long structPointer, int numberOfColumns, int numberOfRows, int[] typesIDs)
	queryResultSetConstructorID = (*env)->GetMethodID(env, queryResultSetID, "<init>", "(Lnl/cwi/monetdb/embedded/env/MonetDBEmbeddedConnection;JII[I)V");

	connectionResultPointerID = (*env)->GetFieldID(env, queryResultSetID, "structPointer", "J");

	tempLocalRef = (jobject) (*env)->FindClass(env, "nl/cwi/monetdb/embedded/resultset/PreparedQueryResultSet");
	preparedQueryResultSetClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !preparedQueryResultSetClassID) {
		return 0;
	}

	//public PreparedQueryResultSet(MonetDBEmbeddedConnection connection, long structPointer, int numberOfColumns, int numberOfRows, int[] typesIDs, int preparedID)
	preparedQueryResultSetClassConstructorID = (*env)->GetMethodID(env, preparedQueryResultSetClassID, "<init>", "(Lnl/cwi/monetdb/embedded/env/MonetDBEmbeddedConnection;JII[II)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "nl/cwi/monetdb/embedded/resultset/ExecResultSet");
	execResultSetClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !execResultSetClassID) {
		return 0;
	}

	//public ExecResultSet(boolean status, QueryResultSet resultSet, int numberOfRows)
	execResultSetClassConstructorID = (*env)->GetMethodID(env, execResultSetClassID, "<init>", "(ZLnl/cwi/monetdb/embedded/resultset/QueryResultSet;I)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "nl/cwi/monetdb/embedded/tables/MonetDBTable");
	monetDBTableClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !monetDBTableClassID) {
		return 0;
	}

	//public MonetDBTable(MonetDBEmbeddedConnection connection, String tableSchema, String tableName)
	monetDBTableClassConstructorID = (*env)->GetMethodID(env, monetDBTableClassID, "<init>", "(Lnl/cwi/monetdb/embedded/env/MonetDBEmbeddedConnection;Ljava/lang/String;Ljava/lang/String;)V");

	/* Java MonetDB mappings constructors */
	tempLocalRef = (jobject) (*env)->FindClass(env, "java/lang/Boolean");
	booleanClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !booleanClassID) {
		return 0;
	}

	booleanConstructorID = (*env)->GetMethodID(env, booleanClassID, "<init>", "(Z)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "java/lang/Byte");
	byteClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !byteClassID) {
		return 0;
	}

	byteConstructorID = (*env)->GetMethodID(env, byteClassID, "<init>", "(B)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "java/lang/Short");
	shortClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !shortClassID) {
		return 0;
	}

	shortConstructorID = (*env)->GetMethodID(env, shortClassID, "<init>", "(S)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "java/lang/Integer");
	integerClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !integerClassID) {
		return 0;
	}

	integerConstructorID = (*env)->GetMethodID(env, integerClassID, "<init>", "(I)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "java/lang/Long");
	longClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !longClassID) {
		return 0;
	}

	longConstructorID = (*env)->GetMethodID(env, longClassID, "<init>", "(J)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "java/lang/Float");
	floatClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !floatClassID) {
		return 0;
	}

	floatConstructorID = (*env)->GetMethodID(env, floatClassID, "<init>", "(F)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "java/lang/Double");
	doubleClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !doubleClassID) {
		return 0;
	}

	doubleConstructorID = (*env)->GetMethodID(env, doubleClassID, "<init>", "(D)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "[B");
	byteArrayClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !byteArrayClassID) {
		return 0;
	}

	tempLocalRef = (jobject) (*env)->FindClass(env, "[S");
	shortArrayClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !shortArrayClassID) {
		return 0;
	}

	tempLocalRef = (jobject) (*env)->FindClass(env, "[I");
	integerArrayClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !integerArrayClassID) {
		return 0;
	}

	tempLocalRef = (jobject) (*env)->FindClass(env, "[J");
	longArrayClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !longArrayClassID) {
		return 0;
	}

	tempLocalRef = (jobject) (*env)->FindClass(env, "[F");
	floatArrayClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !floatArrayClassID) {
		return 0;
	}

	tempLocalRef = (jobject) (*env)->FindClass(env, "[D");
	doubleArrayClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !doubleArrayClassID) {
		return 0;
	}

	tempLocalRef = (jobject) (*env)->FindClass(env, "[[B");
	byteMatrixClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !byteMatrixClassID) {
		return 0;
	}

	tempLocalRef = (jobject) (*env)->FindClass(env, "java/math/BigDecimal");
	bigDecimalClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !bigDecimalClassID) {
		return 0;
	}

	tempLocalRef = (jobject) (*env)->FindClass(env, "[Ljava/math/BigDecimal;");
	bigDecimalArrayClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !bigDecimalArrayClassID) {
		return 0;
	}

	bigDecimalConstructorID = (*env)->GetMethodID(env, bigDecimalClassID, "<init>", "(Ljava/lang/String;)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "java/sql/Date");
	dateClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !dateClassID) {
		return 0;
	}

	tempLocalRef = (jobject) (*env)->FindClass(env, "[Ljava/sql/Date;");
	dateClassArrayID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !dateClassArrayID) {
		return 0;
	}

	dateConstructorID = (*env)->GetMethodID(env, dateClassID, "<init>", "(J)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "java/sql/Time");
	timeClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !timeClassID) {
		return 0;
	}

	tempLocalRef = (jobject) (*env)->FindClass(env, "[Ljava/sql/Time;");
	timeArrayClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !timeArrayClassID) {
		return 0;
	}

	timeConstructorID = (*env)->GetMethodID(env, timeClassID, "<init>", "(J)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "java/sql/Timestamp");
	timestampClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !timestampClassID) {
		return 0;
	}

	tempLocalRef = (jobject) (*env)->FindClass(env, "[Ljava/sql/Timestamp;");
	timestampArrayClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !timestampArrayClassID) {
		return 0;
	}

	timestampConstructorID = (*env)->GetMethodID(env, timestampClassID, "<init>", "(J)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "java/util/GregorianCalendar");
	gregorianCalendarClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !gregorianCalendarClassID) {
		return 0;
	}

	gregorianCalendarConstructorID = (*env)->GetMethodID(env, gregorianCalendarClassID, "<init>", "()V");
	gregorianCalendarSetterID = (*env)->GetMethodID(env, gregorianCalendarClassID, "setTimeInMillis", "(J)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "java/lang/String");
	stringClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !stringClassID) {
		return 0;
	}

	tempLocalRef = (jobject) (*env)->FindClass(env, "[Ljava/lang/String;");
	stringArrayClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !stringArrayClassID) {
		return 0;
	}

	stringByteArrayConstructorID = (*env)->GetMethodID(env, stringClassID, "<init>", "([B)V");

	/* JDBC Embedded Connection */

	serverResponsesID = (*env)->GetFieldID(env, jDBCEmbeddedConnectionClassID, "lineResponse", "[I");
	lastErrorID = (*env)->GetFieldID(env, jDBCEmbeddedConnectionClassID, "lastError", "Ljava/lang/String;");
	lastResultSetPointerID = (*env)->GetFieldID(env, jDBCEmbeddedConnectionClassID, "lastResultSetPointer", "J");
	serverHeaderResponseID = (*env)->GetFieldID(env, jDBCEmbeddedConnectionClassID, "serverHeaderResponse", "I");
	lastServerResponseParametersID = (*env)->GetFieldID(env, jDBCEmbeddedConnectionClassID, "lastServerResponseParameters", "[I");
	lastServerResponseID = (*env)->GetFieldID(env, jDBCEmbeddedConnectionClassID, "lastServerResponse", "Lnl/cwi/monetdb/mcl/responses/IResponse;");

	embeddedDataBlockResponseClass = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/jdbc/EmbeddedDataBlockResponse");
	structPointerID = (*env)->GetFieldID(env, embeddedDataBlockResponseClass, "structPointer", "J");
	(*env)->DeleteLocalRef(env, (jobject) embeddedDataBlockResponseClass);

	tempLocalRef = (jobject) (*env)->FindClass(env, "nl/cwi/monetdb/mcl/responses/AutoCommitResponse");
	autoCommitResponseClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !autoCommitResponseClassID) {
		return 0;
	}

	autoCommitResponseConstructorID = (*env)->GetMethodID(env, autoCommitResponseClassID, "<init>", "(Z)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "nl/cwi/monetdb/mcl/responses/UpdateResponse");
	updateResponseClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !updateResponseClassID) {
		return 0;
	}

	updateResponseConstructorID = (*env)->GetMethodID(env, updateResponseClassID, "<init>", "(II)V");

	tempLocalRef = (jobject) (*env)->FindClass(env, "nl/cwi/monetdb/embedded/tables/MonetDBTableColumn");
	monetDBTableColumnClassID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !monetDBTableColumnClassID) {
		return 0;
	}

	//public MonetDBTableColumn(String columnType, String columnName, int columnDigits, int columnScale, String defaultValue, boolean isNullable)
	monetDBTableColumnConstructorID = (*env)->GetMethodID(env, monetDBTableColumnClassID, "<init>", "(Ljava/lang/String;Ljava/lang/String;IILjava/lang/String;Z)V");

	tempLocalRef = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/mapping/MonetDBToJavaMapping");
	mappingEnumID = (jclass) (*env)->NewGlobalRef(env, tempLocalRef);
	(*env)->DeleteLocalRef(env, tempLocalRef);
	if(!tempLocalRef || !mappingEnumID) {
		return 0;
	}

	getEnumValueID = (*env)->GetStaticMethodID(env, mappingEnumID, "getJavaMappingFromMonetDBString", "(Ljava/lang/String;)Lnl/cwi/monetdb/embedded/mapping/MonetDBToJavaMapping;");
	tableClass = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/tables/MonetDBTable");
	if(!tableClass) {
		return 0;
	}

	getConnectionID = (*env)->GetFieldID(env, tableClass, "connection", "Lnl/cwi/monetdb/embedded/env/MonetDBEmbeddedConnection;");
	getConnectionLongID = (*env)->GetFieldID(env, monetDBEmbeddedConnectionClassID, "connectionPointer", "J");
	getSchemaID = (*env)->GetFieldID(env, tableClass, "tableSchema", "Ljava/lang/String;");
	getTableID = (*env)->GetFieldID(env, tableClass, "tableName", "Ljava/lang/String;");
	(*env)->DeleteLocalRef(env, tableClass);

	bigDecimalToStringID = (*env)->GetMethodID(env, bigDecimalClassID, "toPlainString", "()Ljava/lang/String;");
	setBigDecimalScaleID = (*env)->GetMethodID(env, bigDecimalClassID, "setScale", "(II)Ljava/math/BigDecimal;");
	dateToLongID = (*env)->GetMethodID(env, dateClassID, "getTime", "()J");
	timeToLongID = (*env)->GetMethodID(env, timeClassID, "getTime", "()J");
	timestampToLongID = (*env)->GetMethodID(env, timestampClassID, "getTime", "()J");

	return 1;
}

void releaseIDS(JNIEnv *env) {
	/* Embedded database environment Classes */
	if(monetDBEmbeddedExceptionClassID) {
		(*env)->DeleteGlobalRef(env, monetDBEmbeddedExceptionClassID);
		monetDBEmbeddedExceptionClassID = NULL;
	}
	if(jDBCEmbeddedConnectionClassID) {
		(*env)->DeleteGlobalRef(env, jDBCEmbeddedConnectionClassID);
		jDBCEmbeddedConnectionClassID = NULL;
	}
	if(queryResultSetID) {
		(*env)->DeleteGlobalRef(env, queryResultSetID);
		queryResultSetID = NULL;
	}
	if(preparedQueryResultSetClassID) {
		(*env)->DeleteGlobalRef(env, preparedQueryResultSetClassID);
		preparedQueryResultSetClassID = NULL;
	}
	if(execResultSetClassID) {
		(*env)->DeleteGlobalRef(env, execResultSetClassID);
		execResultSetClassID = NULL;
	}
	if(monetDBTableClassID) {
		(*env)->DeleteGlobalRef(env, monetDBTableClassID);
		monetDBTableClassID = NULL;
	}
	/* Java MonetDB mappings classes */
	if(booleanClassID) {
		(*env)->DeleteGlobalRef(env, booleanClassID);
		booleanClassID = NULL;
	}
	if(byteClassID) {
		(*env)->DeleteGlobalRef(env, byteClassID);
		byteClassID = NULL;
	}
	if(shortClassID) {
		(*env)->DeleteGlobalRef(env, shortClassID);
		shortClassID = NULL;
	}
	if(integerClassID) {
		(*env)->DeleteGlobalRef(env, integerClassID);
		integerClassID = NULL;
	}
	if(longClassID) {
		(*env)->DeleteGlobalRef(env, longClassID);
		longClassID = NULL;
	}
	if(floatClassID) {
		(*env)->DeleteGlobalRef(env, floatClassID);
		floatClassID = NULL;
	}
	if(doubleClassID) {
		(*env)->DeleteGlobalRef(env, doubleClassID);
		doubleClassID = NULL;
	}
	if(byteArrayClassID) {
		(*env)->DeleteGlobalRef(env, byteArrayClassID);
		byteArrayClassID = NULL;
	}
	if(shortArrayClassID) {
		(*env)->DeleteGlobalRef(env, shortArrayClassID);
		shortArrayClassID = NULL;
	}
	if(integerArrayClassID) {
		(*env)->DeleteGlobalRef(env, integerArrayClassID);
		integerArrayClassID = NULL;
	}
	if(longArrayClassID) {
		(*env)->DeleteGlobalRef(env, longArrayClassID);
		longArrayClassID = NULL;
	}
	if(floatArrayClassID) {
		(*env)->DeleteGlobalRef(env, floatArrayClassID);
		floatArrayClassID = NULL;
	}
	if(doubleArrayClassID) {
		(*env)->DeleteGlobalRef(env, doubleArrayClassID);
		doubleArrayClassID = NULL;
	}
	if(byteMatrixClassID) {
		(*env)->DeleteGlobalRef(env, byteMatrixClassID);
		byteMatrixClassID = NULL;
	}
	if(bigDecimalClassID) {
		(*env)->DeleteGlobalRef(env, bigDecimalClassID);
		bigDecimalClassID = NULL;
	}
	if(bigDecimalArrayClassID) {
		(*env)->DeleteGlobalRef(env, bigDecimalArrayClassID);
		bigDecimalArrayClassID = NULL;
	}
	if(dateClassID) {
		(*env)->DeleteGlobalRef(env, dateClassID);
		dateClassID = NULL;
	}
	if(dateClassArrayID) {
		(*env)->DeleteGlobalRef(env, dateClassArrayID);
		dateClassArrayID = NULL;
	}
	if(timeClassID) {
		(*env)->DeleteGlobalRef(env, timeClassID);
		timeClassID = NULL;
	}
	if(timeArrayClassID) {
		(*env)->DeleteGlobalRef(env, timeArrayClassID);
		timeArrayClassID = NULL;
	}
	if(timestampClassID) {
		(*env)->DeleteGlobalRef(env, timestampClassID);
		timestampClassID = NULL;
	}
	if(timestampArrayClassID) {
		(*env)->DeleteGlobalRef(env, timestampArrayClassID);
		timestampArrayClassID = NULL;
	}
	if(gregorianCalendarClassID) {
		(*env)->DeleteGlobalRef(env, gregorianCalendarClassID);
		gregorianCalendarClassID = NULL;
	}
	if(gregorianCalendarClassID) {
		(*env)->DeleteGlobalRef(env, gregorianCalendarClassID);
		gregorianCalendarClassID = NULL;
	}
	if(stringClassID) {
		(*env)->DeleteGlobalRef(env, stringClassID);
		stringClassID = NULL;
	}
	if(stringArrayClassID) {
		(*env)->DeleteGlobalRef(env, stringArrayClassID);
		stringArrayClassID = NULL;
	}
	/* JDBC Embedded Connection */
	if(autoCommitResponseClassID) {
		(*env)->DeleteGlobalRef(env, autoCommitResponseClassID);
		autoCommitResponseClassID = NULL;
	}
	if(updateResponseClassID) {
		(*env)->DeleteGlobalRef(env, updateResponseClassID);
		updateResponseClassID = NULL;
	}
	/* MonetDB Table */
	if(monetDBTableColumnClassID) {
		(*env)->DeleteGlobalRef(env, monetDBTableColumnClassID);
		monetDBTableColumnClassID = NULL;
	}
	if(mappingEnumID) {
		(*env)->DeleteGlobalRef(env, mappingEnumID);
		mappingEnumID = NULL;
	}
}

/* Embedded database environment Classes */

jmethodID getMonetDBEmbeddedDatabaseConstructorID(void) {
	return monetDBEmbeddedDatabaseConstructorID;
}

jclass getMonetDBEmbeddedExceptionClassID(void) {
	return monetDBEmbeddedExceptionClassID;
}

jclass getMonetDBEmbeddedConnectionClassID(void) {
	return monetDBEmbeddedConnectionClassID;
}

jmethodID getMonetDBEmbeddedConnectionConstructorID(void) {
	return monetDBEmbeddedConnectionConstructorID;
}

jclass getJDBCEmbeddedConnectionClassID(void) {
	return jDBCEmbeddedConnectionClassID;
}

jmethodID getJDBCDBEmbeddedConnectionConstructorID(void) {
	return jDBCDBEmbeddedConnectionConstructorID;
}

jclass getQueryResultSetID(void) {
	return queryResultSetID;
}

jmethodID getQueryResultSetConstructorID(void) {
	return queryResultSetConstructorID;
}

jclass getPreparedQueryResultSetClassID(void) {
	return preparedQueryResultSetClassID;
}

jmethodID getPreparedQueryResultSetClassConstructorID(void) {
	return preparedQueryResultSetClassConstructorID;
}

jclass getExecResultSetClassID(void) {
	return execResultSetClassID;
}

jmethodID getExecResultSetClassConstructorID(void) {
	return execResultSetClassConstructorID;
}

jclass getMonetDBTableClassID(void) {
	return monetDBTableClassID;
}

jmethodID getMonetDBTableClassConstructorID(void) {
	return monetDBTableClassConstructorID;
}

jfieldID getConnectionResultPointerID(void) {
	return connectionResultPointerID;
}

/* Java MonetDB mappings constructors */

jclass getBooleanClassID(void) {
	return booleanClassID;
}

jmethodID getBooleanConstructorID(void) {
	return booleanConstructorID;
}

jclass getByteClassID(void) {
	return byteClassID;
}

jmethodID getByteConstructorID(void) {
	return byteConstructorID;
}

jclass getShortClassID(void) {
	return shortClassID;
}

jmethodID getShortConstructorID(void) {
	return shortConstructorID;
}

jclass getIntegerClassID(void) {
	return integerClassID;
}

jmethodID getIntegerConstructorID(void) {
	return integerConstructorID;
}

jclass getLongClassID(void) {
	return longClassID;
}

jmethodID getLongConstructorID(void) {
	return longConstructorID;
}

jclass getFloatClassID(void) {
	return floatClassID;
}

jmethodID getFloatConstructorID(void) {
	return floatConstructorID;
}

jclass getDoubleClassID(void) {
	return doubleClassID;
}

jmethodID getDoubleConstructorID(void) {
	return doubleConstructorID;
}

jclass getByteArrayClassID(void) {
	return byteArrayClassID;
}

jclass getShortArrayClassID(void) {
	return shortArrayClassID;
}

jclass getIntegerArrayClassID(void) {
	return integerArrayClassID;
}

jclass getLongArrayClassID(void) {
	return longArrayClassID;
}

jclass getFloatArrayClassID(void) {
	return floatArrayClassID;
}

jclass getDoubleArrayClassID(void) {
	return doubleArrayClassID;
}

jclass getByteMatrixClassID(void) {
	return byteMatrixClassID;
}

jclass getBigDecimalClassID(void) {
	return bigDecimalClassID;
}

jclass getBigDecimalArrayClassID(void) {
	return bigDecimalArrayClassID;
}

jmethodID getBigDecimalConstructorID(void) {
	return bigDecimalConstructorID;
}

jclass getDateClassID(void) {
	return dateClassID;
}

jclass getDateClassArrayID(void) {
	return dateClassArrayID;
}

jmethodID getDateConstructorID(void) {
	return dateConstructorID;
}

jclass getTimeClassID(void) {
	return timeClassID;
}

jclass getTimeArrayClassID(void) {
	return timeArrayClassID;
}

jmethodID getTimeConstructorID(void) {
	return timeConstructorID;
}

jclass getTimestampClassID(void) {
	return timestampClassID;
}

jclass getTimestampArrayClassID(void) {
	return timestampArrayClassID;
}

jmethodID getTimestampConstructorID(void) {
	return timestampConstructorID;
}

jclass getGregorianCalendarClassID(void) {
	return gregorianCalendarClassID;
}

jmethodID getGregorianCalendarConstructorID(void) {
	return gregorianCalendarConstructorID;
}

jmethodID getGregorianCalendarSetterID(void) {
	return gregorianCalendarSetterID;
}

jclass getStringClassID(void) {
	return stringClassID;
}

jclass getStringArrayClassID(void) {
	return stringArrayClassID;
}

jmethodID getStringByteArrayConstructorID(void) {
	return stringByteArrayConstructorID;
}

/* JDBC Embedded Connection */

jfieldID getServerResponsesID(void) {
	return serverResponsesID;
}

jfieldID getLastErrorID(void) {
	return lastErrorID;
}

jfieldID getLastResultSetPointerID(void) {
	return lastResultSetPointerID;
}

jfieldID getServerHeaderResponseID(void) {
	return serverHeaderResponseID;
}

jfieldID getLastServerResponseParametersID(void) {
	return lastServerResponseParametersID;
}

jfieldID getLastServerResponseID(void) {
	return lastServerResponseID;
}

jfieldID getStructPointerID(void) {
	return structPointerID;
}

jclass getAutoCommitResponseClassID(void) {
	return autoCommitResponseClassID;
}

jmethodID getAutoCommitResponseConstructorID(void) {
	return autoCommitResponseConstructorID;
}

jclass getUpdateResponseClassID(void) {
	return updateResponseClassID;
}

jmethodID getUpdateResponseConstructorID(void) {
	return updateResponseConstructorID;
}

/* MonetDB Table */

jclass getMonetDBTableColumnClassID(void) {
	return monetDBTableColumnClassID;
}

jmethodID getMonetDBTableColumnConstructorID(void) {
	return monetDBTableColumnConstructorID;
}

jclass getMappingEnumID(void) {
	return mappingEnumID;
}

jmethodID getGetEnumValueID(void) {
	return getEnumValueID;
}

jfieldID getGetConnectionID(void) {
	return getConnectionID;
}

jfieldID getGetConnectionLongID(void) {
	return getConnectionLongID;
}

jfieldID getGetSchemaID(void) {
	return getSchemaID;
}

jfieldID getGetTableID(void) {
	return getTableID;
}

jmethodID getBigDecimalToStringID(void) {
	return bigDecimalToStringID;
}

jmethodID getSetBigDecimalScaleID(void) {
	return setBigDecimalScaleID;
}

jmethodID getDateToLongID(void) {
	return dateToLongID;
}

jmethodID getTimeToLongID(void) {
	return timeToLongID;
}

jmethodID getTimestampToLongID(void) {
	return timestampToLongID;
}
