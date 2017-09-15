/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2017 MonetDB B.V.
 */

#include "nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection.h"

#include "monetdb_config.h"
#include "embedded.h"
#include "embeddedjvm.h"
#include "javaids.h"
#include "jresulset.h"
#include "res_table.h"
#include "mal_type.h"

JNIEXPORT jboolean JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection_getAutoCommitInternal
	(JNIEnv *env, jobject jconnection, jlong connectionPointer) {
	int result = getAutocommitFlag((monetdb_connection) connectionPointer);
	(void) env;
	(void) jconnection;
	return (result == 0) ? JNI_FALSE : JNI_TRUE;
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection_setAutoCommitInternal
	(JNIEnv *env, jobject jconnection, jlong connectionPointer, jboolean autoCommit) {
	int toSet = (autoCommit == JNI_FALSE) ? 0 : 1;
	(void) env;
	(void) jconnection;
	setAutocommitFlag((monetdb_connection) connectionPointer, toSet);
}

static int executeQuery(JNIEnv *env, jlong connectionPointer, jstring query, jboolean execute, monetdb_result **output,
						int *query_type, size_t *lastId, long *rowCount, long *prepareID) {
	const char *query_string_tmp;
	char* err = NULL;

	if(connectionPointer == 0) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "Connection already closed?");
		return 1;
	}
	query_string_tmp = (*env)->GetStringUTFChars(env, query, NULL);
	if(query_string_tmp == NULL) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "System out of memory!");
		return 2;
	}
	// Execute the query
	err = monetdb_query((monetdb_connection) connectionPointer, (char*) query_string_tmp, (char) execute, output, rowCount, prepareID);
	(*env)->ReleaseStringUTFChars(env, query, query_string_tmp);
	if (err) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), err);
		GDKfree(err);
		return 3;
	}
	if(*output) {
		if(query_type) {
			*query_type = (int) ((*output)->type);
		}
		if(lastId) {
			*lastId = (size_t) ((*output)->id);
		}
	}
	return 0;
}

static jobject generateQueryResultSet(JNIEnv *env, jobject jconnection, jlong connectionPointer, monetdb_result *output,
									  int query_type, long prepareID) {
	int i, numberOfColumns;
	jobject result = NULL;
	jintArray typesIDs;
	jint* copy = NULL;
	JResultSet* thisResultSet = NULL;

	// Check if we had results, otherwise we send an exception
	if (output && (query_type == Q_TABLE || query_type == Q_PREPARE || query_type == Q_BLOCK) && output->ncols > 0) {
		numberOfColumns = (int) output->ncols;
	} else {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "There query returned no results?");
		monetdb_cleanup_result((monetdb_connection) connectionPointer, output);
		return result;
	}

	copy = GDKmalloc(sizeof(jint) * numberOfColumns);
	thisResultSet = createResultSet((monetdb_connection) connectionPointer, output);
	if(copy == NULL || thisResultSet == NULL) {
		GDKfree(copy);
		freeResultSet(thisResultSet);
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "System out of memory!");
		return result;
	}
	typesIDs = (*env)->NewIntArray(env, numberOfColumns);
	if(typesIDs == NULL) {
		GDKfree(copy);
		freeResultSet(thisResultSet);
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "System out of memory!");
		return result;
	}

	for (i = 0; i < numberOfColumns; i++) {
		res_col* col = (res_col*) monetdb_result_fetch_rawcol(output, i);
		char* nextSQLName = col->type.type->sqlname;
		if(strncmp(nextSQLName, "boolean", 7) == 0) {
			copy[i] = 1;
		} else if(strncmp(nextSQLName, "tinyint", 7) == 0) {
			copy[i] = 2;
		} else if(strncmp(nextSQLName, "smallint", 8) == 0) {
			copy[i] = 3;
		} else if(strncmp(nextSQLName, "int", 3) == 0 || strncmp(nextSQLName, "month_interval", 14) == 0) {
			copy[i] = 4;
		} else if(strncmp(nextSQLName, "bigint", 6) == 0 || strncmp(nextSQLName, "sec_interval", 12) == 0) {
			copy[i] = 5;
		} else if(strncmp(nextSQLName, "real", 4) == 0) {
			copy[i] = 6;
		} else if(strncmp(nextSQLName, "double", 6) == 0) {
			copy[i] = 7;
		} else if(strncmp(nextSQLName, "char", 4) == 0 || strncmp(nextSQLName, "varchar", 7) == 0 || strncmp(nextSQLName, "clob", 4) == 0) {
			copy[i] = 8;
		} else if(strncmp(nextSQLName, "date", 4) == 0) {
			copy[i] = 9;
		} else if(strncmp(nextSQLName, "timestamp", 9) == 0 || strncmp(nextSQLName, "timestamptz", 11) == 0) { //WARNING must come before the time type!!!
			copy[i] = 10;
		} else if(strncmp(nextSQLName, "time", 4) == 0 || strncmp(nextSQLName, "timetz", 6) == 0) {
			copy[i] = 11;
		} else if(strncmp(nextSQLName, "blob", 4) == 0) {
			copy[i] = 12;
		} else if(strncmp(nextSQLName, "decimal", 7) == 0) {
			copy[i] = 13;
		} else {
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "Unknown MonetDB type!");
		}
	}

	if((*env)->ExceptionCheck(env) == JNI_TRUE) {
		freeResultSet(thisResultSet);
	} else {
		(*env)->SetIntArrayRegion(env, typesIDs, 0, numberOfColumns, copy);
		if(prepareID) {
			//public PreparedQueryResultSet(MonetDBEmbeddedConnection connection, long structPointer, int numberOfColumns, int numberOfRows, int[] typesIDs, long preparedID)
			result = (*env)->NewObject(env, getPreparedQueryResultSetClassID(), getPreparedQueryResultSetClassConstructorID(), jconnection,
									   (jlong) thisResultSet, numberOfColumns, (jint) output->nrows, typesIDs, (jlong) prepareID);
		} else {
			//QueryResultSet(MonetDBEmbeddedConnection connection, long structPointer, int numberOfColumns, int numberOfRows, int[] typesIDs)
			result = (*env)->NewObject(env, getQueryResultSetID(), getQueryResultSetConstructorID(), jconnection,
									   (jlong) thisResultSet, numberOfColumns, (jint) output->nrows, typesIDs);
		}
		if(result == NULL) {
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "System out of memory!");
		}
	}
	GDKfree(copy);
	return result;
}

JNIEXPORT jint JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection_sendUpdateInternal
	(JNIEnv *env, jobject jconnection, jlong connectionPointer, jstring query, jboolean execute) {
	monetdb_result *output = NULL;
	long rowCount;
	jint returnValue = -1;
	int query_type, res;

	(void) jconnection;
	res = executeQuery(env, connectionPointer, query, execute, &output, &query_type, NULL, &rowCount, NULL);
	monetdb_cleanup_result((monetdb_connection) connectionPointer, output);
	if(res) {
		return returnValue;
	} else if(query_type == Q_UPDATE) {
		returnValue = (jint) rowCount;
	} else if(query_type == Q_SCHEMA) {
		returnValue = -2;
	}
	return returnValue;
}

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection_sendQueryInternal
	(JNIEnv *env, jobject jconnection, jlong connectionPointer, jstring query, jboolean execute) {
	int res, query_type;
	monetdb_result *output = NULL;

	res = executeQuery(env, connectionPointer, query, execute, &output, &query_type, NULL, NULL, NULL);
	if(res) {
		return NULL;
	} else if(query_type != Q_TABLE && query_type != Q_BLOCK) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "The query did not produce a result set!");
		monetdb_cleanup_result((monetdb_connection) connectionPointer, output);
		return NULL;
	} else {
		return generateQueryResultSet(env, jconnection, connectionPointer, output, query_type, 0);
	}
}

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection_prepareStatementInternal
	(JNIEnv *env, jobject jconnection, jlong connectionPointer, jstring query, jboolean execute) {
	int query_type, res;
	monetdb_result *output = NULL;
	long prepareID;

	res = executeQuery(env, connectionPointer, query, execute, &output, &query_type, NULL, NULL, &prepareID);
	if(res) {
		return NULL;
	} else if(query_type != Q_PREPARE) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "The query did not produce a prepared statement!");
		monetdb_cleanup_result((monetdb_connection) connectionPointer, output);
		return NULL;
	} else {
		return generateQueryResultSet(env, jconnection, connectionPointer, output, query_type, prepareID);
	}
}

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection_executePrepareStatementInternal
	(JNIEnv *env, jobject jconnection, jlong connectionPointer, jstring query, jboolean execute) {
	int query_type, res;
	long rowCount;
	monetdb_result *output = NULL;
	jobject resultSet = NULL, result;
	jint returnValue = -1;
	jboolean retStatus = JNI_FALSE;

	res = executeQuery(env, connectionPointer, query, execute, &output, &query_type, NULL, &rowCount, NULL);
	if(res) {
		return NULL;
	} else if(query_type == Q_TABLE || query_type == Q_BLOCK || query_type == Q_PREPARE) {
		retStatus = JNI_TRUE;
		resultSet = generateQueryResultSet(env, jconnection, connectionPointer, output, query_type, 0);
	} else {
		if(query_type == Q_UPDATE) {
			returnValue = (jint) rowCount;
		} else if(query_type == Q_SCHEMA) {
			returnValue = -2;
		}
		monetdb_cleanup_result((monetdb_connection) connectionPointer, output);
	}
	//public ExecResultSet(boolean status, QueryResultSet resultSet, int numberOfRows)
	result = (*env)->NewObject(env, getExecResultSetClassID(), getExecResultSetClassConstructorID(), retStatus, resultSet, returnValue);
	if(result == NULL) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "System out of memory!");
	}
	return result;
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection_executePrepareStatementAndIgnoreInternal
	(JNIEnv *env, jobject jconnection, jlong connectionPointer, jstring query, jboolean execute) {
	monetdb_result *output = NULL;
	(void) jconnection;
	(void) executeQuery(env, connectionPointer, query, execute, &output, NULL, NULL, NULL, NULL);
	monetdb_cleanup_result((monetdb_connection) connectionPointer, output);
}

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection_getMonetDBTableInternal
	(JNIEnv *env, jobject jconnection, jlong connectionPointer, jstring tableSchema, jstring tableName) {
	const char *schema_name_tmp, *table_name_tmp;
	char *err = NULL;
	sql_table* table;
	jobject result;

	schema_name_tmp = (*env)->GetStringUTFChars(env, tableSchema, NULL);
	if(schema_name_tmp == NULL) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "System out of memory!");
		return NULL;
	}
	table_name_tmp = (*env)->GetStringUTFChars(env, tableName, NULL);
	if(table_name_tmp == NULL) {
		(*env)->ReleaseStringUTFChars(env, tableSchema, schema_name_tmp);
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "System out of memory!");
		return NULL;
	}

	err = monetdb_find_table((monetdb_connection) connectionPointer, &table, schema_name_tmp, table_name_tmp);
	(*env)->ReleaseStringUTFChars(env, tableSchema, schema_name_tmp);
	(*env)->ReleaseStringUTFChars(env, tableName, table_name_tmp);
	if (err) {
		GDKfree(err);
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), err);
		return NULL;
	}
	result = (*env)->NewObject(env, getMonetDBTableClassID(), getMonetDBTableClassConstructorID(), jconnection, tableSchema, tableName);
	if(result == NULL) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "System out of memory!");
	}
	return result;
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection_closeConnectionInternal
	(JNIEnv *env, jobject jconnection, jlong connectionPointer) {
	(void) env;
	(void) jconnection;

	monetdb_disconnect((monetdb_connection) connectionPointer);
}
