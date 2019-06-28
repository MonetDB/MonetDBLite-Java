/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2017 MonetDB B.V.
 */

#include "nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection.h"

#include "monetdb_config.h"
#include "mal_exception.h"
#include "monetdb_embedded.h"
#include "javaids.h"
#include "jresulset.h"
#include "res_table.h"
#include "mal_type.h"
#include "sql_querytype.h"

JNIEXPORT jboolean JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection_getAutoCommitInternal
	(JNIEnv *env, jobject jconnection, jlong connectionPointer) {
	int result;
	char *err;
	(void) env;
	(void) jconnection;

	if((err = monetdb_get_autocommit((monetdb_connection) connectionPointer, &result)) != MAL_SUCCEED) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), err);
		freeException(err);
	}
	return (result == 0) ? JNI_FALSE : JNI_TRUE;
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection_setAutoCommitInternal
	(JNIEnv *env, jobject jconnection, jlong connectionPointer, jboolean autoCommit) {
	char *err = NULL;
	int foundExc = 0, i = 0, toSet = (autoCommit == JNI_FALSE) ? 0 : 1;

	(void) env;
	(void) jconnection;
	if((err = monetdb_set_autocommit((monetdb_connection) connectionPointer, toSet)) != MAL_SUCCEED) {
		while(err[i] && !foundExc) {
			if(err[i] == '!')
				foundExc = 1;
			i++;
		}
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), err + (foundExc ? i : 0));
		freeException(err);
	}
}

static int executeQuery(JNIEnv *env, jlong connectionPointer, jstring query, jboolean execute, monetdb_result **output,
						int *query_type, lng *lastId, lng *rowCount, int *prepareID) {
	const char *query_string_tmp;
	char* err = NULL;
	int foundExc = 0, i = 0;

	(void) execute;
	if(connectionPointer == 0) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "Connection already closed?");
		return 1;
	}
	query_string_tmp = (*env)->GetStringUTFChars(env, query, NULL);
	if(query_string_tmp == NULL) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
		return 2;
	}
	// Execute the query
	err = monetdb_query((monetdb_connection) connectionPointer, (char*) query_string_tmp, output,
						rowCount, prepareID);
	(*env)->ReleaseStringUTFChars(env, query, query_string_tmp);
	if (err) {
		while(err[i] && !foundExc) {
			if(err[i] == '!')
				foundExc = 1;
			i++;
		}
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), err + (foundExc ? i : 0));
		freeException(err);
		return 3;
	}
	if(*output) {
		if(query_type)
			*query_type = (*output)->type;
		if(lastId)
			*lastId = (*output)->id;
	}
	return 0;
}

static jobject generateQueryResultSet(JNIEnv *env, jobject jconnection, jlong connectionPointer, monetdb_result *output,
									  int query_type, int prepareID) {
	size_t i, numberOfColumns;
	jobject result = NULL;
	jintArray typesIDs;
	jint* copy = NULL;
	JResultSet* thisResultSet = NULL;
	char* err = NULL;
	monetdb_connection conn = (monetdb_connection) connectionPointer;

	// Check if we had results, otherwise we send an exception
	if (output && (query_type == Q_TABLE || query_type == Q_PREPARE || query_type == Q_BLOCK) && output->ncols > 0) {
		numberOfColumns = output->ncols;
	} else {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "There query returned no results?");
		return result;
	}

	if((err = createResultSet(conn, &thisResultSet, output)) != MAL_SUCCEED) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), err);
		freeException(err);
	}
	copy = GDKmalloc(sizeof(jint) * numberOfColumns);
	typesIDs = (*env)->NewIntArray(env, numberOfColumns);
	if(copy == NULL || typesIDs == NULL) {
		if(copy)
			GDKfree(copy);
		freeResultSet(thisResultSet);
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
		return result;
	}

	for (i = 0; i < numberOfColumns; i++) {
		res_col* col = NULL;
		char* nextSQLName, *err;
		if((err = monetdb_result_fetch_rawcol(conn, &col, output, i)) != MAL_SUCCEED) {
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), err);
			freeException(err);
			break;
		}
		nextSQLName = col->type.type->sqlname;
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
		} else if(strncmp(nextSQLName, "oid", 3) == 0) {
			copy[i] = 14;
		} else {
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "Unknown MonetDB type");
		}
	}

	if((*env)->ExceptionCheck(env) == JNI_TRUE) {
		freeResultSet(thisResultSet);
	} else {
		(*env)->SetIntArrayRegion(env, typesIDs, 0, numberOfColumns, copy);
		if(prepareID) {
			//public PreparedQueryResultSet(MonetDBEmbeddedConnection connection, long structPointer, int numberOfColumns, int numberOfRows, int[] typesIDs, int preparedID)
			result = (*env)->NewObject(env, getPreparedQueryResultSetClassID(), getPreparedQueryResultSetClassConstructorID(), jconnection,
									   (jlong) thisResultSet, numberOfColumns, (jint) output->nrows, typesIDs, prepareID);
		} else {
			//QueryResultSet(MonetDBEmbeddedConnection connection, long structPointer, int numberOfColumns, int numberOfRows, int[] typesIDs)
			result = (*env)->NewObject(env, getQueryResultSetID(), getQueryResultSetConstructorID(), jconnection,
									   (jlong) thisResultSet, numberOfColumns, (jint) output->nrows, typesIDs);
		}
		if(result == NULL)
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
	}
	GDKfree(copy);
	return result;
}

JNIEXPORT jint JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection_sendUpdateInternal
	(JNIEnv *env, jobject jconnection, jlong connectionPointer, jstring query, jboolean execute) {
	monetdb_result *output = NULL;
	lng rowCount;
	jint returnValue = -1;
	int query_type = Q_UPDATE, res;

	(void) jconnection;
	res = executeQuery(env, connectionPointer, query, execute, &output, &query_type, NULL, &rowCount, NULL);
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
	int res, query_type = Q_TABLE;
	monetdb_result *output = NULL;

	res = executeQuery(env, connectionPointer, query, execute, &output, &query_type, NULL, NULL, NULL);
	if(res) {
		return NULL;
	} else if(query_type != Q_TABLE && query_type != Q_BLOCK) {
		char* other;
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "The query did not produce a result set");
		if(output && (other = monetdb_cleanup_result((monetdb_connection) connectionPointer, output)) != MAL_SUCCEED)
			freeException(other);
		return NULL;
	} else {
		return generateQueryResultSet(env, jconnection, connectionPointer, output, query_type, 0);
	}
}

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection_prepareStatementInternal
	(JNIEnv *env, jobject jconnection, jlong connectionPointer, jstring query, jboolean execute) {
	int query_type = Q_PREPARE, res, prepareID;
	monetdb_result *output = NULL;

	res = executeQuery(env, connectionPointer, query, execute, &output, &query_type, NULL, NULL, &prepareID);
	if(res) {
		return NULL;
	} else if(query_type != Q_PREPARE) {
		char* other;
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "The query did not produce a prepared statement");
		if(output && (other = monetdb_cleanup_result((monetdb_connection) connectionPointer, output)) != MAL_SUCCEED)
			freeException(other);
		return NULL;
	} else {
		return generateQueryResultSet(env, jconnection, connectionPointer, output, query_type, prepareID);
	}
}

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection_executePrepareStatementInternal
	(JNIEnv *env, jobject jconnection, jlong connectionPointer, jstring query, jboolean execute) {
	int query_type = Q_PREPARE, res;
	lng rowCount;
	monetdb_result *output = NULL;
	jobject resultSet = NULL, result;
	jint returnValue = -1;
	jboolean retStatus = JNI_FALSE;
	char* other;

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
		if(output && (other = monetdb_cleanup_result((monetdb_connection) connectionPointer, output)) != MAL_SUCCEED)
			freeException(other);
	}
	//public ExecResultSet(boolean status, QueryResultSet resultSet, int numberOfRows)
	result = (*env)->NewObject(env, getExecResultSetClassID(), getExecResultSetClassConstructorID(), retStatus, resultSet, returnValue);
	if(result == NULL)
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
	return result;
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection_executePrepareStatementAndIgnoreInternal
	(JNIEnv *env, jobject jconnection, jlong connectionPointer, jstring query, jboolean execute) {
	monetdb_result *output = NULL;
	char* other;
	(void) jconnection;

	(void) executeQuery(env, connectionPointer, query, execute, &output, NULL, NULL, NULL, NULL);
	if(output && (other = monetdb_cleanup_result((monetdb_connection) connectionPointer, output)) != MAL_SUCCEED)
		freeException(other);
}

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection_getMonetDBTableInternal
	(JNIEnv *env, jobject jconnection, jlong connectionPointer, jstring tableSchema, jstring tableName) {
	const char *schema_name_tmp, *table_name_tmp;
	char *err = NULL;
	sql_table* table;
	jobject result;

	if((schema_name_tmp = (*env)->GetStringUTFChars(env, tableSchema, NULL)) == NULL) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
		return NULL;
	}
	if((table_name_tmp = (*env)->GetStringUTFChars(env, tableName, NULL)) == NULL) {
		(*env)->ReleaseStringUTFChars(env, tableSchema, schema_name_tmp);
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
		return NULL;
	}

	err = monetdb_get_table((monetdb_connection) connectionPointer, &table, schema_name_tmp, table_name_tmp);
	(*env)->ReleaseStringUTFChars(env, tableSchema, schema_name_tmp);
	(*env)->ReleaseStringUTFChars(env, tableName, table_name_tmp);
	if (err) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), err);
		freeException(err);
		return NULL;
	}
	result = (*env)->NewObject(env, getMonetDBTableClassID(), getMonetDBTableClassConstructorID(), jconnection, tableSchema, tableName);
	if(result == NULL)
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
	return result;
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedConnection_closeConnectionInternal
	(JNIEnv *env, jobject jconnection, jlong connectionPointer) {
	char *err = NULL;
	(void) env;
	(void) jconnection;

	if ((err = monetdb_disconnect((monetdb_connection) connectionPointer)) != MAL_SUCCEED) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), err);
		freeException(err);
	}
}
