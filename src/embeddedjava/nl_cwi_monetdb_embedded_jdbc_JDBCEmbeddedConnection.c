/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2017 MonetDB B.V.
 */

#include "nl_cwi_monetdb_embedded_jdbc_JDBCEmbeddedConnection.h"

#include "monetdb_config.h"
#include "embedded.h"
#include "embeddedjvm.h"
#include "javaids.h"
#include "jresulset.h"
#include "res_table.h"
#include "gdk.h"

static void setErrorResponse(JNIEnv *env, jobject jdbccon, char* errorMessage) {
	jintArray lineResponse = (jintArray) (*env)->GetObjectField(env, jdbccon, getServerResponsesID());
	if(lineResponse == NULL) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "System out of memory!");
		GDKfree(errorMessage);
		return;
	}
	jint response[2] = {1,4}; //ERROR AND PROMPT
	(*env)->SetIntArrayRegion(env, lineResponse, 0, 2, response);
	(*env)->SetObjectField(env, jdbccon, getLastErrorID(), (*env)->NewStringUTF(env, errorMessage));
	GDKfree(errorMessage);
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_jdbc_JDBCEmbeddedConnection_getNextTableHeaderInternal
	(JNIEnv *env, jobject jdbccon, jlong resultSetPointer, jobjectArray columnNames, jintArray columnLengths,
	jobjectArray types, jobjectArray tableNames) {
	monetdb_result *output = (monetdb_result *) resultSetPointer;
	int numberOfColumns = (*env)->GetArrayLength(env, columnNames), i;
	jint* columnLengthsFound;
	jstring colname, sqlname, tablename;
	(void) jdbccon;

	if(numberOfColumns > 0) {
		columnLengthsFound = GDKmalloc(numberOfColumns * sizeof(jint));
		if(columnLengthsFound == NULL) {
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "System out of memory!");
			return;
		}

		for (i = 0; i < numberOfColumns; i++) {
			res_col* col = (res_col*) monetdb_result_fetch_rawcol(output, i);
			columnLengthsFound[i] = col->type.digits;
			colname = (*env)->NewStringUTF(env, col->name);
			sqlname = (*env)->NewStringUTF(env, col->type.type->sqlname);
			tablename = (*env)->NewStringUTF(env, col->tn);
			(*env)->SetObjectArrayElement(env, columnNames, i, colname);
			(*env)->SetObjectArrayElement(env, types, i, sqlname);
			(*env)->SetObjectArrayElement(env, tableNames, i, tablename);
			(*env)->DeleteLocalRef(env, colname);
			(*env)->DeleteLocalRef(env, sqlname);
			(*env)->DeleteLocalRef(env, tablename);
		}
		(*env)->SetIntArrayRegion(env, columnLengths, 0, numberOfColumns, columnLengthsFound);
		GDKfree(columnLengthsFound);
	}
	//Important! Don't free the result table yet!
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_jdbc_JDBCEmbeddedConnection_initializePointersInternal
	(JNIEnv *env, jobject jdbccon, jlong connectionPointer, jlong lastResultSetPointer,
		jobject embeddedDataBlockResponse) {
	monetdb_result* output = (monetdb_result*) lastResultSetPointer;
	JResultSet* thisResultSet;
	(void) jdbccon;

	thisResultSet = createResultSet((monetdb_connection) connectionPointer, output);
	if(thisResultSet == NULL) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "System out of memory!");
	}
	(*env)->SetLongField(env, embeddedDataBlockResponse, getStructPointerID(), (jlong) thisResultSet);
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_jdbc_JDBCEmbeddedConnection_sendQueryInternal
	(JNIEnv *env, jobject jdbccon, jlong connectionPointer, jstring query, jboolean execute) {
	lng rowCount = 0, prepareID = 0;
	size_t lastId = 0;
	int lineResponseCounter = 0, query_type = 0, autoCommitStatus = 1;
	jint nextResponses[4], responseParameters[3];
	const char *query_string_tmp;
	char *err = NULL;
	monetdb_result *output = NULL;
	jintArray lineResponse, lastServerResponseParameters;
	jobject result;
	monetdb_connection conn = (monetdb_connection) connectionPointer;

	if(conn == NULL) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "Connection already closed?");
		return;
	}

	query_string_tmp = (*env)->GetStringUTFChars(env, query, NULL);
	if(query_string_tmp == NULL) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "System out of memory!");
		return;
	}

	err = monetdb_query(conn, (char*) query_string_tmp, (char) execute, &output, &rowCount, &prepareID);
	(*env)->ReleaseStringUTFChars(env, query, query_string_tmp);
	if (err) { //if there are errors, set the error string and exit
		setErrorResponse(env, jdbccon, err);
		monetdb_cleanup_result(conn, output);
		return;
	}

	if(output) {
		query_type = (int) (output->type);
		lastId = (size_t) (output->id);
	}
	//set the result set pointer
	(*env)->SetLongField(env, jdbccon, getLastResultSetPointerID(), (jlong) output);
	nextResponses[lineResponseCounter++] = 6; //SOHEADER

	//set the next serverHeader
	(*env)->SetIntField(env, jdbccon, getServerHeaderResponseID(), query_type);

	// The SCHEMA responses (query_type == Q_SCHEMA), don't need anything from the server
	switch(query_type) {
		case Q_TABLE: //TABLE
		case Q_PREPARE: //PREPARE
		case Q_BLOCK: //BLOCK
			//set the Table Headers values
			if(output) {
				responseParameters[0] = (query_type == Q_PREPARE) ? (int) prepareID : (int) lastId;
				responseParameters[1] = (jint) output->nrows; //number of rows
			} else {
				responseParameters[0] = -1;
				responseParameters[1] = 0;
			}
			if(query_type == Q_TABLE || query_type == Q_PREPARE) {
				responseParameters[2] = (output) ? (jint) output->ncols : 0; //number of columns
			}
			//set the other headers
			nextResponses[lineResponseCounter++] = 2; //HEADER
			//IMPORTANT Due to the Embedded architecture, we can skip the RESULT header in the response
			lastServerResponseParameters = (jintArray) (*env)->GetObjectField(env, jdbccon,
																			  getLastServerResponseParametersID());
			if(lastServerResponseParameters == NULL) {
				(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "System out of memory!");
			} else {
				(*env)->SetIntArrayRegion(env, lastServerResponseParameters, 0, 3, responseParameters);
			}
			break;
		case Q_UPDATE: //UPDATE
			result = (*env)->NewObject(env, getUpdateResponseClassID(), getUpdateResponseConstructorID(),
									   (jint) lastId, (jint) rowCount);
			if(result == NULL) {
				(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "System out of memory!");
			}
			(*env)->SetObjectField(env, jdbccon, getLastServerResponseID(), result);
			break;
		case Q_TRANS: //TRANSACTION
			autoCommitStatus = getAutocommitFlag(conn);
			result = (*env)->NewObject(env, getAutoCommitResponseClassID(), getAutoCommitResponseConstructorID(),
									   (autoCommitStatus) ? JNI_TRUE : JNI_FALSE);
			if(result == NULL) {
				(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "System out of memory!");
			}
			(*env)->SetObjectField(env, jdbccon, getLastServerResponseID(), result);
			break;
	}
	nextResponses[lineResponseCounter++] = 4; //PROMPT
	//set the line response headers
	lineResponse = (jintArray) (*env)->GetObjectField(env, jdbccon, getServerResponsesID());
	if(lineResponse == NULL) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "System out of memory!");
	} else {
		(*env)->SetIntArrayRegion(env, lineResponse, 0, lineResponseCounter, nextResponses);
	}

	if(query_type != Q_TABLE && query_type != Q_PREPARE && output) { //if the result is not a table or a prepare, delete it right away
		monetdb_cleanup_result(conn, output);
	}
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_jdbc_JDBCEmbeddedConnection_sendAutocommitCommandInternal
	(JNIEnv *env, jobject jdbccon, jlong connectionPointer, jint flag) {
	int autoCommitStatus;
	jobject result;

	char *err = sendAutoCommitCommand((monetdb_connection) connectionPointer, flag, &autoCommitStatus);
	if (err) { //if there is an error set it and return
		setErrorResponse(env, jdbccon, err);
	} else {
		result = (*env)->NewObject(env, getAutoCommitResponseClassID(), getAutoCommitResponseConstructorID(),
								   (autoCommitStatus) ? JNI_TRUE : JNI_FALSE);
		if(result == NULL) {
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "System out of memory!");
		}
		(*env)->SetObjectField(env, jdbccon, getLastServerResponseID(), result);
	}
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_jdbc_JDBCEmbeddedConnection_sendReplySizeCommandInternal
	(JNIEnv *env, jobject jdbccon, jlong connectionPointer, jint size) {
	(void) env;
	(void) jdbccon;
	sendReplySizeCommand((monetdb_connection) connectionPointer, size);
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_jdbc_JDBCEmbeddedConnection_sendReleaseCommandInternal
	(JNIEnv *env, jobject jdbccon, jlong connectionPointer, jint commandId) {
	(void) env;
	(void) jdbccon;
	sendReleaseCommand((monetdb_connection) connectionPointer, commandId);
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_jdbc_JDBCEmbeddedConnection_sendCloseCommandInternal
	(JNIEnv *env, jobject jdbccon, jlong connectionPointer, jint commandId) {
	(void) env;
	(void) jdbccon;
	sendCloseCommand((monetdb_connection) connectionPointer, commandId);
}
