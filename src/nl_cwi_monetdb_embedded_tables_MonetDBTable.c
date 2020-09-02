/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2019 MonetDB B.V.
 */

#include "nl_cwi_monetdb_embedded_tables_MonetDBTable.h"

#include "monetdb_config.h"
#include "monetdb_embedded.h"
#include "mal_exception.h"
#include "res_table.h"
#include "converters.h"
#include "javaids.h"

static char* loadTable(JNIEnv *env, jobject monetDBTable, sql_table** table, int *ncols, jlong* connectionPointer) {
	char* err = NULL;
	jobject connection = (*env)->GetObjectField(env, monetDBTable, getGetConnectionID());
	jstring schemaName = (*env)->GetObjectField(env, monetDBTable, getGetSchemaID());
	jstring tableName = (*env)->GetObjectField(env, monetDBTable, getGetTableID());
	const char *schema = (*env)->GetStringUTFChars(env, schemaName, NULL);
	const char *name = (*env)->GetStringUTFChars(env, tableName, NULL);

	*connectionPointer = (*env)->GetLongField(env, connection, getGetConnectionLongID());
	if (!(err = monetdb_get_table((monetdb_connection) (*connectionPointer), table, schema, name)))
		*ncols = (*table)->columns.set->cnt;
	(*env)->ReleaseStringUTFChars(env, schemaName, schema);
	(*env)->ReleaseStringUTFChars(env, tableName, name);

	(*env)->DeleteLocalRef(env, connection);
	(*env)->DeleteLocalRef(env, schemaName);
	(*env)->DeleteLocalRef(env, tableName);

	return err;
}

#define LOADTABLEDATA \
	sql_table* tableData; \
	int ncols; \
	jlong connectionPointer; \
	node *n; \
	char* err = loadTable(env, monetDBTable, &tableData, &ncols, &connectionPointer);

#define AFTERLOAD \
	if (err) { \
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), err); \
		freeException(err); \
	}

JNIEXPORT jint JNICALL Java_nl_cwi_monetdb_embedded_tables_MonetDBTable_getNumberOfColumns
	(JNIEnv *env, jobject monetDBTable) {
	LOADTABLEDATA
	AFTERLOAD

	(void) n;
	if((*env)->ExceptionCheck(env) == JNI_TRUE) {
		return 0;
	} else {
		return ncols;
	}
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_tables_MonetDBTable_getColumnNamesInternal
	(JNIEnv *env, jobject monetDBTable, jobjectArray result) {
	LOADTABLEDATA
	AFTERLOAD

	for (n = tableData->columns.set->h; n; n = n->next) {
		sql_column *col = n->data;
		jstring colname = (*env)->NewStringUTF(env, col->base.name);
		if (!colname) {
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
			return;
		}
		(*env)->SetObjectArrayElement(env, result, col->colnr, colname);
		(*env)->DeleteLocalRef(env, colname);
	}
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_tables_MonetDBTable_getColumnTypesInternal
	(JNIEnv *env, jobject monetDBTable, jobjectArray result) {
	LOADTABLEDATA
	AFTERLOAD

	for (n = tableData->columns.set->h; n; n = n->next) {
		sql_column *col = n->data;
		jstring coltype = (*env)->NewStringUTF(env, col->type.type->sqlname);
		if (!coltype) {
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
			return;
		}
		(*env)->SetObjectArrayElement(env, result, col->colnr, coltype);
		(*env)->DeleteLocalRef(env, coltype);
	}
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_tables_MonetDBTable_getMappingsInternal
	(JNIEnv *env, jobject monetDBTable, jobjectArray result) {
	LOADTABLEDATA
	AFTERLOAD

	for (n = tableData->columns.set->h; n; n = n->next) {
		sql_column *col = n->data;
		jstring colsqlname = (*env)->NewStringUTF(env, col->type.type->sqlname);
		if (!colsqlname) {
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
			return;
		}
		jobject next = (*env)->CallStaticObjectMethod(env, getMappingEnumID(), getGetEnumValueID(), colsqlname);
		(*env)->SetObjectArrayElement(env, result, col->colnr, next);
		(*env)->DeleteLocalRef(env, next);
	}
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_tables_MonetDBTable_getColumnDigitsInternal
	(JNIEnv *env, jobject monetDBTable, jintArray result) {
	LOADTABLEDATA
	jint* fdigits;
	AFTERLOAD

	if (!(fdigits = GDKmalloc(ncols * sizeof(jint)))) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
		return;
	}

	for (n = tableData->columns.set->h; n; n = n->next) {
		sql_column *col = n->data;
		fdigits[col->colnr] = col->type.digits;
	}
	(*env)->SetIntArrayRegion(env, result, 0, ncols, fdigits);
	GDKfree(fdigits);
};

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_tables_MonetDBTable_getColumnScalesInternal
	(JNIEnv *env, jobject monetDBTable, jintArray result) {
	LOADTABLEDATA
	jint* fscales;
	AFTERLOAD

	if (!(fscales = GDKmalloc(ncols * sizeof(jint)))) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
		return;
	}

	for (n = tableData->columns.set->h; n; n = n->next) {
		sql_column *col = n->data;
		fscales[col->colnr] = col->type.scale;
	}
	(*env)->SetIntArrayRegion(env, result, 0, ncols, fscales);
	GDKfree(fscales);
};

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_tables_MonetDBTable_getColumnNullableIndexesInternal
	(JNIEnv *env, jobject monetDBTable, jbooleanArray result) {
	LOADTABLEDATA
	jboolean* fnulls;
	AFTERLOAD

	if (!(fnulls = GDKmalloc(ncols * sizeof(jboolean)))) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
		return;
	}

	for (n = tableData->columns.set->h; n; n = n->next) {
		sql_column *col = n->data;
		fnulls[col->colnr] = (jboolean) col->null;
	}
	(*env)->SetBooleanArrayRegion(env, result, 0, ncols, fnulls);
	GDKfree(fnulls);
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_tables_MonetDBTable_getColumnDefaultValuesInternal
	(JNIEnv *env, jobject monetDBTable, jobjectArray result) {
	LOADTABLEDATA
	AFTERLOAD

	for (n = tableData->columns.set->h; n; n = n->next) {
		sql_column *col = n->data;
		jstring defaultValue = (*env)->NewStringUTF(env, col->def);
		if (!defaultValue) {
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
			return;
		}
		(*env)->SetObjectArrayElement(env, result, col->colnr, defaultValue);
		(*env)->DeleteLocalRef(env, defaultValue);
	}
}

static jobject getColumnData(JNIEnv *env, sql_column *col) {
	jobject res = NULL;
	jstring sqlname = (*env)->NewStringUTF(env, col->type.type->sqlname);
	jstring colname = (*env)->NewStringUTF(env, col->base.name);
	jstring defaultValue = (*env)->NewStringUTF(env, col->def);

	if (sqlname && colname  && defaultValue) {
		res = (*env)->NewObject(env, getMonetDBTableColumnClassID(), getMonetDBTableColumnConstructorID(), sqlname, colname, col->type.digits, col->type.scale, defaultValue, (jboolean) col->null);
		if (!res)
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
	} else {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
	}
	if (sqlname)
		(*env)->DeleteLocalRef(env, sqlname);
	if (colname)
		(*env)->DeleteLocalRef(env, colname);
	if (defaultValue)
		(*env)->DeleteLocalRef(env, defaultValue);
	return res;
}

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_embedded_tables_MonetDBTable_getColumnMetadataByIndex
	(JNIEnv *env, jobject monetDBTable, jint index) {
	LOADTABLEDATA
	jobject res = NULL;
	AFTERLOAD

	index--;
	if(index > -1 && index < ncols) {
		for (n = tableData->columns.set->h; n; n = n->next) {
			sql_column *col = n->data;
			if(col->colnr == index) {
				res = getColumnData(env, col);
				break;
			}
		}
	}
	return res;
}

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_embedded_tables_MonetDBTable_getColumnMetadataByName
	(JNIEnv *env, jobject monetDBTable, jstring colname) {
	LOADTABLEDATA
	jobject res = NULL;
	const char *col_name_tmp;
	AFTERLOAD

	if (!(col_name_tmp = (*env)->GetStringUTFChars(env, colname, NULL))) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
		return NULL;
	}

	for (n = tableData->columns.set->h; n; n = n->next) {
		sql_column *col = n->data;
		if(!strcmp(col_name_tmp, col->base.name)) {
			res = getColumnData(env, col);
			break;
		}
	}
	(*env)->ReleaseStringUTFChars(env, colname, col_name_tmp);
	return res;
}

JNIEXPORT jobjectArray JNICALL Java_nl_cwi_monetdb_embedded_tables_MonetDBTable_getAllColumnsMetadata
	(JNIEnv *env, jobject monetDBTable) {
	LOADTABLEDATA
	jobjectArray result;
	AFTERLOAD

	if (!(result = (*env)->NewObjectArray(env, ncols, getMonetDBTableColumnClassID(), NULL))) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
		return NULL;
	}

	for (n = tableData->columns.set->h; n; n = n->next) {
		sql_column *col = n->data;
		jobject newColumn = getColumnData(env, col);
		if ((*env)->ExceptionCheck(env) == JNI_TRUE) {
			(*env)->DeleteLocalRef(env, result);
			return NULL;
		}
		(*env)->SetObjectArrayElement(env, result, col->colnr, newColumn);
		(*env)->DeleteLocalRef(env, newColumn);
	}
	return result;
}

#define CHECK_ARRAY_CLASS(METHOD, ARRAY_CLASS) \
	if((*env)->IsInstanceOf(env, nextArray, METHOD) == JNI_FALSE) { \
		err = createException(MAL, "append", "The array at column %d must be a %s array!", nextColumnIndex + 1, ARRAY_CLASS); \
		break; \
	}

JNIEXPORT jint JNICALL Java_nl_cwi_monetdb_embedded_tables_MonetDBTable_appendColumnsInternal
	(JNIEnv *env, jobject monetDBTable, jobjectArray columnData, jintArray javaIndexes, jint roundingMode) {
	LOADTABLEDATA

	jint *jindexes;
	bat* newdata = NULL;
	jsize numberOfRows, nextSize;
	sql_column *col;
	int nextMonetDBIndex, nextColumnIndex, foundExc = 0, i = 0;
	jint nextJavaIndex;
	BAT* nextBAT;
	jobject nextArray, columnDataZero;

	AFTERLOAD

	if ((*env)->ExceptionCheck(env) == JNI_TRUE)
		return -1;
	if (!(jindexes = (*env)->GetIntArrayElements(env, javaIndexes, NULL))) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
		return -1;
	}
	if (!(columnDataZero = (*env)->GetObjectArrayElement(env, columnData, 0))) {
		(*env)->ReleaseIntArrayElements(env, javaIndexes, jindexes, JNI_ABORT);
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
		return -1;
	}
	numberOfRows = (*env)->GetArrayLength(env, columnDataZero);
	if (!(newdata = GDKzalloc(ncols * sizeof(bat*)))) {
		(*env)->ReleaseIntArrayElements(env, javaIndexes, jindexes, JNI_ABORT);
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
		return -1;
	}

	for (n = tableData->columns.set->h; n; n = n->next) {
		col = n->data;
		nextMonetDBIndex = col->type.type->localtype;
		nextColumnIndex = col->colnr;
		nextJavaIndex = jindexes[nextColumnIndex];

		nextBAT = NULL;
		nextArray = (*env)->GetObjectArrayElement(env, columnData, nextColumnIndex);
		nextSize = (*env)->GetArrayLength(env, nextArray);
		if(nextSize != numberOfRows) {
			err = createException(MAL, "append", "The row sizes between columns are not consistent");
			break;
		}

		switch(nextJavaIndex) {
			case 0: //boolean
				CHECK_ARRAY_CLASS(getByteArrayClassID(), "byte")
				storeBooleanColumn(env, &nextBAT, (jbyteArray) nextArray, numberOfRows, nextMonetDBIndex);
				break;
			case 1: //char
			case 2: //varchar
			case 3: //clob
				CHECK_ARRAY_CLASS(getStringArrayClassID(), "java.lang.String")
				storeStringColumn(env, &nextBAT, (jobjectArray) nextArray, numberOfRows, nextMonetDBIndex);
				break;
			case 4: //tinyint
				CHECK_ARRAY_CLASS(getByteArrayClassID(), "byte")
				storeTinyintColumn(env, &nextBAT, (jbyteArray) nextArray, numberOfRows, nextMonetDBIndex);
				break;
			case 5: //smallint
				CHECK_ARRAY_CLASS(getShortArrayClassID(), "short")
				storeSmallintColumn(env, &nextBAT, (jshortArray) nextArray, numberOfRows, nextMonetDBIndex);
				break;
			case 6: //int
			case 11: //month_interval
				CHECK_ARRAY_CLASS(getIntegerArrayClassID(), "int")
				storeIntColumn(env, &nextBAT, (jintArray) nextArray, numberOfRows, nextMonetDBIndex);
				break;
			case 7: //bigint
			case 12: //second_interval
				CHECK_ARRAY_CLASS(getLongArrayClassID(), "long")
				storeBigintColumn(env, &nextBAT, (jlongArray) nextArray, numberOfRows, nextMonetDBIndex);
				break;
			case 8: //decimal
				CHECK_ARRAY_CLASS(getBigDecimalArrayClassID(), "java.math.BigDecimal")
				if(col->type.digits <= 2) {
					storeDecimalbteColumn(env, &nextBAT, (jobjectArray) nextArray, numberOfRows, nextMonetDBIndex, col->type.scale, roundingMode);
				} else if(col->type.digits > 2 && col->type.digits <= 4) {
					storeDecimalshtColumn(env, &nextBAT, (jobjectArray) nextArray, numberOfRows, nextMonetDBIndex, col->type.scale, roundingMode);
				} else if(col->type.digits > 4 && col->type.digits <= 8) {
					storeDecimalintColumn(env, &nextBAT, (jobjectArray) nextArray, numberOfRows, nextMonetDBIndex, col->type.scale, roundingMode);
				} else {
					storeDecimallngColumn(env, &nextBAT, (jobjectArray) nextArray, numberOfRows, nextMonetDBIndex, col->type.scale, roundingMode);
				}
				break;
			case 9: //real
				CHECK_ARRAY_CLASS(getFloatArrayClassID(), "float")
				storeRealColumn(env, &nextBAT, (jfloatArray) nextArray, numberOfRows, nextMonetDBIndex);
				break;
			case 10: //double
				CHECK_ARRAY_CLASS(getDoubleArrayClassID(), "double")
				storeDoubleColumn(env, &nextBAT, (jdoubleArray) nextArray, numberOfRows, nextMonetDBIndex);
				break;
			case 13: //time
			case 14: //timetz
				CHECK_ARRAY_CLASS(getTimeArrayClassID(), "java.sql.Time")
				storeTimeColumn(env, &nextBAT, (jobjectArray) nextArray, numberOfRows, nextMonetDBIndex);
				break;
			case 15: //date
				CHECK_ARRAY_CLASS(getDateClassArrayID(), "java.sql.Date")
				storeDateColumn(env, &nextBAT, (jobjectArray) nextArray, numberOfRows, nextMonetDBIndex);
				break;
			case 16: //timestamp
			case 17: //timestamptz
				CHECK_ARRAY_CLASS(getTimestampArrayClassID(), "java.sql.Timestamp")
				storeTimestampColumn(env, &nextBAT, (jobjectArray) nextArray, numberOfRows, nextMonetDBIndex);
				break;
			case 18: //blob
				CHECK_ARRAY_CLASS(getByteMatrixClassID(), "byte[]")
				storeBlobColumn(env, &nextBAT, (jobjectArray) nextArray, numberOfRows, nextMonetDBIndex);
				break;
			case 19: //oid
				CHECK_ARRAY_CLASS(getStringArrayClassID(), "java.lang.String")
				storeOidColumn(env, &nextBAT, (jobjectArray) nextArray, numberOfRows, nextMonetDBIndex);
				break;
			default:
				err = createException(MAL, "append", "Unknown Java mapping class");
		}
		(*env)->DeleteLocalRef(env, nextArray);
		if(!err) {
			newdata[nextColumnIndex] = nextBAT->batCacheid;
		} else {
			break;
		}
	}

	if(!err)
		err = monetdb_append((monetdb_connection) connectionPointer, tableData->s->base.name, tableData->base.name, newdata, ncols);
	(*env)->ReleaseIntArrayElements(env, javaIndexes, jindexes, JNI_ABORT);
	if (newdata) {
		for(int j = 0; j < ncols; j++) {
			if (newdata[j])
				BBPunfix(newdata[j]);
		}
		GDKfree(newdata);
	}

	if (err) {
		while(err[i] && !foundExc) {
			if(err[i] == '!')
				foundExc = 1;
			i++;
		}
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), err + (foundExc ? i : 0));
		freeException(err);
		return -1;
	} else {
		return numberOfRows;
	}
}
