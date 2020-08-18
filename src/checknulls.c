/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2019 MonetDB B.V.
 */

#include "checknulls.h"

#include "monetdb_config.h"
#include "jni.h"
#include "javaids.h"
#include "mtime.h"
#include "blob.h"

#define CHECK_NULLS_LEVEL_ONE(NAME, JAVA_CAST, NULL_CONST) \
	void check##NAME##Nulls(JNIEnv* env, jbooleanArray input, jint size, BAT* b) { \
		int i; \
		const JAVA_CAST* array = (const JAVA_CAST*) Tloc(b, 0); \
		jboolean* aux = (jboolean*) GDKmalloc(sizeof(jboolean) * size); \
		if(aux == NULL) { \
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
		} else { \
			for(i = 0 ; i < size ; i++) \
				aux[i] = (array[i] == NULL_CONST##_nil) ? JNI_TRUE : JNI_FALSE; \
			(*env)->SetBooleanArrayRegion(env, input, 0, size, aux); \
			GDKfree(aux); \
		} \
	}

CHECK_NULLS_LEVEL_ONE(Boolean, jboolean, bit)
CHECK_NULLS_LEVEL_ONE(Tinyint, jbyte, bte)
CHECK_NULLS_LEVEL_ONE(Smallint, jshort, sht)
CHECK_NULLS_LEVEL_ONE(Int, jint, int)
CHECK_NULLS_LEVEL_ONE(Bigint, jlong, lng)
CHECK_NULLS_LEVEL_ONE(Real, jfloat, flt)
CHECK_NULLS_LEVEL_ONE(Double, jdouble, dbl)
CHECK_NULLS_LEVEL_ONE(Date, jint, int)
CHECK_NULLS_LEVEL_ONE(Time, jlong, lng)
CHECK_NULLS_LEVEL_ONE(Timestamp, jlong, lng)
CHECK_NULLS_LEVEL_ONE(Oid, oid, oid)

#define GET_BAT_STRING_NULL      str nvalue = BUNtail(li, p);
#define CHECK_NULL_STRING_NULL   strcmp((str) str_nil, nvalue) == 0

#define GET_BAT_BLOB_NULL        blob* nvalue = (blob*) BUNtail(li, p);
#define CHECK_NULL_BLOB_NULL     nvalue->nitems == ~(size_t) 0

#define CHECK_NULLS_LEVEL_TWO(NAME, GET_ATOM, CHECK_ATOM) \
	void check##NAME##Nulls(JNIEnv* env, jbooleanArray input, jint size, BAT* b) { \
		BUN p, q; \
		BATiter li = bat_iterator(b); \
		jboolean* aux = (jboolean*) GDKmalloc(sizeof(jboolean) * size); \
		if(aux == NULL) { \
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
		} else { \
			int i = 0; \
			for (p = 0, q = (BUN) size; p < q; p++) { \
				GET_ATOM \
				aux[i++] = (CHECK_ATOM) ? JNI_TRUE : JNI_FALSE; \
			} \
			(*env)->SetBooleanArrayRegion(env, input, 0, size, aux); \
			GDKfree(aux); \
		} \
	}

CHECK_NULLS_LEVEL_TWO(String, GET_BAT_STRING_NULL, CHECK_NULL_STRING_NULL)
CHECK_NULLS_LEVEL_TWO(Blob, GET_BAT_BLOB_NULL, CHECK_NULL_BLOB_NULL)
