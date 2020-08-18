/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2019 MonetDB B.V.
 */

#include "converters.h"

#include "monetdb_config.h"
#include "jni.h"

#include <limits.h>
#include "javaids.h"
#include "gdk.h"
#include "blob.h"
#include "mtime.h"
#include "sql_decimal.h"

#define DO_NOTHING                        ;
#define OID_STRING_BUFFER_SIZE            32
#define min(a, b)                         (((a) < (b)) ? (a) : (b))

/* -- Get just a single value -- */

/* For primitive types the mapping is direct <3 */

#define FETCHING_LEVEL_ONE(NAME, JAVACAST) \
	JAVACAST get##NAME##Single(jint position, BAT* b) { \
		const JAVACAST* array = (const JAVACAST*) Tloc(b, 0); \
		return array[position]; \
	}

FETCHING_LEVEL_ONE(Tinyint, jbyte)
FETCHING_LEVEL_ONE(Smallint, jshort)
FETCHING_LEVEL_ONE(Int, jint)
FETCHING_LEVEL_ONE(Bigint, jlong)
FETCHING_LEVEL_ONE(Real, jfloat)
FETCHING_LEVEL_ONE(Double, jdouble)

/* For dates we have to create Java objects :( */

/* Convert monetdb dates to milliseconds since 1 jan 1970. We store year/month/day in the date representation */
#define GET_NEXT_JDATE           value = ((jlong) date_dayofyear(nvalue) - 1) * 86400000L + ((jlong) date_year(nvalue) - 1970) * 31557600000L;

#define CREATE_JDATE             (*env)->NewObject(env, getDateClassID(), getDateConstructorID(), value)

/* Convert number of microseconds since start of the day to the number of milliseconds since 1 jan 1970 */
#define GET_NEXT_JTIME           value = (jlong) nvalue / 1000L - 3600000L;

#define CREATE_JTIME             (*env)->NewObject(env, getTimeClassID(),  getTimeConstructorID(), value)

#define GET_NEXT_JTIMESTAMP      date aux1 = timestamp_date(nvalue); \
								 value = (jlong) ((date_dayofyear(aux1) - 1) * 86400000L + (date_year(aux1) - 1970) * 31557600000L + timestamp_daytime(nvalue) / 1000L - 3600000L);

#define CREATE_JTIMESTAMP        (*env)->NewObject(env, getTimestampClassID(), getTimestampConstructorID(), value)

#define CREATE_JGREGORIAN        (*env)->NewObject(env, getGregorianCalendarClassID(), getGregorianCalendarConstructorID())
#define GREGORIAN_EXTRA          (*env)->CallVoidMethod(env, result, getGregorianCalendarSetterID(), value);

#define FETCHING_LEVEL_TWO(NAME, BAT_CAST, GET_ATOM, ATOM, CONVERT_ATOM, EXTRA_STEP) \
	jobject get##NAME##Single(JNIEnv* env, jint position, BAT* b) { \
		const BAT_CAST *array = (BAT_CAST *) Tloc(b, 0); \
		BAT_CAST nvalue = array[position]; \
		jobject result; \
		jlong value; \
		if (nvalue != ATOM##_nil) { \
			GET_ATOM \
			if (!(result = CONVERT_ATOM)) { \
				(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
				return NULL; \
			} \
			EXTRA_STEP \
		} else { \
			result = NULL; \
		} \
		return result; \
	}

FETCHING_LEVEL_TWO(Date, jint, GET_NEXT_JDATE, date, CREATE_JDATE, DO_NOTHING)
FETCHING_LEVEL_TWO(Time, jlong, GET_NEXT_JTIME, daytime, CREATE_JTIME, DO_NOTHING)
FETCHING_LEVEL_TWO(Timestamp, timestamp, GET_NEXT_JTIMESTAMP, timestamp, CREATE_JTIMESTAMP, DO_NOTHING)

FETCHING_LEVEL_TWO(GregorianCalendarDate, jint, GET_NEXT_JDATE, date, CREATE_JGREGORIAN, GREGORIAN_EXTRA)
FETCHING_LEVEL_TWO(GregorianCalendarTime, jlong, GET_NEXT_JTIME, daytime, CREATE_JGREGORIAN, GREGORIAN_EXTRA)
FETCHING_LEVEL_TWO(GregorianCalendarTimestamp, timestamp, GET_NEXT_JTIMESTAMP, timestamp, CREATE_JGREGORIAN, GREGORIAN_EXTRA)

jobject getOidSingle(JNIEnv* env, jint position, BAT* b) {
	const oid *array = (oid *) Tloc(b, 0);
	oid nvalue = array[position];
	jobject result;
	if (!is_oid_nil(nvalue)) {
		char store[OID_STRING_BUFFER_SIZE];
		snprintf(store, OID_STRING_BUFFER_SIZE, OIDFMT "@0", nvalue);
		if (!(result = (*env)->NewStringUTF(env, store)))
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
	} else {
		result = NULL;
	}
	return result;
}

/* Decimals are harder! */

#define FETCHING_LEVEL_THREE(BAT_CAST, CONVERSION_CAST) \
	jobject getDecimal##BAT_CAST##Single(JNIEnv* env, jint position, BAT* b, jint scale) { \
		char *value; \
		const BAT_CAST *array = (const BAT_CAST *) Tloc(b, 0); \
		BAT_CAST nvalue = array[position]; \
		jobject result; \
		jstring aux; \
		sql_subtype t; \
		if (!sql_find_subtype(&t, "decimal", 0, scale)) {\
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "Unable to find decimal type"); \
			return NULL; \
		} \
		if (nvalue != BAT_CAST##_nil) { \
			if (!(value = decimal_to_str(nvalue, &t))) { \
				(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
				return NULL; \
			} \
			aux = (*env)->NewStringUTF(env, value); \
			_DELETE(value); \
			if (!aux) { \
				(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
				return NULL; \
			} \
			result = (*env)->NewObject(env, getBigDecimalClassID(), getBigDecimalConstructorID(), aux); \
			(*env)->DeleteLocalRef(env, aux); \
			if (!result) \
				(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
		} else { \
			result = NULL; \
		} \
		return result; \
	}

FETCHING_LEVEL_THREE(bte, lng)
FETCHING_LEVEL_THREE(sht, lng)
FETCHING_LEVEL_THREE(int, lng)
FETCHING_LEVEL_THREE(lng, lng)

/* Strings and BLOBs have to be retrieved from the BAT's heap */

#define GET_BAT_STRING      nvalue = BUNtail(li, p);
#define CHECK_NULL_STRING   strcmp(str_nil, nvalue) != 0
#define BAT_TO_STRING       if (!(value = (*env)->NewStringUTF(env, nvalue))) { \
								(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
								goto end; \
							}

#define GET_BAT_BLOB        nvalue = (blob*) BUNtail(li, p);
#define BAT_TO_JBLOB        if (nvalue->nitems > INT_MAX) { \
								(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "Internal BLOB is too large (larger than JVM max array size)"); \
								goto end; \
							} else { \
								if (!(value = (*env)->NewByteArray(env, (jsize) nvalue->nitems))) { \
									(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
									goto end; \
								} else { \
									(*env)->SetByteArrayRegion(env, value, 0, (jsize) nvalue->nitems, (jbyte*) nvalue->data); \
								} \
							}
#define CHECK_NULL_BLOB     nvalue->nitems != ~(size_t) 0

#define FETCHING_LEVEL_FOUR(NAME, RETURN_TYPE, GET_ATOM, CHECK_NOT_NULL, CONVERT_ATOM, ONE_CAST, TWO_CAST) \
	RETURN_TYPE get##NAME##Single(JNIEnv* env, jint p, BAT* b) { \
		BATiter li = bat_iterator(b); \
		ONE_CAST nvalue; \
		TWO_CAST value; \
		GET_ATOM \
		if (CHECK_NOT_NULL) { \
			CONVERT_ATOM \
			return value; \
		} \
end: \
		return NULL; \
	}

FETCHING_LEVEL_FOUR(String, jstring, GET_BAT_STRING, CHECK_NULL_STRING, BAT_TO_STRING, str, jstring)
FETCHING_LEVEL_FOUR(Blob, jbyteArray, GET_BAT_BLOB, CHECK_NULL_BLOB, BAT_TO_JBLOB, blob*, jbyteArray)

/* -- Converting BATs to Java Classes and primitives -- */

#define BATCH_LEVEL_ONE(NAME, JAVA_CAST, ARRAY_FUNCTION_NAME, INTERNAL_SIZE) \
	void get##NAME##Column(JNIEnv* env, JAVA_CAST##Array input, jint first, jint size, BAT* b) { \
		jboolean isCopy; \
		const JAVA_CAST* array = (const JAVA_CAST*) Tloc(b, 0); \
		JAVA_CAST* inputConverted = (JAVA_CAST*) (*env)->GetPrimitiveArrayCritical(env, input, &isCopy); \
		if (inputConverted == NULL) { \
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
		} else { \
			if(isCopy == JNI_FALSE) { \
				memcpy(inputConverted, array + first, size * INTERNAL_SIZE); \
			} else { \
				(*env)->Set##ARRAY_FUNCTION_NAME##ArrayRegion(env, input, first, size, array); \
			} \
			(*env)->ReleasePrimitiveArrayCritical(env, input, inputConverted, 0); \
		} \
	}

BATCH_LEVEL_ONE(Boolean, jboolean, Boolean, sizeof(bit))
BATCH_LEVEL_ONE(Tinyint, jbyte, Byte, sizeof(bte))
BATCH_LEVEL_ONE(Smallint, jshort, Short, sizeof(sht))
BATCH_LEVEL_ONE(Int, jint, Int, sizeof(jint))
BATCH_LEVEL_ONE(Bigint, jlong, Long, sizeof(lng))
BATCH_LEVEL_ONE(Real, jfloat, Float, sizeof(flt))
BATCH_LEVEL_ONE(Double, jdouble, Double, sizeof(dbl))

//If the user wants objects for everything, we can't deny him :)

#define CREATE_NEW_BOOLEAN     (*env)->NewObject(env, getBooleanClassID(),  getBooleanConstructorID(), nvalue)
#define CREATE_NEW_BYTE        (*env)->NewObject(env, getByteClassID(),  getByteConstructorID(), nvalue)
#define CREATE_NEW_SHORT       (*env)->NewObject(env, getShortClassID(),  getShortConstructorID(), nvalue)
#define CREATE_NEW_INTEGER     (*env)->NewObject(env, getIntegerClassID(),  getIntegerConstructorID(), nvalue)
#define CREATE_NEW_LONG        (*env)->NewObject(env, getLongClassID(),  getLongConstructorID(), nvalue)
#define CREATE_NEW_FLOAT       (*env)->NewObject(env, getFloatClassID(),  getFloatConstructorID(), nvalue)
#define CREATE_NEW_DOUBLE      (*env)->NewObject(env, getDoubleClassID(),  getDoubleConstructorID(), nvalue)

#define BATCH_LEVEL_ONE_OBJECT(NAME, BAT_CAST, NULL_ATOM, CONVERT_ATOM) \
	void get##NAME##ColumnAsObject(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b) { \
		BAT_CAST *array = (BAT_CAST *) Tloc(b, 0), nvalue; \
		jobject next; \
		array += first; \
		if (b->tnonil && !b->tnil) { \
			for (jint i = 0; i < size; i++) { \
				nvalue = array[i]; \
				if (!(next = CONVERT_ATOM)) { \
					(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
					return; \
				} \
				(*env)->SetObjectArrayElement(env, input, i, next); \
				(*env)->DeleteLocalRef(env, next); \
			} \
		} else { \
			for (jint i = 0; i < size; i++) { \
				nvalue = array[i]; \
				if (nvalue != NULL_ATOM##_nil) { \
					if (!(next = CONVERT_ATOM)) { \
						(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
						return; \
					} \
					(*env)->SetObjectArrayElement(env, input, i, next); \
					(*env)->DeleteLocalRef(env, next); \
				} else { \
					(*env)->SetObjectArrayElement(env, input, i, NULL); \
				} \
			} \
		} \
	}

BATCH_LEVEL_ONE_OBJECT(Boolean, jboolean, bit, CREATE_NEW_BOOLEAN)
BATCH_LEVEL_ONE_OBJECT(Tinyint, jbyte, bte, CREATE_NEW_BYTE)
BATCH_LEVEL_ONE_OBJECT(Smallint, jshort, sht, CREATE_NEW_SHORT)
BATCH_LEVEL_ONE_OBJECT(Int, jint, int, CREATE_NEW_INTEGER)
BATCH_LEVEL_ONE_OBJECT(Bigint, jlong, lng, CREATE_NEW_LONG)
BATCH_LEVEL_ONE_OBJECT(Real, jfloat, flt, CREATE_NEW_FLOAT)
BATCH_LEVEL_ONE_OBJECT(Double, jdouble, dbl, CREATE_NEW_DOUBLE)

#define BATCH_LEVEL_TWO(NAME, BAT_CAST, GET_ATOM, CONVERT_ATOM) \
	void get##NAME##Column(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b) { \
		BAT_CAST *array = (BAT_CAST *) Tloc(b, 0), nvalue; \
		jobject next; \
		jlong value; \
		array += first; \
		if (b->tnonil && !b->tnil) { \
			for (jint i = 0; i < size; i++) { \
				nvalue = array[i]; \
				GET_ATOM \
				if (!(next = CONVERT_ATOM)) { \
					(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
					return; \
				} \
				(*env)->SetObjectArrayElement(env, input, i, next); \
				(*env)->DeleteLocalRef(env, next); \
			} \
		} else { \
			for (jint i = 0; i < size; i++) { \
				nvalue = array[i]; \
				if(nvalue != BAT_CAST##_nil) { \
					GET_ATOM \
					if (!(next = CONVERT_ATOM)) { \
						(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
						return; \
					} \
					(*env)->SetObjectArrayElement(env, input, i, next); \
					(*env)->DeleteLocalRef(env, next); \
				} else { \
					(*env)->SetObjectArrayElement(env, input, i, NULL); \
				} \
			} \
		} \
	}

BATCH_LEVEL_TWO(Date, date, GET_NEXT_JDATE, CREATE_JDATE)
BATCH_LEVEL_TWO(Time, daytime, GET_NEXT_JTIME, CREATE_JTIME)
BATCH_LEVEL_TWO(Timestamp, timestamp, GET_NEXT_JTIMESTAMP, CREATE_JTIMESTAMP)

void getOidColumn(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b) {
	oid *array = (oid *) Tloc(b, 0);
	oid nvalue;
	jobject next;
	array += first;
	if (b->tnonil && !b->tnil) {
		for (jint i = 0; i < size; i++) {
			char store[OID_STRING_BUFFER_SIZE];
			snprintf(store, OID_STRING_BUFFER_SIZE, OIDFMT "@0", array[i]);
			if (!(next = (*env)->NewStringUTF(env, store))) {
				(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
				return;
			}
			(*env)->SetObjectArrayElement(env, input, i, next);
			(*env)->DeleteLocalRef(env, next);
		}
	} else {
		for (jint i = 0; i < size; i++) {
			nvalue = array[i];
			if (!is_oid_nil(nvalue)) {
				char store[OID_STRING_BUFFER_SIZE];
				snprintf(store, OID_STRING_BUFFER_SIZE, OIDFMT "@0", array[i]);
				if (!(next = (*env)->NewStringUTF(env, store))) {
					(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
					return;
				}
				(*env)->SetObjectArrayElement(env, input, i, next);
				(*env)->DeleteLocalRef(env, next);
			} else {
				(*env)->SetObjectArrayElement(env, input, i, NULL);
			}
		}
	}
}

#define BATCH_LEVEL_THREE(BAT_CAST, CONVERSION_CAST) \
	void getDecimal##BAT_CAST##Column(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b, jint scale) { \
		char *value; \
		const BAT_CAST *array = (const BAT_CAST *) Tloc(b, 0); \
		jclass lbigDecimalClassID = getBigDecimalClassID(); \
		jmethodID lbigDecimalConstructorID = getBigDecimalConstructorID(); \
		jstring aux; \
		jobject next; \
		sql_subtype t; \
		if (!sql_find_subtype(&t, "decimal", 0, scale)) {\
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "Unable to find decimal type"); \
			return; \
		} \
		array += first; \
		if (b->tnonil && !b->tnil) { \
			for (jint i = 0; i < size; i++) { \
				if (!(value = decimal_to_str((CONVERSION_CAST) array[i], &t))) { \
					(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
					return; \
				} \
				aux = (*env)->NewStringUTF(env, value); \
				_DELETE(value); \
				if (!aux) { \
					(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
					return; \
				} \
				next = (*env)->NewObject(env, lbigDecimalClassID, lbigDecimalConstructorID, aux); \
				(*env)->DeleteLocalRef(env, aux); \
				if (!next) { \
					(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
					return; \
				} \
				(*env)->SetObjectArrayElement(env, input, i, next); \
				(*env)->DeleteLocalRef(env, next); \
			} \
		} else { \
			for (jint i = 0; i < size; i++) { \
				BAT_CAST nvalue = array[i]; \
				if (nvalue != BAT_CAST##_nil) { \
					if (!(value = decimal_to_str((CONVERSION_CAST) array[i], &t))) { \
						(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
						return; \
					} \
					aux = (*env)->NewStringUTF(env, value); \
					_DELETE(value); \
					if (!aux) { \
						(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
						return; \
					} \
					next = (*env)->NewObject(env, lbigDecimalClassID, lbigDecimalConstructorID, aux); \
					(*env)->DeleteLocalRef(env, aux); \
					if (!next) { \
						(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
						return; \
					} \
					(*env)->SetObjectArrayElement(env, input, i, next); \
					(*env)->DeleteLocalRef(env, next); \
				} else { \
					(*env)->SetObjectArrayElement(env, input, i, NULL); \
				} \
			} \
		} \
	}

BATCH_LEVEL_THREE(bte, lng)
BATCH_LEVEL_THREE(sht, lng)
BATCH_LEVEL_THREE(int, lng)
BATCH_LEVEL_THREE(lng, lng)

#define BATCH_LEVEL_FOUR(NAME, GET_ATOM, CHECK_NOT_NULL, CONVERT_ATOM, ONE_CAST, TWO_CAST) \
	void get##NAME##Column(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b) { \
		jint i = 0; \
		BUN p, q; \
		BATiter li = bat_iterator(b); \
		ONE_CAST nvalue; \
		TWO_CAST value; \
		if (b->tnonil && !b->tnil) { \
			for (p = first, q = (BUN) size; p < q; p++) { \
				GET_ATOM \
				CONVERT_ATOM \
				(*env)->SetObjectArrayElement(env, input, i, value); \
				i++; \
				(*env)->DeleteLocalRef(env, value); \
			} \
		} else { \
			for (p = (BUN) first, q = (BUN) size; p < q; p++) { \
				GET_ATOM \
				if (CHECK_NOT_NULL) { \
					CONVERT_ATOM \
					(*env)->SetObjectArrayElement(env, input, i, value); \
					(*env)->DeleteLocalRef(env, value); \
				} else { \
					(*env)->SetObjectArrayElement(env, input, i, NULL); \
				} \
				i++; \
			} \
		} \
end: \
		return; \
	}

BATCH_LEVEL_FOUR(String, GET_BAT_STRING, CHECK_NULL_STRING, BAT_TO_STRING, str, jstring)
BATCH_LEVEL_FOUR(Blob, GET_BAT_BLOB, CHECK_NULL_BLOB, BAT_TO_JBLOB, blob*, jbyteArray)

/* --  Converting Java Classes and primitives to BATs -- */

/* Direct mapping for primitives :) */

#define CONVERSION_LEVEL_ONE(NAME, BAT_CAST, JAVA_CAST, COPY_METHOD) \
	void store##NAME##Column(JNIEnv *env, BAT** b, JAVA_CAST##Array data, size_t cnt, jint localtype) { \
		BAT *aux = COLnew(0, localtype, cnt, TRANSIENT); \
		BAT_CAST *p, value, prev = BAT_CAST##_nil; \
		if (!aux) { \
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
			*b = NULL; \
			return; \
		} \
		aux->tnil = 0; \
		aux->tnonil = 1; \
		aux->tkey = 0; \
		aux->tsorted = 1; \
		aux->trevsorted = 1; \
		p = (BAT_CAST *) Tloc(aux, 0); \
		(*env)->Get##COPY_METHOD##ArrayRegion(env, data, 0, (jsize) cnt, (JAVA_CAST *) p); \
		for(size_t i = 0; i < cnt; i++) { \
			if ((value = p[i]) == BAT_CAST##_nil) { \
				aux->tnil = 1; \
				aux->tnonil = 0; \
			} \
			if (i > 0) { \
				if (value > prev && aux->trevsorted) { \
					aux->trevsorted = 0; \
				} else if (value < prev && aux->tsorted) { \
					aux->tsorted = 0; \
				} \
			} \
			prev = value; \
		} \
		BATsetcount(aux, cnt); \
		BATsettrivprop(aux); \
		BBPkeepref(aux->batCacheid); \
		*b = aux; \
	}

CONVERSION_LEVEL_ONE(Boolean, bit, jbyte, Byte)
CONVERSION_LEVEL_ONE(Tinyint, bte, jbyte, Byte)
CONVERSION_LEVEL_ONE(Smallint, sht, jshort, Short)
CONVERSION_LEVEL_ONE(Int, int, jint, Int)
CONVERSION_LEVEL_ONE(Bigint, lng, jlong, Long)
CONVERSION_LEVEL_ONE(Real, flt, jfloat, Float)
CONVERSION_LEVEL_ONE(Double, dbl, jdouble, Double)

/* These types have constant sizes, so the conversion is still easy :P */

#ifdef WIN32
#define DIVMOD_HELPER_FIR lldiv_t aux1;
#define DIVMOD_HELPER_SEC lldiv(nvalue, 86400000L);
#else
#define DIVMOD_HELPER_FIR ldiv_t aux1;
#define DIVMOD_HELPER_SEC ldiv(nvalue, 86400000L);
#endif

#define JDATE_TO_BAT             nvalue = (*env)->CallLongMethod(env, value, getDateToLongID()); \
								 *p = date_create((int)(nvalue / 31557600000L), (int)(nvalue / 2629746000L), (int)(nvalue / 86400000L)); \
								 (void) aux1;

#define JTIME_TO_BAT             nvalue = (*env)->CallLongMethod(env, value, getTimeToLongID()); \
								 *p = (daytime) ((nvalue + 3600000L) * 1000L); \
								 (void) aux1;

#define JTIMESTAMP_TO_BAT        nvalue = (*env)->CallLongMethod(env, value, getTimestampToLongID()); \
								 aux1 = DIVMOD_HELPER_SEC \
								 *p = timestamp_create(date_create((int)(aux1.quot / 31557600000L), (int)(aux1.quot / 2629746000L), (int)(aux1.quot / 86400000L)), (daytime) ((aux1.rem + 3600000L) * 1000L));

#define CONVERSION_LEVEL_TWO(NAME, BAT_CAST, CONVERT_TO_BAT) \
	void store##NAME##Column(JNIEnv *env, BAT** b, jobjectArray data, size_t cnt, jint localtype) { \
		BAT *aux = COLnew(0, localtype, cnt, TRANSIENT); \
		jlong nvalue; \
		BAT_CAST *p, prev = BAT_CAST##_nil; \
		jobject value; \
		DIVMOD_HELPER_FIR \
		if (!aux) { \
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
			*b = NULL; \
			return; \
		} \
		aux->tnil = 0; \
		aux->tnonil = 1; \
		aux->tkey = 0; \
		aux->tsorted = 1; \
		aux->trevsorted = 1; \
		p = (BAT_CAST *) Tloc(aux, 0); \
		for(size_t i = 0; i < cnt; i++, p++) { \
			if ((value = (*env)->GetObjectArrayElement(env, data, (jsize) i)) == NULL) { \
				aux->tnil = 1; \
				aux->tnonil = 0; \
				*p = BAT_CAST##_nil; \
			} else { \
				CONVERT_TO_BAT \
				(*env)->DeleteLocalRef(env, value); \
			} \
			if (i > 0) { \
				if (*p > prev && aux->trevsorted) { \
					aux->trevsorted = 0; \
				} else if (*p < prev && aux->tsorted) { \
					aux->tsorted = 0; \
				} \
			} \
			prev = *p; \
		} \
		BATsetcount(aux, cnt); \
		BATsettrivprop(aux); \
		BBPkeepref(aux->batCacheid); \
		*b = aux; \
	}

CONVERSION_LEVEL_TWO(Date, date, JDATE_TO_BAT)
CONVERSION_LEVEL_TWO(Time, daytime, JTIME_TO_BAT)
CONVERSION_LEVEL_TWO(Timestamp, timestamp, JTIMESTAMP_TO_BAT)

void storeOidColumn(JNIEnv *env, BAT** b, jobjectArray data, size_t cnt, jint localtype) {
	BAT *aux = COLnew(0, localtype, cnt, TRANSIENT);
	size_t slen = sizeof(oid);
	oid *p;
	if (!aux) {
		(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
		*b = NULL;
		return;
	}
	aux->tnil = 0;
	aux->tnonil = 1;
	aux->tkey = 0;
	aux->tsorted = 1;
	aux->trevsorted = 1;
	p = (oid *) Tloc(aux, 0);
	for(size_t i = 0; i < cnt; i++) {
		oid prev = oid_nil;
		jstring jvalue = (*env)->GetObjectArrayElement(env, data, (jsize) i);
		if (jvalue == NULL) {
			aux->tnil = 1;
			aux->tnonil = 0;
			*p = oid_nil;
		} else {
			ssize_t parsed;
			const char *nvalue = (*env)->GetStringUTFChars(env, jvalue, 0);
			if (nvalue == NULL) {
				(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL);
				(*env)->DeleteLocalRef(env, jvalue);
				BBPreclaim(aux);
				*b = NULL;
				return;
			}
			parsed = OIDfromStr(nvalue, &slen, &p, true);
			(*env)->ReleaseStringUTFChars(env, jvalue, nvalue);
			(*env)->DeleteLocalRef(env, jvalue);
			if (parsed < 1) {
				(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), "Wrong OID format");
				BBPreclaim(aux);
				*b = NULL;
				return;
			}
		}
		if (i > 0) {
			if (*p > prev && aux->trevsorted) {
				aux->trevsorted = 0;
			} else if (*p < prev && aux->tsorted) {
				aux->tsorted = 0;
			}
		}
		prev = *p;
		p++;
	}
	BATsetcount(aux, cnt);
	BATsettrivprop(aux);
	BBPkeepref(aux->batCacheid);
	*b = aux;
}

/* Decimals are harder */

#define CONVERSION_LEVEL_THREE(BAT_CAST) \
	void storeDecimal##BAT_CAST##Column(JNIEnv *env, BAT** b, jobjectArray data, size_t cnt, jint localtype, jint scale, jint roundingMode) { \
		BAT *aux = COLnew(0, localtype, cnt, TRANSIENT); \
		BAT_CAST *p, prev = BAT_CAST##_nil; \
		jmethodID lbigDecimalToStringID = getBigDecimalToStringID(), lsetBigDecimalScaleID = getSetBigDecimalScaleID(); \
		jobject value, bigDecimal; \
		jstring nvalue; \
		const char *representation; \
		if (!aux) { \
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
			*b = NULL; \
			return; \
		} \
		aux->tnil = 0; \
		aux->tnonil = 1; \
		aux->tkey = 0; \
		aux->tsorted = 1; \
		aux->trevsorted = 1; \
		p = (BAT_CAST *) Tloc(aux, 0); \
		for(size_t i = 0; i < cnt; i++, p++) { \
			if ((value = (*env)->GetObjectArrayElement(env, data, (jsize) i)) == NULL) { \
				aux->tnil = 1; \
				aux->tnonil = 0; \
				*p = BAT_CAST##_nil; \
			} else { \
				bigDecimal = (*env)->CallObjectMethod(env, value, lsetBigDecimalScaleID, scale, roundingMode); \
				nvalue = (*env)->CallObjectMethod(env, bigDecimal, lbigDecimalToStringID); \
				if ((representation = (*env)->GetStringUTFChars(env, nvalue, NULL)) == NULL) { \
					(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
					(*env)->DeleteLocalRef(env, nvalue); \
					(*env)->DeleteLocalRef(env, bigDecimal); \
					BBPreclaim(aux); \
					*b = NULL; \
					return; \
				} else { \
					*p = (BAT_CAST) decimal_from_str((char*) representation, NULL); \
					(*env)->ReleaseStringUTFChars(env, nvalue, representation); \
					(*env)->DeleteLocalRef(env, nvalue); \
					(*env)->DeleteLocalRef(env, bigDecimal); \
					(*env)->DeleteLocalRef(env, value); \
				} \
			} \
			if (i > 0) { \
				if (*p - prev > 0 && aux->trevsorted) { \
					aux->trevsorted = 0; \
				} else if (*p - prev < 0 && aux->tsorted) { \
					aux->tsorted = 0; \
				} \
			} \
			prev = *p; \
		} \
		BATsetcount(aux, cnt); \
		BATsettrivprop(aux); \
		BBPkeepref(aux->batCacheid); \
		*b = aux; \
	}

CONVERSION_LEVEL_THREE(bte)
CONVERSION_LEVEL_THREE(sht)
CONVERSION_LEVEL_THREE(int)
CONVERSION_LEVEL_THREE(lng)

/* Put in the BAT's heap */

#define JSTRING_TO_BAT      if ((nvalue = (*env)->GetStringUTFChars(env, value, 0)) == NULL) { \
								(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
								(*env)->DeleteLocalRef(env, value); \
								BBPreclaim(aux); \
								*b = NULL; \
								return; \
							} else { \
								p = GDKstrdup(nvalue); \
								(*env)->ReleaseStringUTFChars(env, value, nvalue); \
								if (p == NULL) { \
									(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
									(*env)->DeleteLocalRef(env, value); \
									BBPreclaim(aux); \
									*b = NULL; \
									return; \
								} \
							}

#define STRING_START        const char* nvalue;

#define PUT_STR_IN_HEAP     if (BUNappend(aux, p, FALSE) != GDK_SUCCEED) { \
								(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
								BBPreclaim(aux); \
								GDKfree(p); \
								*b = NULL; \
								return; \
							}

#define STR_CMP             strcmp(p, prev)

#define JBLOB_TO_BAT        nvalue = (jbyteArray) value; \
							len = (*env)->GetArrayLength(env, nvalue); \
							if ((p = GDKmalloc(blobsize(len))) == NULL) { \
								(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
								(*env)->DeleteLocalRef(env, value); \
								BBPreclaim(aux); \
								*b = NULL; \
								return; \
							} else { \
								p->nitems = len; \
								(*env)->GetByteArrayRegion(env, nvalue, 0, len, (jbyte*) p->data); \
							}

#define BLOB_START          var_t bun_offset = 0; \
							jsize len; \
							jbyteArray nvalue;

extern const blob* BLOBnull(void);
extern var_t BLOBput(Heap *h, var_t *bun, const blob *val);

#define PUT_BLOB_IN_HEAP    if (BLOBput(aux->tvheap, &bun_offset, p) == 0) { \
								(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
								GDKfree(p); \
								BBPreclaim(aux); \
								*b = NULL; \
								return; \
							} else if (BUNappend(aux, p, FALSE) != GDK_SUCCEED) { \
								(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
								GDKfree(p); \
								BBPreclaim(aux); \
								*b = NULL; \
								return; \
							}

#define BLOB_CMP            memcmp(p->data, prev->data, min(p->nitems, prev->nitems))

#define CONVERSION_LEVEL_FOUR(NAME, BAT_CAST, NULL_CONST, START_STEP, CONVERT_TO_BAT, ORDER_CMP, PUT_IN_HEAP) \
	void store##NAME##Column(JNIEnv *env, BAT** b, jobjectArray data, size_t cnt, jint localtype) { \
		BAT *aux = COLnew(0, localtype, cnt, TRANSIENT); \
		bool previousToFree = false; \
		BAT_CAST p; \
		BAT_CAST prev = NULL_CONST; \
		jobject value; \
		START_STEP \
		if (!aux) { \
			(*env)->ThrowNew(env, getMonetDBEmbeddedExceptionClassID(), MAL_MALLOC_FAIL); \
			*b = NULL; \
			return; \
		} \
		aux->tnil = 0; \
		aux->tnonil = 1; \
		aux->tkey = 0; \
		aux->tsorted = 1; \
		aux->trevsorted = 1; \
		for(size_t i = 0; i < cnt; i++) { \
			if ((value = (*env)->GetObjectArrayElement(env, data, (jsize) i)) == NULL) { \
				aux->tnil = 1; \
				aux->tnonil = 0; \
				p = NULL_CONST; \
			} else { \
				CONVERT_TO_BAT \
				(*env)->DeleteLocalRef(env, value); \
			} \
			PUT_IN_HEAP \
			if (i > 0) { \
				if ((ORDER_CMP > 0) && aux->trevsorted) { \
					aux->trevsorted = 0; \
				} else if ((ORDER_CMP < 0) && aux->tsorted) { \
					aux->tsorted = 0; \
				} \
				if (previousToFree) \
					GDKfree(prev); \
			} \
			prev = p; \
			previousToFree = (value != NULL); \
		} \
		if (previousToFree) \
			GDKfree(p); \
		BATsetcount(aux, cnt); \
		BATsettrivprop(aux); \
		BBPkeepref(aux->batCacheid); \
		*b = aux; \
	}

CONVERSION_LEVEL_FOUR(String, str, (str) str_nil, STRING_START, JSTRING_TO_BAT, STR_CMP, PUT_STR_IN_HEAP)
CONVERSION_LEVEL_FOUR(Blob, blob*, (blob*) BLOBnull(), BLOB_START, JBLOB_TO_BAT, BLOB_CMP, PUT_BLOB_IN_HEAP)
