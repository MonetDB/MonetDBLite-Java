/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2019 MonetDB B.V.
 */

#ifndef SRC_CONVERTERS_H
#define SRC_CONVERTERS_H

#include "monetdb_config.h"
#include "jni.h"
#include "gdk.h"

/* --  Get just a single value -- */

java_export jbyte getTinyintSingle(jint position, BAT* b);
java_export jshort getSmallintSingle(jint position, BAT* b);
java_export jint getIntSingle(jint position, BAT* b);
java_export jlong getBigintSingle(jint position, BAT* b);
java_export jfloat getRealSingle(jint position, BAT* b);
java_export jdouble getDoubleSingle(jint position, BAT* b);

java_export jobject getDateSingle(JNIEnv* env, jint position, BAT* b);
java_export jobject getTimeSingle(JNIEnv* env, jint position, BAT* b);
java_export jobject getTimestampSingle(JNIEnv* env, jint position, BAT* b);

java_export jobject getGregorianCalendarDateSingle(JNIEnv* env, jint position, BAT* b);
java_export jobject getGregorianCalendarTimeSingle(JNIEnv* env, jint position, BAT* b);
java_export jobject getGregorianCalendarTimestampSingle(JNIEnv* env, jint position, BAT* b);
java_export jobject getOidSingle(JNIEnv* env, jint position, BAT* b);

java_export jobject getDecimalbteSingle(JNIEnv* env, jint position, BAT* b, jint scale);
java_export jobject getDecimalshtSingle(JNIEnv* env, jint position, BAT* b, jint scale);
java_export jobject getDecimalintSingle(JNIEnv* env, jint position, BAT* b, jint scale);
java_export jobject getDecimallngSingle(JNIEnv* env, jint position, BAT* b, jint scale);

java_export jstring getStringSingle(JNIEnv* env, jint position, BAT* b);
java_export jbyteArray getBlobSingle(JNIEnv* env, jint position, BAT* b);

/* --  Converting BATs to Java Classes and primitives arrays -- */

java_export void getBooleanColumn(JNIEnv* env, jbooleanArray input, jint first, jint size, BAT* b);
java_export void getTinyintColumn(JNIEnv* env, jbyteArray input, jint first, jint size, BAT* b);
java_export void getSmallintColumn(JNIEnv* env, jshortArray input, jint first, jint size, BAT* b);
java_export void getIntColumn(JNIEnv* env, jintArray input, jint first, jint size, BAT* b);
java_export void getBigintColumn(JNIEnv* env, jlongArray input, jint first, jint size, BAT* b);
java_export void getRealColumn(JNIEnv* env, jfloatArray input, jint first, jint size, BAT* b);
java_export void getDoubleColumn(JNIEnv* env, jdoubleArray input, jint first, jint size, BAT* b);

java_export void getBooleanColumnAsObject(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b);
java_export void getTinyintColumnAsObject(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b);
java_export void getSmallintColumnAsObject(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b);
java_export void getIntColumnAsObject(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b);
java_export void getBigintColumnAsObject(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b);
java_export void getRealColumnAsObject(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b);
java_export void getDoubleColumnAsObject(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b);

java_export void getDateColumn(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b);
java_export void getTimeColumn(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b);
java_export void getTimestampColumn(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b);
java_export void getOidColumn(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b);

java_export void getDecimalbteColumn(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b, jint scale);
java_export void getDecimalshtColumn(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b, jint scale);
java_export void getDecimalintColumn(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b, jint scale);
java_export void getDecimallngColumn(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b, jint scale);

java_export void getStringColumn(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b);
java_export void getBlobColumn(JNIEnv* env, jobjectArray input, jint first, jint size, BAT* b);

/* -- Converting Java Classes and primitives to BATs -- */

java_export void storeBooleanColumn(JNIEnv* env, BAT** b, jbooleanArray input, size_t cnt, jint localtype);
java_export void storeTinyintColumn(JNIEnv* env, BAT** b, jbyteArray input, size_t cnt, jint localtype);
java_export void storeSmallintColumn(JNIEnv* env, BAT** b, jshortArray input, size_t cnt, jint localtype);
java_export void storeIntColumn(JNIEnv* env, BAT** b, jintArray input, size_t cnt, jint localtype);
java_export void storeBigintColumn(JNIEnv* env, BAT** b, jlongArray input, size_t cnt, jint localtype);
java_export void storeRealColumn(JNIEnv* env, BAT** b, jfloatArray input, size_t cnt, jint localtype);
java_export void storeDoubleColumn(JNIEnv* env, BAT** b, jdoubleArray input, size_t cnt, jint localtype);

java_export void storeDateColumn(JNIEnv* env, BAT** b, jobjectArray input, size_t cnt, jint localtype);
java_export void storeTimeColumn(JNIEnv* env, BAT** b, jobjectArray input, size_t cnt, jint localtype);
java_export void storeTimestampColumn(JNIEnv* env, BAT** b, jobjectArray input, size_t cnt, jint localtype);
java_export void storeOidColumn(JNIEnv* env, BAT** b, jobjectArray input, size_t cnt, jint localtype);

java_export void storeDecimalbteColumn(JNIEnv* env, BAT** b, jobjectArray input, size_t cnt, jint localtype, jint scale, jint roundingMode);
java_export void storeDecimalshtColumn(JNIEnv* env, BAT** b, jobjectArray input, size_t cnt, jint localtype, jint scale, jint roundingMode);
java_export void storeDecimalintColumn(JNIEnv* env, BAT** b, jobjectArray input, size_t cnt, jint localtype, jint scale, jint roundingMode);
java_export void storeDecimallngColumn(JNIEnv* env, BAT** b, jobjectArray input, size_t cnt, jint localtype, jint scale, jint roundingMode);

java_export void storeStringColumn(JNIEnv* env, BAT** b, jobjectArray input, size_t cnt, jint localtype);
java_export void storeBlobColumn(JNIEnv* env, BAT** b, jobjectArray input, size_t cnt, jint localtype);

#endif //SRC_CONVERTERS_H
