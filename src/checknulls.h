/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2019 MonetDB B.V.
 */

#ifndef MONETDBLITE_CHECKNULLS_H
#define MONETDBLITE_CHECKNULLS_H

#include "monetdb_config.h"
#include "jni.h"
#include "gdk.h"

java_export void checkBooleanNulls(JNIEnv* env, jbooleanArray input, jint size, BAT* b);
java_export void checkTinyintNulls(JNIEnv* env, jbooleanArray input, jint size, BAT* b);
java_export void checkSmallintNulls(JNIEnv* env, jbooleanArray input, jint size, BAT* b);
java_export void checkIntNulls(JNIEnv* env, jbooleanArray input, jint size, BAT* b);
java_export void checkBigintNulls(JNIEnv* env, jbooleanArray input, jint size, BAT* b);
java_export void checkRealNulls(JNIEnv* env, jbooleanArray input, jint size, BAT* b);
java_export void checkDoubleNulls(JNIEnv* env, jbooleanArray input, jint size, BAT* b);

java_export void checkDateNulls(JNIEnv* env, jbooleanArray input, jint size, BAT* b);
java_export void checkTimeNulls(JNIEnv* env, jbooleanArray input, jint size, BAT* b);
java_export void checkTimestampNulls(JNIEnv* env, jbooleanArray input, jint size, BAT* b);
java_export void checkOidNulls(JNIEnv* env, jbooleanArray input, jint size, BAT* b);

java_export void checkStringNulls(JNIEnv* env, jbooleanArray input, jint size, BAT* b);
java_export void checkBlobNulls(JNIEnv* env, jbooleanArray input, jint size, BAT* b);

#endif //MONETDBLITE_CHECKNULLS_H
