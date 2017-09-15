/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2017 MonetDB B.V.
 */

#include "nl_cwi_monetdb_embedded_env_MonetDBEmbeddedDatabase.h"

#include "monetdb_config.h"
#include "embedded.h"
#include "embeddedjvm.h"
#include "javaids.h"
#include "converters.h"
#include "gdk_posix.h"

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedDatabase_startDatabaseInternal
	(JNIEnv *env, jclass monetDBEmbeddedDatabase, jstring dbDirectory, jboolean silentFlag, jboolean sequentialFlag) {
	const char* dbdir_string_tmp = NULL, *loadPath_tmp = NULL;
	char *err = NULL;
	jclass exceptionCls, loaderCls = NULL;
	jfieldID pathID;
	jstring loadPath = NULL;
	jobject result = NULL;

	if(dbDirectory) {
		dbdir_string_tmp = (*env)->GetStringUTFChars(env, dbDirectory, NULL);
		if(!dbdir_string_tmp) {
			exceptionCls = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedException");
			(*env)->ThrowNew(env, exceptionCls, "System out of memory!");
			goto endofinit;
		}
		if(!strncmp(dbdir_string_tmp, ":memory:", 8)) { //activate in-memory mode
			(*env)->ReleaseStringUTFChars(env, dbDirectory, dbdir_string_tmp);
			dbdir_string_tmp = NULL;
		}
	}
	if(!monetdb_is_initialized()) {
		//initialize the java ID fields for faster Java data loading later on
		if(!initializeIDS(env)) {
			exceptionCls = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedException");
			(*env)->ThrowNew(env, exceptionCls, "System out of memory!");
			goto endofinit;
		}
		//because of the dlopen stuff, this step has to be done before the monetdb_startup call
		loaderCls = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBJavaLiteLoader");
		pathID = (*env)->GetStaticFieldID(env, loaderCls, "loadedLibraryFullPath", "Ljava/lang/String;");
		loadPath = (jstring) (*env)->GetStaticObjectField(env, loaderCls, pathID);
		if(!loadPath) {
			exceptionCls = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedException");
			(*env)->ThrowNew(env, exceptionCls, "System out of memory!");
			goto endofinit;
		}
		loadPath_tmp = (*env)->GetStringUTFChars(env, loadPath, NULL);
		if(!loadPath_tmp) {
			exceptionCls = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedException");
			(*env)->ThrowNew(env, exceptionCls, "System out of memory!");
			goto endofinit;
		}
		if(!setMonetDB5LibraryPath(loadPath_tmp)) {
			exceptionCls = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedException");
			(*env)->ThrowNew(env, exceptionCls, "System out of memory!");
			goto endofinit;
		}
		err = monetdb_startup((char*) dbdir_string_tmp, (char) silentFlag, (char) sequentialFlag);
		if (err) {
			exceptionCls = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedException");
			(*env)->ThrowNew(env, exceptionCls, err);
			GDKfree(err);
			goto endofinit;
		}
		result = (*env)->NewObject(env, monetDBEmbeddedDatabase, getMonetDBEmbeddedDatabaseConstructorID(), dbDirectory,
								   silentFlag, sequentialFlag);
		if(!result) {
			exceptionCls = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedException");
			(*env)->ThrowNew(env, exceptionCls, "System out of memory!");
		}
	} else {
		exceptionCls = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedException");
		(*env)->ThrowNew(env, exceptionCls, "Only one MonetDB Embedded database is allowed per process!");
	}
endofinit:
	if(dbdir_string_tmp) {
		(*env)->ReleaseStringUTFChars(env, dbDirectory, dbdir_string_tmp);
	}
	if(loadPath_tmp) {
		(*env)->ReleaseStringUTFChars(env, loadPath, loadPath_tmp);
	}
	if(loaderCls) {
		(*env)->DeleteLocalRef(env, loaderCls);
	}
	if(loadPath) {
		(*env)->DeleteLocalRef(env, loadPath);
	}
	return result;
}

JNIEXPORT void JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedDatabase_stopDatabaseInternal
	(JNIEnv *env, jobject database) {
	jclass exceptionCls;
	(void) env;
	(void) database;

	if(monetdb_is_initialized()) {
		//release the java ID fields
		releaseIDS(env);
		//release the dlopen string
		freeMonetDB5LibraryPath();
		monetdb_shutdown();
	} else {
		exceptionCls = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedException");
		(*env)->ThrowNew(env, exceptionCls, "The MonetDB Embedded database is not running!");
	}
}

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedDatabase_createConnectionInternal
	(JNIEnv *env, jobject database) {
	jclass exceptionCls;
	monetdb_connection conn = NULL;
	jobject result = NULL;
	(void) database;

	if(monetdb_is_initialized()) {
		conn = monetdb_connect();
		if(!conn) {
			exceptionCls = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedException");
			(*env)->ThrowNew(env, exceptionCls, "The connection initialization failed!");
		} else {
			result = (*env)->NewObject(env, getMonetDBEmbeddedConnectionClassID(),
									   getMonetDBEmbeddedConnectionConstructorID(), (jlong) conn);
			if(!result) {
				exceptionCls = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedException");
				(*env)->ThrowNew(env, exceptionCls, "System out of memory!");
			}
		}
	} else {
		exceptionCls = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedException");
		(*env)->ThrowNew(env, exceptionCls, "The MonetDB Embedded database is not running!");
	}
	return result;
}

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_embedded_env_MonetDBEmbeddedDatabase_createJDBCEmbeddedConnectionInternal
	(JNIEnv *env, jobject database) {
	jclass exceptionCls;
	monetdb_connection conn = NULL;
	jobject result = NULL;
	(void) database;

	if(monetdb_is_initialized()) {
		conn = monetdb_connect();
		if(!conn) {
			exceptionCls = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedException");
			(*env)->ThrowNew(env, exceptionCls, "The connection initialization failed!");
		} else {
			result = (*env)->NewObject(env, getJDBCEmbeddedConnectionClassID(),
									   getJDBCDBEmbeddedConnectionConstructorID(), (jlong) conn);
			if(!result) {
				exceptionCls = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedException");
				(*env)->ThrowNew(env, exceptionCls, "System out of memory!");
			}
		}
	} else {
		exceptionCls = (*env)->FindClass(env, "nl/cwi/monetdb/embedded/env/MonetDBEmbeddedException");
		(*env)->ThrowNew(env, exceptionCls, "The MonetDB Embedded database is not running!");
	}
	return result;
}
