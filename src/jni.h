/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2019 MonetDB B.V.
 */

#ifndef MONETDB_JNI_H
#define MONETDB_JNI_H

#include "monetdb_config.h"

#define java_export extern __attribute__((__visibility__("hidden")))

#if defined(__linux) || defined(__linux__) || defined(linux)
#include "inclinux/jni_md.h"
#include "inclinux/jni.h"
#elif defined(__APPLE__) || defined(__MACH__)
#include "incmacosx/jni_md.h"
#include "incmacosx/jni.h"
#elif defined(_WIN32) || defined(__WIN32__) || defined(WIN32) || defined(NATIVE_WIN32)
#include "incwindows/jni_md.h"
#include "incwindows/jni.h"
#else
#error MonetDBLite-Java is not available for the target OS
#endif

#endif //MONETDB_JNI_H
