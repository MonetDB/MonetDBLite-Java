/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2017 MonetDB B.V.
 */

#ifndef MONETDB_JNI_H
#define MONETDB_JNI_H

#if defined(__linux) || defined(__linux__)
#include "inclinux/jni_md.h"
#include "inclinux/jni.h"
#elif defined__APPLE__)
#include "incmacosx/jni_md.h"
#include "incmacosx/jni.h"
#elif defined(NATIVE_WIN32) || defined(_WIN64)
#include "incwindows/jni_md.h"
#include "incwindows/jni.h"
#else
#error Could not determine the running OS for JNI header files
#endif

#endif //MONETDB_JNI_H
