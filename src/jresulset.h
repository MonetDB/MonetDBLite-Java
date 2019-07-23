/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2019 MonetDB B.V.
 */

#ifndef MONETDBLITE_JRESULTSET_H
#define MONETDBLITE_JRESULTSET_H

#include "monetdb_config.h"
#include "monetdb_embedded.h"
#include "jni.h"
#include "gdk.h"
#include "sql.h"

/*
 * Pedro Ferreira
 * The JResultSet holds the output of a MonetDB query to be translated into Java primitives/Objects
 */

typedef struct {
	monetdb_connection conn;
	monetdb_result *output;
	BAT** bats;
	res_col** cols;
} JResultSet;

java_export char* createResultSet(monetdb_connection conn, JResultSet** res, monetdb_result* output);
java_export void freeResultSet(JResultSet* thisResultSet);

#endif //MONETDBLITE_JRESULTSET_H
