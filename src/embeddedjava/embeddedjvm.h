/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2017 MonetDB B.V.
 */

/*
 * H. Muehleisen, M. Raasveldt, Pedro Ferreira
 * Inverse RAPI
 */
#ifndef _EMBEDDEDJVM_LIB_
#define _EMBEDDEDJVM_LIB_

#include "monetdb_config.h"
#include "embedded.h"
#include "sql.h"

char* monetdb_find_table(monetdb_connection conn, sql_table** table, const char* schema_name, const char* table_name);
int getAutocommitFlag(monetdb_connection conn);

#endif
