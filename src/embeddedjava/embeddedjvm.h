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
#include "monet_options.h"
#include "embedded.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_linker.h"
#include "gdk_utils.h"
#include "sql_scenario.h"
#include "sql_execute.h"
#include "sql.h"
#include "sql_mvc.h"
#include "res_table.h"

char* monetdb_find_table(monetdb_connection conn, sql_table** table, const char* schema_name, const char* table_name);
char* sendAutoCommitCommand(monetdb_connection conn, int flag, int* result);
void sendReleaseCommand(monetdb_connection conn, int commandId);
void sendCloseCommand(monetdb_connection conn, int commandId);
void sendReplySizeCommand(monetdb_connection conn, lng size);
int getAutocommitFlag(monetdb_connection conn);
void setAutocommitFlag(monetdb_connection conn, int autoCommit);

#endif
