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

#include "monetdb_config.h"
#include "embedded.h"
#include "embeddedjvm.h"

#include "mal.h"
#include "mal_client.h"
#include "sql.h"
#include "sql_mvc.h"

char* monetdb_find_table(monetdb_connection conn, sql_table** table, const char* schema_name, const char* table_name) {
	mvc *m;
	sql_schema *s;
	char *msg = MAL_SUCCEED;
	Client connection = (Client) conn;

	if ((msg = getSQLContext(connection, NULL, &m, NULL)) != NULL)
		return msg;
	s = mvc_bind_schema(m, schema_name);
	if (s == NULL)
		return createException(MAL, "embedded", SQLSTATE(3F000) "Missing schema!");
	*table = mvc_bind_table(m, s, table_name);
	if ((*table) == NULL)
		return createException(MAL, "embedded", SQLSTATE(3F000) "Could not find table %s", table_name);
	return msg;
}

int getAutocommitFlag(monetdb_connection conn) {
	Client connection = (Client) conn;
	mvc* m = ((backend *) connection->sqlcontext)->mvc;
	return m->session->auto_commit;
}
