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

#include "embeddedjvm.h"

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

char* monetdb_find_table(monetdb_connection conn, sql_table** table, const char* schema_name, const char* table_name) {
	mvc *m;
	sql_schema *s;
	char *msg = MAL_SUCCEED;
	Client connection = (Client) conn;

	if ((msg = getSQLContext(connection, NULL, &m, NULL)) != NULL)
		return msg;
	s = mvc_bind_schema(m, schema_name);
	if (s == NULL)
		return createException(MAL, "embedded", "Missing schema!");
	*table = mvc_bind_table(m, s, table_name);
	if ((*table) == NULL)
		return createException(MAL, "embedded", "Could not find table %s", table_name);
	return NULL;
}

char* sendAutoCommitCommand(monetdb_connection conn, int flag, int* result) {
	Client connection = (Client) conn;
	mvc* m = ((backend *) connection->sqlcontext)->mvc;
	char *msg = MAL_SUCCEED;
	int commit = (!m->session->auto_commit && flag);

	m->session->auto_commit = (flag) != 0;
	m->session->ac_on_commit = m->session->auto_commit;
	*result = m->session->auto_commit;
	if (m->session->active) {
		if (commit && mvc_commit(m, 0, NULL) < 0) {
			msg = createException(MAL, "embedded", "auto_commit (commit) failed");
		} else if (!commit && mvc_rollback(m, 0, NULL) < 0) {
			msg = createException(MAL, "embedded", "auto_commit (rollback) failed");
		}
	}
	SQLautocommit(connection, m);

	return msg;
}

void sendReleaseCommand(monetdb_connection conn, int commandId) {
	Client connection = (Client) conn;
	mvc* m = ((backend *) connection->sqlcontext)->mvc;

	if(m->qc) {
		cq *q = qc_find(m->qc, commandId);
		if (q) {
			qc_delete(m->qc, q);
		}
	}
}

void sendCloseCommand(monetdb_connection conn, int tableID) {
	Client connection = (Client) conn;
	mvc* m = ((backend *) connection->sqlcontext)->mvc;
	res_table *t = res_tables_find(m->results, tableID);

	if (t) {
		m->results = res_tables_remove(m->results, t);
	}
}

void sendReplySizeCommand(monetdb_connection conn, lng size) {
	Client connection = (Client) conn;
	mvc* m = ((backend *) connection->sqlcontext)->mvc;

	if(size >= -1) {
		m->reply_size = size;
	}
}

int getAutocommitFlag(monetdb_connection conn) {
	Client connection = (Client) conn;
	mvc* m = ((backend *) connection->sqlcontext)->mvc;
	return m->session->auto_commit;
}

void setAutocommitFlag(monetdb_connection conn, int autoCommit) {
	Client connection = (Client) conn;
	mvc* m = ((backend *) connection->sqlcontext)->mvc;

	m->session->auto_commit = autoCommit;
	if(autoCommit == 0) {
		m->session->status = 0;
	}
	SQLautocommit(connection, m);
}

int setMonetDB5LibraryPathEmbedded(const char* path) {
	return setMonetDB5LibraryPath(path);
}

void freeMonetDB5LibraryPathEmbedded(void) {
	freeMonetDB5LibraryPath();
}
