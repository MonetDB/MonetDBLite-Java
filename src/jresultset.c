/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "monetdb_embedded.h"

#include "jresulset.h"
#include "gdk.h"
#include "mal.h"
#include "res_table.h"
#include "mal_exception.h"

char*
createResultSet(monetdb_connection conn, JResultSet** res, monetdb_result* output)
{
	size_t numberOfColumns, i, j;
	BAT** dearBats = NULL;
	char *msg = MAL_SUCCEED;
	JResultSet *thisResultSet;

	*res = (JResultSet*) GDKzalloc(sizeof(JResultSet));
	thisResultSet = *res;
	if(!thisResultSet) {
		msg = createException(MAL, "embedded", MAL_MALLOC_FAIL);
		goto cleanup;
	}

	thisResultSet->conn = conn;
	thisResultSet->output = output;
	if (output && output->ncols > 0) {
		numberOfColumns = output->ncols;
		thisResultSet->bats = (BAT**) GDKmalloc(sizeof(BAT*) * numberOfColumns);
		thisResultSet->cols = (res_col**) GDKmalloc(sizeof(res_col*) * numberOfColumns);
		if (!thisResultSet->bats || !thisResultSet->cols) {
			msg = createException(MAL, "embedded", MAL_MALLOC_FAIL);
			goto cleanup;
		}

		dearBats = thisResultSet->bats;
		for (i = 0; i < numberOfColumns; i++) {
			res_col* col = NULL;
			if((msg = monetdb_result_fetch_rawcol(conn, &col, output, i)) != MAL_SUCCEED)
				goto cleanup;
			thisResultSet->cols[i] = col;
			if(!(dearBats[i] = BATdescriptor(col->b))) {
				msg = createException(MAL, "embedded", RUNTIME_OBJECT_MISSING);
				goto cleanup;
			}
		}
	}

	return MAL_SUCCEED;
cleanup:
	if(thisResultSet) {
		char *other;
		if(dearBats)
			for (j = 0; j < i; j++)
				BBPunfix(dearBats[j]->batCacheid);
		if((other = monetdb_cleanup_result(thisResultSet->conn, thisResultSet->output)) != MAL_SUCCEED)
			freeException(other);
		if(thisResultSet->bats)
			GDKfree(thisResultSet->bats);
		if(thisResultSet->cols)
			GDKfree(thisResultSet->cols);
		GDKfree(thisResultSet);
	}
	return msg;
}

void
freeResultSet(JResultSet* thisResultSet)
{
	size_t numberOfColumns, i;
	BAT **dearBats;

	if(thisResultSet) {
		if(thisResultSet->bats) {
			dearBats = thisResultSet->bats;
			if(thisResultSet->output) {
				numberOfColumns = thisResultSet->output->ncols;
				for (i = 0; i < numberOfColumns; i++)
					BBPunfix(dearBats[i]->batCacheid);
			}
			GDKfree(dearBats);
			thisResultSet->bats = NULL;
		}
		if(thisResultSet->cols) {
			GDKfree(thisResultSet->cols);
			thisResultSet->cols = NULL;
		}
		if(thisResultSet->output) {
			char* other;
			if((other = monetdb_cleanup_result(thisResultSet->conn, thisResultSet->output)) != MAL_SUCCEED)
				freeException(other);
			thisResultSet->output = NULL;
		}
		GDKfree(thisResultSet);
	}
}
