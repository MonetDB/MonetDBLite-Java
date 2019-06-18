/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.resultset;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedConnection;

/**
 * Embedded MonetDB query result extension for prepared statements.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public final strictfp class PreparedQueryResultSet extends QueryResultSet {

	/** The prepared statement ID */
	private final int preparedID;

	private PreparedQueryResultSet(MonetDBEmbeddedConnection connection, long structPointer, int numberOfColumns,
								   int numberOfRows, int[] typesIDs, int preparedID) {
		super(connection, structPointer, numberOfColumns, numberOfRows, typesIDs);
		this.preparedID = preparedID;
	}

	/**
	 * Get the prepared statement ID.
	 *
	 * @return The prepared statement ID
	 */
	public int getPreparedID() {
		return preparedID;
	}
}
