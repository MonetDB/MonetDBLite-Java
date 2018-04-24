/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.resultset;

/**
 * The result of a execute query.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public final class ExecResultSet {

	/** True for a result set query, false for an update */
	private final boolean status;

	/** The query result set if exists */
	private final QueryResultSet resultSet;

	/** The number of rows if exists */
	private final int numberOfRows;

	public ExecResultSet(boolean status, QueryResultSet resultSet, int numberOfRows) {
		this.status = status;
		this.resultSet = resultSet;
		this.numberOfRows = numberOfRows;
	}

	/**
	 * Get the status response.
	 *
	 * @return The status response
	 */
	public boolean getStatus() {
		return status;
	}

	/**
	 * Get the result set if available.
	 *
	 * @return The result set if available
	 */
	public QueryResultSet getResultSet() {
		return resultSet;
	}

	/**
	 * Get the number of rows if available.
	 *
	 * @return The number of rows if available
	 */
	public int getNumberOfRows() {
		return numberOfRows;
	}
}
