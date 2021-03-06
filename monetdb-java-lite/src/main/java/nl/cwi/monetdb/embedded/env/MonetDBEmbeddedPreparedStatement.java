/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.env;

import nl.cwi.monetdb.embedded.mapping.MonetDBToJavaMapping;
import nl.cwi.monetdb.embedded.resultset.ExecResultSet;
import nl.cwi.monetdb.embedded.resultset.PreparedQueryResultSet;
import nl.cwi.monetdb.embedded.resultset.QueryResultSet;
import nl.cwi.monetdb.jdbc.MonetWrapper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.URL;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * A prepared statement result set. The user should replace the respective inputs.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public final class MonetDBEmbeddedPreparedStatement extends AbstractConnectionResult {

	/**
	 * The prepared statement ID
	 */
	private int id;
	/**
	 * The number of rows (which means the number of ?)
	 */
	private final int size;
	/**
	 * The number of columns in the result set (either 3 or 6)
	 */
	private final int rscolcnt;
	/**
	 * The user input values to submit
	 */
	private final String[] values;

	/**
	 * The MonetDB internal types
	 */
	private final String[] monetdbType;
	/**
	 * The number of digits
	 */
	private final int[] digits;
	/**
	 * The scale
	 */
	private final int[] scale;
	/**
	 * The value schema
	 */
	private final String[] schema;
	/**
	 * The value table
	 */
	private final String[] table;
	/**
	 * The value column
	 */
	private final String[] column;

	/**
	 * Format dates, times and timestamps
	 */
	private final SimpleDateFormat mTimestampZ;
	private final SimpleDateFormat mTimestamp;
	private final SimpleDateFormat mTimeZ;
	private final SimpleDateFormat mTime;
	private final SimpleDateFormat mDate;

	MonetDBEmbeddedPreparedStatement(MonetDBEmbeddedConnection connection, PreparedQueryResultSet qrs)
			throws MonetDBEmbeddedException {
		super(connection);

		this.id = qrs.getPreparedID();
		this.size = qrs.getNumberOfRows();
		this.rscolcnt = qrs.getNumberOfColumns();
		this.values = new String[this.size];

		this.monetdbType = new String[this.size];
		this.digits = new int[this.size];
		this.scale = new int[this.size];
		this.schema = new String[this.size];
		this.table = new String[this.size];
		this.column = new String[this.size];

		qrs.getStringColumnByIndex(3, this.monetdbType);
		qrs.getIntColumnByIndex(4, this.digits);
		qrs.getIntColumnByIndex(5, this.scale);
		qrs.getStringColumnByIndex(6, this.schema);
		qrs.getStringColumnByIndex(7, this.table);
		qrs.getStringColumnByIndex(8, this.column);
		qrs.close();

		mTimestampZ = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
		mTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		mTimeZ = new SimpleDateFormat("HH:mm:ss.SSSZ");
		mTime = new SimpleDateFormat("HH:mm:ss.SSS");
		mDate = new SimpleDateFormat("yyyy-MM-dd");
	}

	@Override
	public int getNumberOfColumns() {
		return this.rscolcnt;
	}

	@Override
	public int getNumberOfRows() {
		return this.size;
	}

	@Override
	public void getColumnNames(String[] input) throws MonetDBEmbeddedException {
		if(this.rscolcnt != 3) {
			System.arraycopy(this.column, 0, input, 0, Math.min(this.column.length, input.length));
		} else {
			throw new MonetDBEmbeddedException("The column names information is not available in Prepared Statement");
		}
	}

	@Override
	public void getColumnTypes(String[] input) throws MonetDBEmbeddedException {
		System.arraycopy(this.monetdbType, 0, input, 0, Math.min(this.monetdbType.length, input.length));
	}

	@Override
	public void getMappings(MonetDBToJavaMapping[] input) throws MonetDBEmbeddedException {
		for(int i = 0 ; i < input.length && i < monetdbType.length; i++) {
			input[i] = MonetDBToJavaMapping.getJavaMappingFromMonetDBString(monetdbType[i]);
		}
	}

	@Override
	public void getColumnDigits(int[] input) throws MonetDBEmbeddedException {
		System.arraycopy(this.digits, 0, input, 0, Math.min(this.digits.length, input.length));
	}

	@Override
	public void getColumnScales(int[] input) throws MonetDBEmbeddedException {
		System.arraycopy(this.scale, 0, input, 0, Math.min(this.scale.length, input.length));
	}

	/**
	 * Tells if the prepared statement was closed or not
	 *
	 * @return A boolean indicating if the prepared statement has been closed or not
	 */
	public boolean isPreparedStatementClosed() { return this.id == 0; }

	/**
	 * Clears the current parameter values immediately.
	 */
	public void clearParameters() {
		for (int i = 0; i < values.length; i++) {
			values[i] = null;
		}
	}

	/**
	 * Transforms the prepared query into a simple SQL query by replacing the ?'s with the given column contents.
	 * Mind that the JDBC specs allow `reuse' of a value for a column over multiple executes.
	 *
	 * @return the simple SQL string for the prepare query
	 * @throws MonetDBEmbeddedException if not all columns are set
	 */
	private String transform() throws MonetDBEmbeddedException {
		StringBuilder buf = new StringBuilder(8 + 12 * size);
		buf.append("execute ");
		buf.append(id);
		buf.append("(");
		// check if all columns are set and do a replace
		int col = 0;
		for (int i = 0; i < size; i++) {
			if (column[i] != null)
				continue;
			col++;
			if (col > 1)
				buf.append(',');
			if (values[i] == null)
				throw new MonetDBEmbeddedException("Cannot execute, parameter " + col + " is missing.");

			buf.append(values[i]);
		}
		buf.append(");");
		return buf.toString();
	}

	/**
	 * Executes the SQL statement in this PreparedStatement object, which may be any kind of SQL statement. Some
	 * prepared statements return multiple results; the execute method handles these complex statements as well as the
	 * simpler form of statements handled by the methods executeQuery and executeUpdate.
	 *
	 * @return true if the first result is a ResultSet object; false if the first result is an update count or there is
	 * no result
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public boolean execute() throws MonetDBEmbeddedException {
		ExecResultSet result = this.getConnection().executePrepareStatement(this.transform());
		boolean res = result.getStatus();
		if(res) { //Why JDBC???
			result.getResultSet().close();
		}
		return result.getStatus();
	}

	/**
	 * Like the previous one, but without returning any result.
	 *
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void executeAndIgnore() throws MonetDBEmbeddedException {
		this.getConnection().executePreparedStatementAndIgnore(this.transform());
	}

	/**
	 * Executes the SQL query in this PreparedStatement object and returns QueryResultSet ResultSet object generated by
	 * the query.
	 *
	 * @return a QueryResultSet object that contains the data produced by the query never null
	 * @throws MonetDBEmbeddedException if a database access error occurs or the SQL statement does not return a
	 *                                  QueryResultSet
	 */
	public QueryResultSet executeQuery() throws MonetDBEmbeddedException {
		ExecResultSet result = this.getConnection().executePrepareStatement(this.transform());
		if (!result.getStatus()) {
			throw new MonetDBEmbeddedException("Query did not produce a result set");
		}
		return result.getResultSet();
	}

	/**
	 * Executes the SQL statement in this PreparedStatement object, which must be an SQL INSERT, UPDATE or DELETE
	 * statement; or an SQL statement that returns nothing, such as a DDL statement.
	 *
	 * @return either (1) the row count for INSERT, UPDATE, or DELETE
	 * statements or (2) 0 for SQL statements that return nothing
	 * @throws MonetDBEmbeddedException if a database access error occurs or the SQL statement returns a ResultSet
	 */
	public int executeUpdate() throws MonetDBEmbeddedException {
		ExecResultSet result = this.getConnection().executePrepareStatement(this.transform());
		if (result.getStatus()) {
			throw new MonetDBEmbeddedException("Query produced a result set");
		}
		return result.getNumberOfRows();
	}

	/**
	 * Returns the index (0..size-1) in the backing arrays for the given resultset column number or a
	 * MonetDBEmbeddedException when not found
	 */
	private int getColumnIdx(int colnr) throws MonetDBEmbeddedException {
		int curcol = 0;
		for (int i = 0; i < size; i++) {
			if (column[i] == null)
				continue;
			curcol++;
			if (curcol == colnr)
				return i;
		}
		throw new MonetDBEmbeddedException("No such column with index: " + colnr);
	}

	/**
	 * Returns the index (0..size-1) in the backing arrays for the given parameter number or an MonetDBEmbeddedException
	 * when not found
	 */
	private int getParamIdx(int paramnr) throws MonetDBEmbeddedException {
		int curparam = 0;
		for (int i = 0; i < size; i++) {
			if (column[i] != null)
				continue;
			curparam++;
			if (curparam == paramnr)
				return i;
		}
		throw new MonetDBEmbeddedException("No such parameter with index: " + paramnr);
	}

	/**
	 * Sets the given index with the supplied value. If the given index is out of bounds, and MonetDBEmbeddedException
	 * is thrown. The given value should never be null.
	 *
	 * @param index the parameter index
	 * @param val the exact String representation to set
	 * @throws MonetDBEmbeddedException if the given index is out of bounds
	 */
	private void setValue(int index, String val) throws MonetDBEmbeddedException {
		values[getParamIdx(index)] = val;
	}

	/**
	 * Sets the designated parameter to SQL NULL.
	 *
	 * Note: You must specify the parameter's SQL type.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param sqlType the SQL type code defined in java.sql.Types
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setNull(int parameterIndex, int sqlType) throws MonetDBEmbeddedException {
		// we discard the given type here, the backend converts the
		// value NULL to whatever it needs for the column
		setValue(parameterIndex, "NULL");
	}

	/**
	 * Sets the designated parameter to SQL NULL. This version of the method setNull should be used for user-defined
	 * types and REF type parameters. Examples of user-defined types include: STRUCT, DISTINCT, JAVA_OBJECT,
	 * and named array types.
	 *
	 * Note: To be portable, applications must give the SQL type code and the
	 * fully-qualified SQL type name when specifying a NULL user-defined or REF
	 * parameter. In the case of a user-defined type the name is the type name
	 * of the parameter itself. For a REF parameter, the name is the type name
	 * of the referenced type. If a JDBC driver does not need the type code or
	 * type name information, it may ignore it. Although it is intended for
	 * user-defined and Ref parameters, this method may be used to set a null
	 * parameter of any JDBC type. If the parameter does not have a
	 * user-defined or REF type, the given typeName is ignored.
	 *
	 * @param paramIndex the first parameter is 1, the second is 2, ...
	 * @param sqlType a value from java.sql.Types
	 * @param typeName the fully-qualified name of an SQL user-defined type; ignored if the parameter is not a
	 *                 user-defined type or REF
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setNull(int paramIndex, int sqlType, String typeName) throws MonetDBEmbeddedException {
		// MonetDB/SQL's NULL needs no type
		setNull(paramIndex, sqlType);
	}

	/**
	 * Sets the designated parameter to the given java.math.BigDecimal value.
	 *
	 * @param idx the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setBigDecimal(int idx, BigDecimal x) throws MonetDBEmbeddedException {
		// get array position
		int i = getParamIdx(idx);
		// round to the scale of the DB:
		x = x.setScale(scale[i], RoundingMode.HALF_UP);
		// if precision is now greater than that of the db, throw an error:
		if (x.precision() > digits[i]) {
			throw new MonetDBEmbeddedException("DECIMAL value exceeds allowed digits/scale: " + x.toPlainString() +
					" (" + digits[i] + "/" + scale[i] + ")");
		}
		// MonetDB doesn't like leading 0's, since it counts them as part of
		// the precision, so let's strip them off. (But be careful not to do
		// this to the exact number "0".)  Also strip off trailing
		// numbers that are inherent to the double representation.
		String xStr = x.toPlainString();
		int dot = xStr.indexOf('.');
		if (dot >= 0)
			xStr = xStr.substring(0, Math.min(xStr.length(), dot + 1 + scale[i]));
		while (xStr.startsWith("0") && xStr.length() > 1)
			xStr = xStr.substring(1);
		setValue(idx, xStr);
	}

	/**
	 * Sets the designated parameter to the given Blob object. The driver converts this to an SQL BLOB value when it
	 * sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x a Blob object that maps an SQL BLOB value
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setBlob(int parameterIndex, Blob x) throws MonetDBEmbeddedException {
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}
		byte[] bbytes;
		try {
			long length = x.length();
			bbytes = x.getBytes(0, (int) length);
		} catch (SQLException ex) {
			throw new MonetDBEmbeddedException(ex);
		}
		setBytes(parameterIndex, bbytes);
	}

	/**
	 * Sets the designated parameter to the given Java boolean value. The driver converts this to an SQL BOOLEAN value
	 * when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setBoolean(int parameterIndex, boolean x) throws MonetDBEmbeddedException {
		setValue(parameterIndex, Boolean.toString(x));
	}

	/**
	 * Sets the designated parameter to the given Java byte value. The driver converts this to an SQL TINYINT value when
	 * it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setByte(int parameterIndex, byte x) throws MonetDBEmbeddedException {
		setValue(parameterIndex, Byte.toString(x));
	}

	private static final String HEXES = "0123456789ABCDEF";

	/**
	 * Sets the designated parameter to the given Java array of bytes. The driver converts this to an SQL VARBINARY or
	 * LONGVARBINARY (depending on the argument's size relative to the driver's limits on VARBINARY values) when it
	 * sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setBytes(int parameterIndex, byte[] x) throws MonetDBEmbeddedException {
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}
		StringBuilder hex = new StringBuilder(x.length * 2);
		for (byte aX : x) {
			hex.append(HEXES.charAt((aX & 0xF0) >> 4)).append(HEXES.charAt((aX & 0x0F)));
		}
		setValue(parameterIndex, "blob '" + hex.toString() + "'");
	}

	/**
	 * Sets the designated parameter to the given Clob object. The driver converts this to an SQL CLOB value when it
	 * sends it to the database.
	 *
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param x a Clob object that maps an SQL CLOB value
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setClob(int i, Clob x) throws MonetDBEmbeddedException {
		if (x == null) {
			setNull(i, -1);
			return;
		}
		String bbytes;
		try {
			int length = (int) x.length();
			bbytes = x.getSubString(1L, length);
		} catch (SQLException ex) {
			throw new MonetDBEmbeddedException(ex);
		}
		// simply serialise the CLOB into a variable for now... far from efficient, but might work for a few cases...
		// be on your marks: we have to cast the length down!
		setString(i, bbytes);
	}

	/**
	 * Sets the designated parameter to the given java.sql.Date value. The driver converts this to an SQL DATE value
	 * when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setDate(int parameterIndex, Date x) throws MonetDBEmbeddedException {
		setDate(parameterIndex, x, null);
	}

	/**
	 * Sets the designated parameter to the given java.sql.Date value, using
	 * the given Calendar object. The driver uses the Calendar object to
	 * construct an SQL DATE value, which the driver then sends to the
	 * database. With a Calendar object, the driver can calculate the date
	 * taking into account a custom timezone. If no Calendar object is
	 * specified, the driver uses the default timezone, which is that of the
	 * virtual machine running the application.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @param cal the Calendar object the driver will use to construct the date
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setDate(int parameterIndex, Date x, Calendar cal) throws MonetDBEmbeddedException {
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}
		if (cal == null) {
			setValue(parameterIndex, "date '" + x.toString() + "'");
		} else {
			mDate.setTimeZone(cal.getTimeZone());
			setValue(parameterIndex, "date '" + mDate.format(x) + "'");
		}
	}

	/**
	 * Sets the designated parameter to the given Java double value. The driver converts this to an SQL DOUBLE value
	 * when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setDouble(int parameterIndex, double x) throws MonetDBEmbeddedException {
		setValue(parameterIndex, Double.toString(x));
	}

	/**
	 * Sets the designated parameter to the given Java float value. The driver converts this to an SQL FLOAT value when
	 * it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setFloat(int parameterIndex, float x) throws MonetDBEmbeddedException {
		setValue(parameterIndex, Float.toString(x));
	}

	/**
	 * Sets the designated parameter to the given Java int value. The driver converts this to an SQL INTEGER value when
	 * it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setInt(int parameterIndex, int x) throws MonetDBEmbeddedException {
		setValue(parameterIndex, Integer.toString(x));
	}

	/**
	 * Sets the designated parameter to the given Java long value. The driver converts this to an SQL BIGINT value when
	 * it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setLong(int parameterIndex, long x) throws MonetDBEmbeddedException {
		setValue(parameterIndex, Long.toString(x));
	}

	/**
	 * Sets the value of the designated parameter using the given object.  The second parameter must be of type Object;
	 * therefore, the java.lang equivalent objects should be used for built-in types.
	 *
	 * This method throws an exception if there is an ambiguity, for example, if the object is of a class implementing
	 * more than one of the interfaces named above.
	 *
	 * @param index the first parameter is 1, the second is 2, ...
	 * @param x the object containing the input parameter value
	 * @throws MonetDBEmbeddedException if a database access error occurs or the type of the given object is ambiguous
	 */
	public void setObject(int index, Object x) throws MonetDBEmbeddedException {
		int targetSqlType = MonetDBToJavaMapping
				.getJavaMappingFromMonetDBStringOrdinalValue(monetdbType[getParamIdx(index)]);
		setObject(index, x, targetSqlType);
	}

	/**
	 * Sets the value of the designated parameter with the given object. This method is like the method setObject below,
	 * except that it assumes a scale of zero.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the object containing the input parameter value
	 * @param targetSqlType the Java ordinal value from MonetDBToJavaMapping
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws MonetDBEmbeddedException {
		setObject(parameterIndex, x, targetSqlType, 0);
	}

	/**
	 * Sets the value of the designated parameter with the given object. The second argument must be an object type;
	 * for integral values, the java.lang equivalent objects should be used.
	 *
	 * Note that this method may be used to pass database-specific abstract data types.
	 *
	 * To meet the requirements of this interface, the Java object is converted in the driver, instead of using a SQL
	 * CAST construct.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the object containing the input parameter value
	 * @param targetSqlType the Java ordinal value from MonetDBToJavaMapping
	 * @param scale for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types, this is the number of digits after the
	 *              decimal point. For Java Object types InputStream and Reader, this is the length of the data in the
	 *              stream or reader.  For all other types, this value will be ignored.
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 * @see Types
	 */
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws MonetDBEmbeddedException {
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}
		if (x instanceof String) {
			setString(parameterIndex, (String)x);
		} else if (x instanceof BigDecimal || x instanceof Byte || x instanceof Short || x instanceof Integer ||
				x instanceof Long || x instanceof Float || x instanceof Double) {
			Number num = (Number)x;
			switch (targetSqlType) {
				case 4: //TINYINT
					setByte(parameterIndex, num.byteValue());
					break;
				case 5: //SMALLINT
					setShort(parameterIndex, num.shortValue());
					break;
				case 6: //INTEGER
				case 11: //MonthInterval
					setInt(parameterIndex, num.intValue());
					break;
				case 7: //BIGINT
				case 12: //SecondInterval
					if (x instanceof BigDecimal) {
						BigDecimal bd = (BigDecimal)x;
						setLong(parameterIndex, bd.setScale(scale, BigDecimal.ROUND_HALF_UP).longValue());
					} else {
						setLong(parameterIndex, num.longValue());
					}
					break;
				case 9: //REAL
					setFloat(parameterIndex, num.floatValue());
					break;
				case 10: //DOUBLE
					setDouble(parameterIndex, num.doubleValue());
					break;
				case 8: //DECIMAL
					if (x instanceof BigDecimal) {
						setBigDecimal(parameterIndex, (BigDecimal)x);
					} else {
						setBigDecimal(parameterIndex,
								new BigDecimal(num.doubleValue()));
					}
					break;
				case 0: //BOOLEAN
					if (num.doubleValue() != 0.0) {
						setBoolean(parameterIndex, true);
					} else {
						setBoolean(parameterIndex, false);
					}
					break;
				case 1: //CHAR:
				case 2: //VARCHAR:
				case 3: //CLOB:
					setString(parameterIndex, x.toString());
					break;
				default:
					throw new MonetDBEmbeddedException("Conversion not allowed");
			}
		} else if (x instanceof Boolean) {
			boolean val = (Boolean) x;
			switch (targetSqlType) {
				case 4: //TINYINT
					setByte(parameterIndex, (byte)(val ? 1 : 0));
					break;
				case 5: //SMALLINT
					setShort(parameterIndex, (short)(val ? 1 : 0));
					break;
				case 6: //INTEGER
				case 11: //MonthInterval
					setInt(parameterIndex, (val ? 1 : 0));  // do not cast to (int) as it generates a compiler warning
					break;
				case 7: //BIGINT
				case 12: //SecondInterval
					setLong(parameterIndex, (long)(val ? 1 : 0));
					break;
				case 9: //REAL
					setFloat(parameterIndex, (float)(val ? 1.0 : 0.0));
					break;
				case 10: //DOUBLE
					setDouble(parameterIndex, (val ? 1.0 : 0.0));  // do no cast to (double) as it generates a compiler warning
					break;
				case 8: //DECIMAL
				{
					BigDecimal dec;
					try {
						dec = new BigDecimal(val ? 1.0 : 0.0);
					} catch (NumberFormatException e) {
						throw new MonetDBEmbeddedException("Internal error: unable to create template BigDecimal: " + e.getMessage());
					}
					setBigDecimal(parameterIndex, dec);
				} break;
				case 0: //BOOLEAN
					setBoolean(parameterIndex, val);
					break;
				case 1: //CHAR:
				case 2: //VARCHAR:
				case 3: //CLOB:
					setString(parameterIndex, x.toString());
					break;
				default:
					throw new MonetDBEmbeddedException("Conversion not allowed");
			}
		} else if (x instanceof BigInteger) {
			BigInteger num = (BigInteger)x;
			switch (targetSqlType) {
				case 7: //BIGINT
				case 12: //SecondInterval
					setLong(parameterIndex, num.longValue());
					break;
				case 8: //DECIMAL
				{
					BigDecimal dec;
					try {
						dec = new BigDecimal(num);
					} catch (NumberFormatException e) {
						throw new MonetDBEmbeddedException("Internal error: unable to create template BigDecimal: " + e.getMessage());
					}
					setBigDecimal(parameterIndex, dec);
				} break;
				case 1: //CHAR:
				case 2: //VARCHAR:
				case 3: //CLOB:
					setString(parameterIndex, x.toString());
					break;
				default:
					throw new MonetDBEmbeddedException("Conversion not allowed");
			}
		} else if (x instanceof byte[]) {
			switch (targetSqlType) {
				case 18: //Blob:
					setBytes(parameterIndex, (byte[])x);
					break;
				default:
					throw new MonetDBEmbeddedException("Conversion not allowed");
			}
		} else if (x instanceof java.sql.Date || x instanceof Timestamp || x instanceof Time || x instanceof Calendar ||
				x instanceof java.util.Date)
		{
			switch (targetSqlType) {
				case 15: //Types.DATE:
					if (x instanceof java.sql.Date) {
						setDate(parameterIndex, (java.sql.Date)x);
					} else if (x instanceof Timestamp) {
						setDate(parameterIndex, new java.sql.Date(((Timestamp)x).getTime()));
					} else if (x instanceof java.util.Date) {
						setDate(parameterIndex, new java.sql.Date(((java.util.Date)x).getTime()));
					} else {
						setDate(parameterIndex, new Date(((Calendar)x).getTimeInMillis()));
					}
					break;
				case 13: //Types.TIME:
				case 14: //TIMETZ:
					if (x instanceof Time) {
						setTime(parameterIndex, (Time)x);
					} else if (x instanceof Timestamp) {
						setTime(parameterIndex, new Time(((Timestamp)x).getTime()));
					} else if (x instanceof java.util.Date) {
						setTime(parameterIndex, new java.sql.Time(((java.util.Date)x).getTime()));
					} else {
						setTime(parameterIndex, new Time(((Calendar)x).getTimeInMillis()));
					}
					break;
				case 16: //Types.TIMESTAMP:
				case 17: //TIMESTAMPTZ:
					if (x instanceof Timestamp) {
						setTimestamp(parameterIndex, (Timestamp)x);
					} else if (x instanceof java.sql.Date) {
						setTimestamp(parameterIndex, new Timestamp(((java.sql.Date)x).getTime()));
					} else if (x instanceof java.util.Date) {
						setTimestamp(parameterIndex, new java.sql.Timestamp(((java.util.Date)x).getTime()));
					} else {
						setTimestamp(parameterIndex, new Timestamp(((Calendar)x).getTimeInMillis()));
					}
					break;
				case 1: //CHAR:
				case 2: //VARCHAR:
				case 3: //CLOB:
					setString(parameterIndex, x.toString());
					break;
				default:
					throw new MonetDBEmbeddedException("Conversion not allowed");
			}
		} else if (x instanceof Blob) {
			setBlob(parameterIndex, (Blob)x);
		} else if (x instanceof Clob) {
			setClob(parameterIndex, (Clob)x);
		} else if (x instanceof java.net.URL) {
			setURL(parameterIndex, (java.net.URL)x);
		} else {
			throw new MonetDBEmbeddedException("No support for setObject() with object of type: " + x.getClass().getName());
		}
	}

	/**
	 * Sets the designated parameter to the given Java short value. The driver converts this to an SQL SMALLINT value
	 * when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setShort(int parameterIndex, short x) throws MonetDBEmbeddedException {
		setValue(parameterIndex, Short.toString(x));
	}

	/**
	 * Sets the designated parameter to the given Java String value. The driver converts this to an SQL VARCHAR or
	 * LONGVARCHAR value (depending on the argument's size relative to the driver's limits on VARCHAR values) when
	 * it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setString(int parameterIndex, String x) throws MonetDBEmbeddedException {
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}
		int paramIdx = getParamIdx(parameterIndex);	// this will throw a SQLException if parameter can not be found

		/* depending on the parameter data type (as expected by MonetDB) we
		   may need to add the data type as cast prefix to the parameter value */
		int targetSqlType = MonetDBToJavaMapping
				.getJavaMappingFromMonetDBStringOrdinalValue(monetdbType[getParamIdx(parameterIndex)]);
		String paramMonetdbType = monetdbType[paramIdx];

		switch (targetSqlType) {
			case 1: //CHAR:
			case 2: //VARCHAR:
			case 3: //CLOB:
			{
				String castprefix = "";
				switch (paramMonetdbType) {
					// some MonetDB specific data types require a cast prefix
					case "url":
						try {
							// also check if x represents a valid url string to prevent
							// failing exec #(..., ...) calls which destroy the prepared statement, see bug 6351
							java.net.URL url_obj = new java.net.URL(x);
						} catch (java.net.MalformedURLException mue) {
							throw new MonetDBEmbeddedException("Conversion of string: " + x + " to parameter data type " + paramMonetdbType + " failed. " + mue.getMessage());
						}
						castprefix = "url ";
						break;
				}
				/* in specific cases prefix the string with: inet or json or url or uuid */
				setValue(parameterIndex,
						castprefix + "'" + x.replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'") + "'");
				break;
			}
			case 4: //TINYINT
			case 5: //SMALLINT
			case 6: //INTEGER
			case 11: //MonthInterval
			case 7: //BIGINT
			case 12: //SecondInterval
			case 9: //REAL
			case 10: //DOUBLE
			case 8: //DECIMAL
				try {
					// check (by calling parse) if the string represents a valid number to prevent
					// failing exec #(..., ...) calls which destroy the prepared statement, see bug 6351
					if (targetSqlType == 4) {
						int number = Byte.parseByte(x);
					} else if (targetSqlType == 5 ) {
						int number = Short.parseShort(x);
					} else if (targetSqlType == 6 || targetSqlType == 11) {
						int number = Integer.parseInt(x);
					} else if (targetSqlType == 7 || targetSqlType == 12) {
						long number = Long.parseLong(x);
					} else if (targetSqlType == 9 || targetSqlType == 10) {
						double number = Double.parseDouble(x);
					} else {
						BigDecimal number = new BigDecimal(x);
					}
				} catch (NumberFormatException nfe) {
					throw new MonetDBEmbeddedException("Conversion of string: " + x + " to parameter data type " + paramMonetdbType + " failed. " + nfe.getMessage());
				}
				setValue(parameterIndex, x);
				break;
			case 0:
				if  (x.equalsIgnoreCase("false") || x.equalsIgnoreCase("true") || x.equals("0") || x.equals("1")) {
					setValue(parameterIndex, x);
				} else {
					throw new MonetDBEmbeddedException("Conversion of string: " + x + " to parameter data type " + paramMonetdbType + " failed");
				}
				break;
			case 15: //Types.DATE:
			case 13: //Types.TIME:
			case 14: //TIMETZ:
			case 16: //Types.TIMESTAMP:
			case 17: //TIMESTAMPTZ:
				try {
					// check if the string represents a valid calendar date or time or timestamp to prevent
					// failing exec #(..., ...) calls which destroy the prepared statement, see bug 6351
					if (targetSqlType == 15) {
						java.sql.Date datum = java.sql.Date.valueOf(x);
					} else if (targetSqlType == 13 || targetSqlType == 14) {
						Time tijdstip = Time.valueOf(x);
					} else {
						Timestamp tijdstip = Timestamp.valueOf(x);
					}
				} catch (IllegalArgumentException iae) {
					throw new MonetDBEmbeddedException("Conversion of string: " + x + " to parameter data type " + paramMonetdbType + " failed. " + iae.getMessage());
				}
				/* prefix the string with: date or time or timetz or timestamp or timestamptz */
				setValue(parameterIndex, paramMonetdbType + " '" + x + "'");
				break;
			case 18: //Blob:
				// check if the string x contains pairs of hex chars to prevent
				// failing exec #(..., ...) calls which destroy the prepared statement, see bug 6351
				int xlen = x.length();
				for (int i = 0; i < xlen; i++) {
					char c = x.charAt(i);
					if (c < '0' || c > '9') {
						if (c < 'A' || c > 'F') {
							if (c < 'a' || c > 'f') {
								throw new MonetDBEmbeddedException("Invalid string for parameter data type " + paramMonetdbType + ". The string may contain only hex chars");
							}
						}
					}
				}
				/* prefix the string with: blob */
				setValue(parameterIndex, "blob '" + x + "'");
				break;
			default:
				throw new MonetDBEmbeddedException("Conversion of string to parameter data type " + paramMonetdbType + " is not (yet) supported");
		}
	}

	/**
	 * Sets the designated parameter to the given java.sql.Time value. The driver converts this to an SQL TIME value
	 * when it sends it to the database.
	 *
	 * @param index the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setTime(int index, Time x) throws MonetDBEmbeddedException {
		setTime(index, x, null);
	}

	/**
	 * Sets the designated parameter to the given java.sql.Time value,
	 * using the given Calendar object.  The driver uses the Calendar
	 * object to construct an SQL TIME value, which the driver then
	 * sends to the database.  With a Calendar object, the driver can
	 * calculate the time taking into account a custom timezone.  If no
	 * Calendar object is specified, the driver uses the default
	 * timezone, which is that of the virtual machine running the
	 * application.
	 *
	 * @param index the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @param cal the Calendar object the driver will use to construct the time
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setTime(int index, Time x, Calendar cal) throws MonetDBEmbeddedException {
		if (x == null) {
			setNull(index, -1);
			return;
		}

		String MonetDBType = monetdbType[getParamIdx(index)];
		boolean hasTimeZone = ("timetz".equals(MonetDBType) || "timestamptz".equals(MonetDBType));
		if (hasTimeZone) {
			// timezone shouldn't matter, since the server is timezone
			// aware in this case
			String RFC822 = mTimeZ.format(x);
			setValue(index, "timetz '" + RFC822.substring(0, 15) + ":" + RFC822.substring(15) + "'");
		} else {
			// server is not timezone aware for this field, and no calendar given, since we told the server our
			// timezone at connection creation, we can just write a plain timestamp here
			if (cal == null) {
				setValue(index, "time '" + x.toString() + "'");
			} else {
				mTime.setTimeZone(cal.getTimeZone());
				setValue(index, "time '" + mTime.format(x) + "'");
			}
		}
	}

	/**
	 * Sets the designated parameter to the given java.sql.Timestamp value. The driver converts this to an SQL TIMESTAMP
	 * value when it sends it to the database.
	 *
	 * @param index the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setTimestamp(int index, Timestamp x) throws MonetDBEmbeddedException {
		setTimestamp(index, x, null);
	}

	/**
	 * Sets the designated parameter to the given java.sql.Timestamp
	 * value, using the given Calendar object.  The driver uses the
	 * Calendar object to construct an SQL TIMESTAMP value, which the
	 * driver then sends to the database.  With a Calendar object, the
	 * driver can calculate the timestamp taking into account a custom
	 * timezone.  If no Calendar object is specified, the driver uses the
	 * default timezone, which is that of the virtual machine running
	 * the application.
	 *
	 * @param index the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @param cal the Calendar object the driver will use to construct the timestamp
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setTimestamp(int index, Timestamp x, Calendar cal) throws MonetDBEmbeddedException {
		if (x == null) {
			setNull(index, -1);
			return;
		}

		String MonetDBType = monetdbType[getParamIdx(index)];
		boolean hasTimeZone = ("timestamptz".equals(MonetDBType) || "timetz".equals(MonetDBType));
		if (hasTimeZone) {
			// timezone shouldn't matter, since the server is timezone
			// aware in this case
			String RFC822 = mTimestampZ.format(x);
			setValue(index, "timestamptz '" + RFC822.substring(0, 26) + ":" + RFC822.substring(26) + "'");
		} else {
			// server is not timezone aware for this field, and no
			// calendar given, since we told the server our timezone at
			// connection creation, we can just write a plain timestamp
			// here
			if (cal == null) {
				setValue(index, "timestamp '" + x.toString() + "'");
			} else {
				mTimestamp.setTimeZone(cal.getTimeZone());
				setValue(index, "timestamp '" + mTimestamp.format(x) + "'");
			}
		}
	}

	/**
	 * Sets the designated parameter to the given java.net.URL value. The driver converts this to an SQL DATALINK value
	 * when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the java.net.URL object to be set
	 * @throws MonetDBEmbeddedException if a database access error occurs
	 */
	public void setURL(int parameterIndex, URL x) throws MonetDBEmbeddedException {
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}
		String val = x.toString();
		setValue(parameterIndex, "url '" + val.replaceAll("\\\\", "\\\\\\\\")
				.replaceAll("'", "\\\\'") + "'");
	}

	/* helper for the anonymous class inside getMetaData */
	private abstract class ersmdw extends MonetWrapper implements ResultSetMetaData {}
	/**
	 * Retrieves a ResultSetMetaData object that contains information
	 * about the columns of the ResultSet object that will be returned
	 * when this PreparedStatement object is executed.
	 *
	 * Because a PreparedStatement object is precompiled, it is possible
	 * to know about the ResultSet object that it will return without
	 * having to execute it.  Consequently, it is possible to invoke the
	 * method getMetaData on a PreparedStatement object rather than
	 * waiting to execute it and then invoking the ResultSet.getMetaData
	 * method on the ResultSet object that is returned.
	 *
	 * @return the description of a ResultSet object's columns or null if the
	 *         driver cannot return a ResultSetMetaData object
	 */
	public ResultSetMetaData getMetaData() {
		if (rscolcnt == 3)
			return null; // not sufficient data with pre-Dec2011 PREPARE

		// return inner class which implements the ResultSetMetaData interface
		return new ersmdw() {
			/**
			 * Returns the number of columns in this ResultSet object.
			 *
			 * @return the number of columns
			 */
			@Override
			public int getColumnCount() {
				int cnt = 0;
				for (int i = 0; i < size; i++) {
					if (column[i] != null)
						cnt++;
				}
				return cnt;
			}

			/**
			 * Indicates whether the designated column is automatically numbered.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return true if so; false otherwise
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public boolean isAutoIncrement(int column) throws SQLException {
				return false;
			}

			/**
			 * Indicates whether a column's case matters.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return if the column is case sensitive
			 */
			@Override
			public boolean isCaseSensitive(int column) throws SQLException {
				switch (getColumnType(column)) {
					case 1: //Char
					case 2: //Varchar
					case 3: //Clob
						return true;
					default:
						return false;
				}
			}

			/**
			 * Indicates whether the designated column can be used in a where clause.
			 *
			 * Returning true for all here, even for CLOB, BLOB.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return true
			 */
			@Override
			public boolean isSearchable(int column) {
				return true;
			}

			/**
			 * Indicates whether the designated column is a cash value. From the MonetDB database perspective it is by
			 * definition unknown whether the value is a currency, because there are no currency datatypes such as
			 * MONEY. With this knowledge we can always return false here.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return false
			 */
			@Override
			public boolean isCurrency(int column) {
				return false;
			}

			/**
			 * Indicates whether values in the designated column are signed numbers. Within MonetDB all numeric types
			 * (except oid and ptr) are signed.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return true if so; false otherwise
			 */
			@Override
			public boolean isSigned(int column) throws SQLException {
				switch (getColumnType(column)) {
					case 4: //Tinyint
					case 5: //Smallint
					case 6: //Int
					case 7: //Bigint
					case 8: //Decimal
					case 9: //Real
					case 10: //Double
						return true;
					default:
						return false;
				}
			}

			/**
			 * Indicates the designated column's normal maximum width in characters.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return the normal maximum number of characters allowed as the width of the designated column
			 * @throws SQLException if there is no such column
			 */
			@Override
			public int getColumnDisplaySize(int column) throws SQLException {
				try {
					return digits[getColumnIdx(column)];
				} catch (IndexOutOfBoundsException e) {
					throw new SQLException(e);
				}
			}

			/**
			 * Get the designated column's table's schema.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return schema name or "" if not applicable
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public String getSchemaName(int column) throws SQLException {
				try {
					return schema[getColumnIdx(column)];
				} catch (IndexOutOfBoundsException e) {
					throw new SQLException(e);
				}
			}

			/**
			 * Gets the designated column's table name.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return table name or "" if not applicable
			 */
			@Override
			public String getTableName(int column) throws SQLException {
				try {
					return table[getColumnIdx(column)];
				} catch (IndexOutOfBoundsException e) {
					throw new SQLException(e);
				}
			}

			/**
			 * Get the designated column's number of decimal digits. This method is currently very expensive as it
			 * needs to retrieve the information from the database using an SQL query.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return precision
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int getPrecision(int column) throws SQLException {
				try {
					return digits[getColumnIdx(column)];
				} catch (IndexOutOfBoundsException e) {
					throw new SQLException(e);
				}
			}

			/**
			 * Gets the designated column's number of digits to right of the decimal point. This method is currently
			 * very expensive as it needs to retrieve the information from the database using an SQL query.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return scale
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int getScale(int column) throws SQLException {
				try {
					return scale[getColumnIdx(column)];
				} catch (IndexOutOfBoundsException e) {
					throw new SQLException(e);
				}
			}

			/**
			 * Indicates the nullability of values in the designated column. This method is currently very expensive as
			 * it needs to retrieve the information from the database using an SQL query.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return nullability
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int isNullable(int column) throws SQLException {
				return columnNullableUnknown;
			}

			/**
			 * Gets the designated column's table's catalog name.
			 * MonetDB does not support the catalog naming concept as in: catalog.schema.table naming scheme
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return the name of the catalog for the table in which the given column appears or "" if not applicable
			 */
			@Override
			public String getCatalogName(int column) throws SQLException {
				return null;	// MonetDB does NOT support catalogs
			}

			/**
			 * Indicates whether the designated column is definitely not writable.  MonetDB does not support cursor
			 * updates, so nothing is writable.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return true if so; false otherwise
			 */
			@Override
			public boolean isReadOnly(int column) {
				return true;
			}

			/**
			 * Indicates whether it is possible for a write on the designated column to succeed.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return true if so; false otherwise
			 */
			@Override
			public boolean isWritable(int column) {
				return false;
			}

			/**
			 * Indicates whether a write on the designated column will definitely succeed.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return true if so; false otherwise
			 */
			@Override
			public boolean isDefinitelyWritable(int column) {
				return false;
			}

			/**
			 * Returns the fully-qualified name of the Java class whose instances are manufactured if the method
			 * ResultSet.getObject is called to retrieve a value from the column.  ResultSet.getObject may return a
			 * subclass of the class returned by this method.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return the fully-qualified name of the class in the Java programming language that would be used by the
			 * method ResultSet.getObject to retrieve the value in the specified column. This is the class name used
			 * for custom mapping.
			 * @throws SQLException if there is no such column
			 */
			@Override
			public String getColumnClassName(int column) throws SQLException {
				try {
					return MonetDBToJavaMapping.getJavaMappingFromMonetDBString(monetdbType[getColumnIdx(column)])
							.getJavaClass().getName();
				} catch (IndexOutOfBoundsException e) {
					throw new SQLException(e);
				}
			}

			/**
			 * Gets the designated column's suggested title for use in printouts and displays. This is currently equal
			 * to getColumnName().
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return the suggested column title
			 * @throws SQLException if there is no such column
			 */
			@Override
			public String getColumnLabel(int column) throws SQLException {
				return getColumnName(column);
			}

			/**
			 * Gets the designated column's name
			 *
			 * @param colnr the first column is 1, the second is 2, ...
			 * @return the column name
			 * @throws SQLException if there is no such column
			 */
			@Override
			public String getColumnName(int colnr) throws SQLException {
				try {
					return column[getColumnIdx(colnr)];
				} catch (IndexOutOfBoundsException e) {
					throw new SQLException(e);
				}
			}

			/**
			 * Retrieves the designated parameter's MonetDBToJavaMapping value.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return The ordinal mapping from MonetDBToJavaMapping
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int getColumnType(int column) throws SQLException {
				try {
					return MonetDBToJavaMapping
							.getJavaMappingFromMonetDBStringOrdinalValue(monetdbType[getColumnIdx(column)]);
				} catch (IndexOutOfBoundsException e) {
					throw new SQLException(e);
				}
			}

			/**
			 * Retrieves the designated column's database-specific type name.
			 *
			 * @param column the first column is 1, the second is 2, ...
			 * @return type name used by the database. If the column type is a user-defined type, then a
			 * fully-qualified type name is returned.
			 * @throws SQLException if there is no such column
			 */
			@Override
			public String getColumnTypeName(int column) throws SQLException {
				try {
					return monetdbType[getColumnIdx(column)];
				} catch (IndexOutOfBoundsException e) {
					throw new SQLException(e);
				}
			}
		};
	}

	/* helper class for the anonymous class in getParameterMetaData */
	private abstract class epmdw extends MonetWrapper implements ParameterMetaData {}
	/**
	 * Retrieves the number, types and properties of this PreparedStatement object's parameters.
	 *
	 * @return a ParameterMetaData object that contains information about the number, types and properties of this
	 * PreparedStatement object's parameters
	 * @throws SQLException if a database access error occurs
	 */
	public ParameterMetaData getParameterMetaData() throws SQLException {
		return new epmdw() {
			/**
			 * Retrieves the number of parameters in the PreparedStatement object for which this ParameterMetaData
			 * object contains information.
			 *
			 * @return the number of parameters
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int getParameterCount() throws SQLException {
				int cnt = 0;

				for (int i = 0; i < size; i++) {
					if (column[i] == null)
						cnt++;
				}

				return cnt;
			}

			/**
			 * Retrieves whether null values are allowed in the designated parameter.
			 *
			 * This is currently always unknown for MonetDB/SQL.
			 *
			 * @param param the first parameter is 1, the second is 2, ...
			 * @return the nullability status of the given parameter; one of ParameterMetaData.parameterNoNulls,
			 * ParameterMetaData.parameterNullable, or ParameterMetaData.parameterNullableUnknown
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int isNullable(int param) throws SQLException {
				return ParameterMetaData.parameterNullableUnknown;
			}

			/**
			 * Retrieves whether values for the designated parameter can be signed numbers.
			 *
			 * @param param the first parameter is 1, the second is 2, ...
			 * @return true if so; false otherwise
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public boolean isSigned(int param) throws SQLException {
				// we can hardcode this, based on the column type
				switch (getParameterType(param)) {
					case 4: //Tinyint
					case 5: //Smallint
					case 6: //Int
					case 7: //Bigint
					case 8: //Decimal
					case 9: //Real
					case 10: //Double
						return true;
					default:
						return false;
				}
			}

			/**
			 * Retrieves the designated parameter's number of decimal digits.
			 *
			 * @param param the first parameter is 1, the second is 2, ...
			 * @return precision
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int getPrecision(int param) throws SQLException {
				try {
					return digits[getParamIdx(param)];
				} catch (IndexOutOfBoundsException e) {
					throw new SQLException(e);
				}
			}

			/**
			 * Retrieves the designated parameter's number of digits to right of the decimal point.
			 *
			 * @param param the first parameter is 1, the second is 2, ...
			 * @return scale
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int getScale(int param) throws SQLException {
				try {
					return scale[getParamIdx(param)];
				} catch (IndexOutOfBoundsException e) {
					throw new SQLException(e);
				}
			}

			/**
			 * Retrieves the designated parameter's MonetDBToJavaMapping value.
			 *
			 * @param param the first parameter is 1, the second is 2, ...
			 * @return The ordinal mapping from MonetDBToJavaMapping
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int getParameterType(int param) throws SQLException {
				try {
					return MonetDBToJavaMapping
							.getJavaMappingFromMonetDBStringOrdinalValue(monetdbType[getParamIdx(param)]);
				} catch (IndexOutOfBoundsException e) {
					throw new SQLException(e);
				}
			}

			/**
			 * Retrieves the designated parameter's database-specific type name.
			 *
			 * @param param the first parameter is 1, the second is 2, ...
			 * @return type the name used by the database.  If the parameter type is a user-defined type, then a
			 *         fully-qualified type name is returned.
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public String getParameterTypeName(int param) throws SQLException {
				try {
					return monetdbType[getParamIdx(param)];
				} catch (IndexOutOfBoundsException e) {
					throw new SQLException(e);
				}
			}

			/**
			 * Retrieves the fully-qualified name of the Java class whose instances should be passed to the method
			 * PreparedStatement.setObject.
			 *
			 * @param param the first parameter is 1, the second is 2, ...
			 * @return the fully-qualified name of the class in the Java programming language that would be used by the
			 *         method PreparedStatement.setObject to set the value in the specified parameter. This is the
			 *         class name used for custom mapping.
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public String getParameterClassName(int param) throws SQLException {
				try {
					return MonetDBToJavaMapping.getJavaMappingFromMonetDBString(monetdbType[getColumnIdx(param)])
							.getJavaClass().getName();
				} catch (IndexOutOfBoundsException e) {
					throw new SQLException(e);
				}
			}

			/**
			 * Retrieves the designated parameter's mode. For MonetDB/SQL this is currently always unknown.
			 *
			 * @param param - the first parameter is 1, the second is 2, ...
			 * @return mode of the parameter; one of ParameterMetaData.parameterModeIn,
			 *         ParameterMetaData.parameterModeOut, or ParameterMetaData.parameterModeInOut
			 *         ParameterMetaData.parameterModeUnknown.
			 * @throws SQLException if a database access error occurs
			 */
			@Override
			public int getParameterMode(int param) throws SQLException {
				return ParameterMetaData.parameterModeUnknown;
			}
		};
	}

	/**
	 * Release the prepared statement in the server!!!
	 */
	private native void freePreparedStatement(long connectionPointer, int preparedResultSetID)
			throws MonetDBEmbeddedException;

	@Override
	public void close() {
		super.close();
		this.id = 0;
	}

	@Override
	protected void closeResultImplementation() {
		if(!this.isPreparedStatementClosed()) {
			try {
				this.freePreparedStatement(this.getConnection().connectionPointer, this.id);
			} catch (MonetDBEmbeddedException ex) { }
			this.id = 0;
		}
	}
}
