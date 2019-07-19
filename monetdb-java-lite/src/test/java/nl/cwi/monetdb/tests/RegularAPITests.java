/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

package nl.cwi.monetdb.tests;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedConnection;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedDatabase;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedPreparedStatement;
import nl.cwi.monetdb.embedded.mapping.MonetDBRow;
import nl.cwi.monetdb.embedded.mapping.NullMappings;
import nl.cwi.monetdb.embedded.resultset.QueryResultSet;
import nl.cwi.monetdb.embedded.tables.IMonetDBTableCursor;
import nl.cwi.monetdb.embedded.tables.MonetDBTable;
import nl.cwi.monetdb.embedded.tables.RowIterator;
import nl.cwi.monetdb.tests.helpers.ForkJavaProcess;
import nl.cwi.monetdb.tests.helpers.MonetDBJavaLiteTesting;
import nl.cwi.monetdb.tests.helpers.TryStartMonetDBEmbeddedDatabase;
import org.junit.jupiter.api.*;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Test the regular API. Just that :)
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class RegularAPITests extends MonetDBJavaLiteTesting {

	private static MonetDBEmbeddedConnection connection;

	@BeforeAll
	@DisplayName("Startup connection")
	static void startupDatabase() throws MonetDBEmbeddedException {
		MonetDBJavaLiteTesting.startupDatabase(MonetDBJavaLiteTesting.getDirectoryPath().toString());
		connection = MonetDBEmbeddedDatabase.createConnection();
	}

	@Test
	@DisplayName("General assertions from the Database environment")
	void databaseEnv() throws MonetDBEmbeddedException {
		//Cannot have two MonetDBEmbeddedDatabase instances
		try {
			MonetDBEmbeddedDatabase.startDatabase("/", true, false);
			Assertions.fail("The MonetDBEmbeddedException should be thrown");
		} catch (MonetDBEmbeddedException ignored) {
			//I was getting unexpected results with this inside of a Assertions.assertThrows, so I made this...
		}

		Assertions.assertTrue(MonetDBEmbeddedDatabase::isDatabaseRunning);
		boolean inMemory = MonetDBEmbeddedDatabase.isDatabaseRunningInMemory();
		boolean silentFlagSet = MonetDBEmbeddedDatabase.isSilentFlagSet();
		boolean sequentialFlagSet = MonetDBEmbeddedDatabase.isSequentialFlagSet();
		Assertions.assertFalse(inMemory);
		Assertions.assertTrue(silentFlagSet);
		Assertions.assertFalse(sequentialFlagSet);

		String thisPath = MonetDBJavaLiteTesting.getDirectoryPath().toString();
		String dbPath = MonetDBEmbeddedDatabase.getDatabaseDirectory();
		Assertions.assertEquals(thisPath, dbPath, "The database is running on a different directory?");

		MonetDBEmbeddedConnection con1 = MonetDBEmbeddedDatabase.createConnection();

		QueryResultSet rs = con1.executeQuery("SELECT 1;");
		//A query result set cannot do any further statements after is closed
		rs.close();
		Assertions.assertThrows(MonetDBEmbeddedException.class, () -> rs.getIntegerByColumnIndexAndRow(1, 1));
		Assertions.assertTrue(rs::isQueryResultSetClosed);

		//The same happens for the connection
		con1.close();
		Assertions.assertThrows(MonetDBEmbeddedException.class, () -> con1.executeQuery("SELECT 1;"));
		Assertions.assertTrue(con1::isClosed);
	}

	@Test
	@DisplayName("Just another race of MonetDBEmbeddedConnections")
	void oneMoreRace() throws InterruptedException {
		int stress = 16;
		Thread[] otherStressers = new Thread[stress];
		String[] messages = new String[stress];

		for (int i = 0; i < stress; i++) {
			final int threadID = i;
			final int j = i;
			Thread t = new Thread(() -> {
				MonetDBEmbeddedConnection con = null;
				QueryResultSet qrs = null;
				try {
					con = MonetDBEmbeddedDatabase.createConnection();
					qrs = con.executeQuery("SELECT " + threadID);
					int intt = qrs.getIntegerByColumnIndexAndRow(1, 1);
					if(threadID != intt) {
						messages[j] = "No response from the server in one of the Threads";
					}
				} catch (MonetDBEmbeddedException e) {
					messages[j] = e.getMessage();
				}
				if(qrs != null) {
					qrs.close();
				}
				if(con != null) {
					con.close();
				}
			});
			t.start();
			otherStressers[j] = t;
		}

		for (Thread t : otherStressers) {
			t.join();
		}
		for(String ss : messages) {
			if(ss != null) {
				Assertions.fail(ss);
			}
		}
	}

	@Test
	@DisplayName("Empty result sets")
	void testEmptyResulSets() throws MonetDBEmbeddedException {
		QueryResultSet qrs = connection.executeQuery("SELECT id from types WHERE 1=0;");
		Assertions.assertThrows(ArrayIndexOutOfBoundsException.class, () -> qrs.getIntegerByColumnIndexAndRow(1, 1));
		int numberOfRows = qrs.getNumberOfRows();
		Assertions.assertEquals(0, numberOfRows, "The number of rows should be 0, got " + numberOfRows + " instead");
		qrs.close();
	}

	@Test
	@DisplayName("SELECT NULL")
	void selectNull() throws MonetDBEmbeddedException {
		QueryResultSet qrs = connection.executeQuery("SELECT NULL AS stresser;");
		int numberOfRows = qrs.getNumberOfRows(), numberOfColumns = qrs.getNumberOfColumns();
		Assertions.assertEquals(1, numberOfRows, "The number of rows should be 1, got " + numberOfRows + " instead");
		Assertions.assertEquals(1, numberOfColumns, "The number of columns should be 1, got " + numberOfColumns + " instead");
		qrs.close();
	}

	@Test
	@DisplayName("Testing single values retrieved from a query")
	void testSingleValues() throws MonetDBEmbeddedException {
		QueryResultSet qrs = connection.executeQuery("SELECT 1+1 AS a1, 'monetdb' AS a2, AVG(2.3) as a3;");
		int numberOfRows = qrs.getNumberOfRows(), numberOfColumns = qrs.getNumberOfColumns();
		Assertions.assertEquals(1, numberOfRows, "The number of rows should be 1, got " + numberOfRows + " instead");
		Assertions.assertEquals(3, numberOfColumns, "The number of columns should be 3, got " + numberOfColumns + " instead");

		String[] columnNames = new String[numberOfColumns];
		qrs.getColumnNames(columnNames);
		Assertions.assertArrayEquals(new String[]{"a1", "a2", "a3"}, columnNames, "Column names not correctly retrieved");

		byte sum = qrs.getByteByColumnIndexAndRow(1,1);
		String monetDB = qrs.getStringByColumnNameAndRow("a2",1);
		double avg = qrs.getDoubleByColumnIndexAndRow(3, 1);

		Assertions.assertEquals(2, sum, "The sum is not 2?");
		Assertions.assertEquals("monetdb", monetDB, "The sum is not 2?");
		Assertions.assertEquals(2.3, avg, 0.01, "The avg is not right?");
		qrs.close();
		Assertions.assertTrue(qrs::isQueryResultSetClosed, "The query result set should be closed");
	}

	@Test
	@DisplayName("Test the autocommit mode")
	void testAutoCommit() throws MonetDBEmbeddedException {
		Assertions.assertTrue(connection.getAutoCommit(), "The default autocommit mode should be true");
		connection.setAutoCommit(false);
		Assertions.assertFalse(connection.getAutoCommit(), "The autocommit mode should be false");
		connection.setAutoCommit(true);
		Assertions.assertTrue(connection.getAutoCommit(), "The autocommit mode should be true");

		connection.startTransaction();
		connection.executeUpdate("CREATE TABLE testcomm (a int);");
		connection.executeUpdate("INSERT INTO testcomm VALUES (1);");
		connection.commit();

		connection.startTransaction();
		connection.executeUpdate("INSERT INTO testcomm VALUES (2);");
		connection.rollback();

		connection.setAutoCommit(false);
		QueryResultSet qrs = connection.executeQuery("SELECT count(*) FROM testcomm;");
		int numberOfRows = qrs.getNumberOfRows();
		Assertions.assertEquals(1, numberOfRows, "The number of rows should be 1, got " + numberOfRows + " instead");
		int retrivedValue = qrs.getIntegerByColumnIndexAndRow(1, 1);
		Assertions.assertEquals(1, retrivedValue, "The transaction didn't go well");
		qrs.close();

		connection.executeUpdate("DROP TABLE testcomm;");
		connection.setAutoCommit(true);
	}

	@Test
	@DisplayName("Test savepoints")
	void testSavepoints() throws MonetDBEmbeddedException {
		connection.setAutoCommit(false);

		connection.setSavepoint();

		connection.executeUpdate("CREATE TABLE testsave (id int);");
		Savepoint svp1 = connection.setSavepoint("emptytable");

		QueryResultSet qrs1 = connection.executeQuery("SELECT id FROM testsave;");
		Assertions.assertEquals(0, qrs1.getNumberOfRows(), "The number of rows should be 0");
		qrs1.close();

		connection.executeUpdate("INSERT INTO testsave VALUES (1), (2), (3);");
		Savepoint svp2 = connection.setSavepoint("threevalues");

		QueryResultSet qrs2 = connection.executeQuery("SELECT id FROM testsave;");
		Assertions.assertEquals(3, qrs2.getNumberOfRows(), "The number of rows should be 3");
		qrs2.close();

		connection.releaseSavepoint(svp2);

		QueryResultSet qrs3 = connection.executeQuery("SELECT id FROM testsave;");
		Assertions.assertEquals(3, qrs3.getNumberOfRows(), "The number of rows should be 3");
		qrs3.close();

		connection.rollback(svp1);

		QueryResultSet qrs4 = connection.executeQuery("SELECT id FROM testsave;");
		Assertions.assertEquals(0, qrs4.getNumberOfRows(), "The number of rows should be 0");
		qrs4.close();

		try {
			connection.releaseSavepoint(svp2);
			Assertions.fail("The MonetDBEmbeddedException should be thrown");
		} catch (MonetDBEmbeddedException ex) {
			connection.rollback();
		}

		try {
			connection.rollback(svp2);
			Assertions.fail("The MonetDBEmbeddedException should be thrown");
		} catch (MonetDBEmbeddedException ex) {
			connection.rollback();
		}

		connection.setAutoCommit(true);
	}

	@Test
	@DisplayName("Retrieve the most common types from a query into arrays (Also testing foreign characters)")
	void testBasicTypes() throws MonetDBEmbeddedException {
		connection.executeUpdate("CREATE TABLE testbasics (a boolean, b text, c tinyint, d smallint, e int, f bigint, h real, i double, j oid);");
		connection.executeUpdate("INSERT INTO testbasics VALUES ('true', 'a1ñ212#da ', 1, 1, 1, 1, 1.22, 1.33, 1);");
		connection.executeUpdate("INSERT INTO testbasics VALUES ('false', 'another with spaces', -2, -2, -2, -2, -1.59, -1.69, 2);");
		connection.executeUpdate("INSERT INTO testbasics VALUES ('true', '0', 0, 0, 0, 0, 0, 0, 0);");

		QueryResultSet qrs = connection.executeQuery("SELECT * FROM testbasics;");
		int numberOfRows = qrs.getNumberOfRows(), numberOfColumns = qrs.getNumberOfColumns();
		Assertions.assertEquals(3, numberOfRows, "The number of rows should be 3, got " + numberOfRows + " instead");
		Assertions.assertEquals(9, numberOfColumns, "The number of columns should be 9, got " + numberOfColumns + " instead");

		boolean[] array1 = new boolean[3];
		qrs.getBooleanColumnByIndex(1, array1);
		Assertions.assertArrayEquals(new boolean[]{true, false, true}, array1, "Booleans not correctly retrieved");

		String[] array2 = new String[3];
		qrs.getStringColumnByIndex(2, array2);
		Assertions.assertArrayEquals(new String[]{"a1ñ212#da ", "another with spaces", "0"}, array2, "Strings not correctly retrieved");

		byte[] array3 = new byte[3];
		qrs.getByteColumnByIndex(3, array3);
		Assertions.assertArrayEquals(new byte[]{1, -2, 0}, array3, "Tinyints not correctly retrieved");

		short[] array4 = new short[3];
		qrs.getShortColumnByIndex(4, array4);
		Assertions.assertArrayEquals(new short[]{1, -2, 0}, array4, "Smallints not correctly retrieved");

		int[] array5 = new int[3];
		qrs.getIntColumnByIndex(5, array5);
		Assertions.assertArrayEquals(new int[]{1, -2, 0}, array5, "Integers not correctly retrieved");

		long[] array6 = new long[3];
		qrs.getLongColumnByIndex(6, array6);
		Assertions.assertArrayEquals(new long[]{1, -2, 0}, array6, "Integers not correctly retrieved");

		float[] array7 = new float[3];
		qrs.getFloatColumnByIndex(7, array7);
		Assertions.assertArrayEquals(new float[]{1.22f, -1.59f, 0}, array7, 0.1f, "Floats not correctly retrieved");

		double[] array8 = new double[3];
		qrs.getDoubleColumnByIndex(8, array8);
		Assertions.assertArrayEquals(new double[]{1.33d, -1.69d, 0}, array8, 0.1d, "Doubles not correctly retrieved");

		String[] array9 = new String[3];
		qrs.getOidColumnByIndex(9, array9);
		Assertions.assertArrayEquals(new String[]{"1@0", "2@0", "0@0"}, array9, "Oids not correctly retrieved");

		qrs.close();
		connection.executeUpdate("DROP TABLE testbasics;");
	}

	@Test
	@Disabled("Who is brave enough to deal with timezones?")
	@DisplayName("Retrieve dates from a query into arrays")
	void testTimeAndDatesTypes() throws MonetDBEmbeddedException, ParseException {

		SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		SimpleDateFormat timeFormater = new SimpleDateFormat("HH:mm:ss", Locale.ENGLISH);
		SimpleDateFormat timestampFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);

		connection.executeUpdate("CREATE TABLE testdates (a date, b time, c time with time zone, d timestamp, e timestamp with time zone, f INTERVAL year to month, g INTERVAL minute to second);");
		connection.executeUpdate("INSERT INTO testdates VALUES ('2016-01-01', '23:10:47', '20:10:47', '2016-01-31T00:01:44', '1986-12-31T12:10:12', 1, 1);");
		connection.executeUpdate("INSERT INTO testdates VALUES ('1998-10-27', '0:10:47', '21:10:47', '1971-11-15T22:10:45', '1972-02-11T00:59:59', -10, -3000);");
		connection.executeUpdate("INSERT INTO testdates VALUES ('2014-12-02', '10:10:47', '11:10:47', '2016-03-04T08:30:30', '2016-03-04T09:00:01', 1023, 12);");
		connection.executeUpdate("INSERT INTO testdates VALUES ('1950-02-12', '20:10:47', '2:10:47', '1992-02-19T00:00:00', '1978-12-07T10:42:31', 0, 0);");

		QueryResultSet qrs = connection.executeQuery("SELECT * FROM testdates;");
		int numberOfRows = qrs.getNumberOfRows(), numberOfColumns = qrs.getNumberOfColumns();
		Assertions.assertEquals(4, numberOfRows, "The number of rows should be 4, got " + numberOfRows + " instead");
		Assertions.assertEquals(7, numberOfColumns, "The number of columns should be 7, got " + numberOfColumns + " instead");

		Date[] array1 = new Date[4];
		qrs.getDateColumnByIndex(1, array1);
		Assertions.assertArrayEquals(new Date[]{
				new Date(dateFormater.parse("2016-01-01").getTime()),
				new Date(dateFormater.parse("1998-10-27").getTime()),
				new Date(dateFormater.parse("2014-12-02").getTime()),
				new Date(dateFormater.parse("1950-02-12").getTime())
		}, array1, "Dates not correctly retrieved");

		Time[] array2 = new Time[4];
		qrs.getTimeColumnByIndex(2, array2);
		Assertions.assertArrayEquals(new Time[]{
				new Time(timeFormater.parse("23:10:47").getTime()),
				new Time(timeFormater.parse("0:10:47").getTime()),
				new Time(timeFormater.parse("10:10:47").getTime()),
				new Time(timeFormater.parse("20:10:47").getTime())
		}, array2, "Times not correctly retrieved");

		Time[] array3 = new Time[4];
		qrs.getTimeColumnByIndex(3, array3);
		Assertions.assertArrayEquals(new Time[]{
				new Time(timeFormater.parse("20:10:47").getTime()),
				new Time(timeFormater.parse("21:10:47").getTime()),
				new Time(timeFormater.parse("11:10:47").getTime()),
				new Time(timeFormater.parse("2:10:47").getTime())
		}, array3, "Times with timezones not correctly retrieved");

		Timestamp[] array4 = new Timestamp[4];
		qrs.getTimestampColumnByIndex(4, array4);
		Assertions.assertArrayEquals(new Timestamp[]{
				new Timestamp(timestampFormater.parse("2016-01-31 00:01:44").getTime()),
				new Timestamp(timestampFormater.parse("1971-11-15 22:10:45").getTime()),
				new Timestamp(timestampFormater.parse("2016-03-04 08:30:30").getTime()),
				new Timestamp(timestampFormater.parse("1992-02-19 00:00:00").getTime())
		}, array4, "Timestamps not correctly retrieved");

		Timestamp[] array5 = new Timestamp[4];
		qrs.getTimestampColumnByIndex(5, array5);
		Assertions.assertArrayEquals(new Timestamp[]{
				new Timestamp(timestampFormater.parse("1986-12-31 12:10:12").getTime()),
				new Timestamp(timestampFormater.parse("1972-02-11 00:59:59").getTime()),
				new Timestamp(timestampFormater.parse("2016-03-04 09:00:01").getTime()),
				new Timestamp(timestampFormater.parse("1978-12-07 10:42:31").getTime())
		}, array5, "Timestamps with timezone not correctly retrieved");

		int[] array6 = new int[4];
		qrs.getIntColumnByIndex(6, array6);
		Assertions.assertArrayEquals(new int[]{1, -10, 1023, 0}, array6, "Month intervals not correctly retrieved");

		long[] array7 = new long[4];
		qrs.getLongColumnByIndex(7, array7);
		Assertions.assertArrayEquals(new long[]{1000, -3000000, 12000, 0}, array7, "Second intervals not correctly retrieved");

		qrs.close();
		connection.executeUpdate("DROP TABLE testdates;");
	}

	@Test
	@DisplayName("Retrieve decimals from a query into arrays")
	void testDecimals() throws MonetDBEmbeddedException {
		connection.executeUpdate("CREATE TABLE testDecimals (a decimal);");
		connection.executeUpdate("INSERT INTO testDecimals VALUES (1.6), (12), (-1.42), (525636.777), (-41242.32), (0);");

		QueryResultSet qrs = connection.executeQuery("SELECT * FROM testDecimals;");
		int numberOfRows = qrs.getNumberOfRows(), numberOfColumns = qrs.getNumberOfColumns();
		Assertions.assertEquals(6, numberOfRows, "The number of rows should be 6, got " + numberOfRows + " instead");
		Assertions.assertEquals(1, numberOfColumns, "The number of columns should be 1, got " + numberOfColumns + " instead");

		//Having default precision of 3
		BigDecimal[] array1 = new BigDecimal[6];
				qrs.getDecimalColumnByIndex(1, array1);
		Assertions.assertArrayEquals(new BigDecimal[]{
				new BigDecimal("1.600"), new BigDecimal("12.000"), new BigDecimal("-1.420"),
				new BigDecimal("525636.777"), new BigDecimal("-41242.320"), new BigDecimal("0.000")
		}, array1, "BigDecimals not correctly retrieved");

		qrs.close();
		connection.executeUpdate("DROP TABLE testDecimals;");
	}

	@Test
	@DisplayName("Retrieve blobs from a query into arrays")
	void testBlobs() throws MonetDBEmbeddedException {
		connection.executeUpdate("CREATE TABLE testblobs (a blob);");
		connection.executeUpdate("INSERT INTO testblobs VALUES ('aabbcc');");

		QueryResultSet qrs = connection.executeQuery("SELECT * FROM testblobs;");
		int numberOfRows = qrs.getNumberOfRows(), numberOfColumns = qrs.getNumberOfColumns();
		Assertions.assertEquals(1, numberOfRows, "The number of rows should be 1, got " + numberOfRows + " instead");
		Assertions.assertEquals(1, numberOfColumns, "The number of columns should be 1, got " + numberOfColumns + " instead");

		byte[][] array1 = new byte[1][];
		qrs.getBlobColumnByIndex(1, array1);
		Assertions.assertArrayEquals(new byte[][]{
				new byte[]{-86, -69, -52}
		}, array1, "Blobs not correctly retrieved");

		qrs.close();
		connection.executeUpdate("DROP TABLE testblobs;");
	}

	@Test
	@DisplayName("Test if the null values are correctly retrieved")
	void testNulls() throws MonetDBEmbeddedException {
		connection.executeUpdate("CREATE TABLE testnullsa (a boolean, b text, c tinyint, d smallint, e int, f bigint, h real, i double, j oid);");
		connection.executeUpdate("INSERT INTO testnullsa VALUES (null, null, null, null, null, null, null, null, null);");

		QueryResultSet qrs = connection.executeQuery("SELECT * FROM testnullsa;");

		boolean first = qrs.checkBooleanIsNull(1,1);
		Assertions.assertEquals(true, first, "Boolean nulls not working");

		String second = qrs.getStringByColumnIndexAndRow(2,1);
		Assertions.assertEquals(null, second, "String nulls not working");

		byte third = qrs.getByteByColumnIndexAndRow(3,1);
		Assertions.assertEquals(NullMappings.getByteNullConstant(), third, "Byte nulls not working");

		short fourth = qrs.getShortByColumnIndexAndRow(4, 1);
		Assertions.assertEquals(NullMappings.getShortNullConstant(), fourth, "Short nulls not working");

		int fifth = qrs.getIntegerByColumnIndexAndRow(5, 1);
		Assertions.assertEquals(NullMappings.getIntNullConstant(), fifth, "Integer nulls not working");

		long sixth = qrs.getLongByColumnIndexAndRow(6, 1);
		Assertions.assertEquals(NullMappings.getLongNullConstant(), sixth, "Long nulls not working");

		float seventh = qrs.getFloatByColumnIndexAndRow(7, 1);
		Assertions.assertEquals(NullMappings.getFloatNullConstant(), seventh, "Float nulls not working");

		double eighth = qrs.getDoubleByColumnIndexAndRow(8, 1);
		Assertions.assertEquals(NullMappings.getDoubleNullConstant(), eighth, "Double nulls not working");

		String nineteenth = qrs.getOidByColumnIndexAndRow(9,1);
		Assertions.assertEquals(null, nineteenth, "Oid nulls not working");

		qrs.close();
		connection.executeUpdate("DROP TABLE testnullsa;");

		connection.executeUpdate("CREATE TABLE testnullsb (a date, b time, c time with time zone, d timestamp, e timestamp with time zone, f INTERVAL year to month, g INTERVAL minute to second);");
		connection.executeUpdate("INSERT INTO testnullsb VALUES (null, null, null, null, null, null, null);");

		QueryResultSet qrs2 = connection.executeQuery("SELECT * FROM testnullsb;");

		Date nineth = qrs2.getDateByColumnIndexAndRow(1, 1);
		Assertions.assertEquals(null, nineth, "Date nulls not working");

		Time tenth = qrs2.getTimeByColumnIndexAndRow(2, 1);
		Assertions.assertEquals(null, tenth, "Time nulls not working");

		Time eleventh = qrs2.getTimeByColumnIndexAndRow(3, 1);
		Assertions.assertEquals(null, eleventh, "Time nulls not working");

		Timestamp twelfth = qrs2.getTimestampByColumnIndexAndRow(4, 1);
		Assertions.assertEquals(null, twelfth, "Timestamp nulls not working");

		Timestamp thirteenth = qrs2.getTimestampByColumnIndexAndRow(5, 1);
		Assertions.assertEquals(null, thirteenth, "Timestamp nulls not working");

		int fourteenth = qrs2.getIntegerByColumnIndexAndRow(6, 1);
		Assertions.assertEquals(NullMappings.getIntNullConstant(), fourteenth, "Month interval nulls not working");

		long fifteenth = qrs2.getLongByColumnIndexAndRow(7, 1);
		Assertions.assertEquals(NullMappings.getLongNullConstant(), fifteenth, "Second interval nulls not working");

		qrs2.close();
		connection.executeUpdate("DROP TABLE testnullsb;");

		connection.executeUpdate("CREATE TABLE testnullsc (a blob, b decimal);");
		connection.executeUpdate("INSERT INTO testnullsc VALUES (null, null), (null, 1), ('aa', null), ('bb', 3.0);");

		QueryResultSet qrs3 = connection.executeQuery("SELECT * FROM testnullsc;");

		byte[] sixteenth = qrs3.getBlobByColumnIndexAndRow(1, 1);
		Assertions.assertEquals(null, sixteenth, "Blob nulls not working");

		BigDecimal seventeenth = qrs3.getDecimalByColumnIndexAndRow(2, 1);
		Assertions.assertEquals(null, seventeenth, "Decimal nulls not working");

		int numberOfRows = qrs3.getNumberOfRows();
		Assertions.assertEquals(4, numberOfRows, "The number of rows should be 4, got " + numberOfRows + " instead");

		boolean[] eighteenth = new boolean[numberOfRows];
		qrs3.getColumnNullMappingsByIndex(1, eighteenth);
		Assertions.assertArrayEquals(new boolean[]{true, true, false, false}, eighteenth, "Null mapping problem");
		qrs3.getColumnNullMappingsByIndex(2, eighteenth);
		Assertions.assertArrayEquals(new boolean[]{true, false, true, false}, eighteenth, "Null mapping problem");

		qrs3.close();
		connection.executeUpdate("DROP TABLE testnullsc;");
	}

	@Test
	@DisplayName("Test query result set rows iteration")
	void testQueryResultSet() throws MonetDBEmbeddedException {
		connection.executeUpdate("CREATE TABLE testresultsets (a text, b int, c real);");
		connection.executeUpdate("INSERT INTO testresultsets VALUES ('I', 12, 234.534);");
		connection.executeUpdate("INSERT INTO testresultsets VALUES ('like', -13, 43.123);");
		connection.executeUpdate("INSERT INTO testresultsets VALUES ('Java', 123, -34.43);");

		QueryResultSet qrs1 = connection.executeQuery("SELECT * FROM testresultsets WHERE 1=0;");
		ListIterator<MonetDBRow> iterator1 = qrs1.iterator();
		Assertions.assertNotNull(iterator1, "The iterator should not be null");
		Assertions.assertFalse(iterator1.hasNext(), "The iterator should have no rows");
		qrs1.close();

		QueryResultSet qrs2 = connection.executeQuery("SELECT * FROM testresultsets WHERE a='I';");
		ListIterator<MonetDBRow> iterator2 = qrs2.iterator();
		Assertions.assertNotNull(iterator2, "The iterator should not be null");
		Assertions.assertTrue(iterator2.hasNext(), "The iterator should have 1 row left");

		MonetDBRow row1 = iterator2.next();
		Assertions.assertFalse(iterator2.hasNext(), "The iterator should have no rows");

		Assertions.assertEquals(row1.getColumnByIndex(1), "I", "Strings not correctly retrieved");
		Assertions.assertEquals(row1.getColumnByIndex(2), new Integer(12), "Integers not correctly retrieved");
		Assertions.assertEquals(row1.getColumnByIndex(3), new Float(234.534f), "Floats not correctly retrieved");

		ListIterator<Object> cols  = row1.iterator();
		Assertions.assertTrue(cols.hasNext(), "The iterator should have 3 rows left");
		Assertions.assertEquals(cols.next(), "I", "Strings not correctly retrieved");
		Assertions.assertTrue(cols.hasNext(), "The iterator should have 2 rows left");
		Assertions.assertEquals(cols.next(), 12, "Integers not correctly retrieved");
		Assertions.assertTrue(cols.hasNext(), "The iterator should have 1 rows left");
		Assertions.assertEquals(cols.next(), 234.534f, "Floats not correctly retrieved");
		Assertions.assertFalse(cols.hasNext(), "The iterator should have no rows");

		qrs2.close();

		connection.executeUpdate("DROP TABLE testresultsets;");
	}

	@Test
	@DisplayName("Test the table iteration")
	void testIterateTable() throws MonetDBEmbeddedException {
		connection.executeUpdate("CREATE TABLE test4 (a text, b int);");
		connection.executeUpdate("INSERT INTO test4 VALUES ('one', -40);");
		connection.executeUpdate("INSERT INTO test4 VALUES ('more', 5);");
		connection.executeUpdate("INSERT INTO test4 VALUES ('test', 37);");

		MonetDBTable test4 = connection.getMonetDBTable("sys", "test4");

		test4.iterateTable(new IMonetDBTableCursor() {
			@Override
			public void processNextRow(RowIterator rowIterator) {
				String test1 = rowIterator.getColumnByIndex(1, String.class);
				Integer test2 = rowIterator.getColumnByIndex(2, Integer.class);
				switch (rowIterator.getCurrentIterationNumber()) {
					case 1:
						Assertions.assertEquals("one", test1, "Table iteration not working");
						Assertions.assertEquals(new Integer(-40), test2, "Table iteration not working");
						break;
					case 2:
						Assertions.assertEquals("more", test1, "Table iteration not working");
						Assertions.assertEquals(new Integer(5), test2, "Table iteration not working");
						break;
					default:
						Assertions.assertEquals("test", test1, "Table iteration not working");
						Assertions.assertEquals(new Integer(37), test2, "Table iteration not working");
				}
			}

			@Override
			public int getFirstRowToIterate() {
				return 1;
			}

			@Override
			public int getLastRowToIterate() {
				return 3;
			}
		});

		connection.executeUpdate("DROP TABLE test4;");
	}

	@Test
	@DisplayName("Test appending basic types into a table (Also testing foreign characters)")
	void testAppendBasic() throws MonetDBEmbeddedException {
		connection.executeUpdate("CREATE TABLE test5 (a boolean, b text, c tinyint, d smallint, e int, f bigint, h real, i double, j oid);");
		MonetDBTable test5 = connection.getMonetDBTable("sys", "test5");

		byte[] append1 = new byte[]{1, 1, 0, 0, NullMappings.getBooleanNullConstant()};
		String[] append2 = new String[]{"eerlijk", "lekker", "smullen", "ñsmañakñ", NullMappings.getObjectNullConstant()};
		byte[] append3 = new byte[]{-1, 54, -65, 1 , NullMappings.getByteNullConstant() };
		short[] append4 = new short[]{-9808, 54, 99, 5233, NullMappings.getShortNullConstant() };
		int[] append5 = new int[]{2, 3, -1122100, -23123, NullMappings.getIntNullConstant()};
		long[] append6 = new long[]{635L, 123L, -1122343100L, -2312433L, NullMappings.getLongNullConstant()};
		float[] append7 = new float[]{635.2f, 123.1f, -1.4f, -2.33f, NullMappings.getFloatNullConstant()};
		double[] append8 = new double[]{635.2d, 123.1d, -1.4d, -2.23d, NullMappings.getDoubleNullConstant()};
		String[] append9 = new String[]{"0@0", "1@0", "2@0", "3@0", NullMappings.getObjectNullConstant()};
		Object[] appends = new Object[]{append1, append2, append3, append4, append5, append6, append7, append8, append9};
		test5.appendColumns(appends);

		QueryResultSet qrs = connection.executeQuery("SELECT * FROM test5;");
		int numberOfRows = qrs.getNumberOfRows(), numberOfColumns = qrs.getNumberOfColumns();
		Assertions.assertEquals(5, numberOfRows, "The number of rows should be 5, got " + numberOfRows + " instead");
		Assertions.assertEquals(9, numberOfColumns, "The number of columns should be 9, got " + numberOfColumns + " instead");

		boolean[] array1 = new boolean[4];
		qrs.getBooleanColumnByIndex(1, array1, 0, 4);
		Assertions.assertArrayEquals(new boolean[]{true, true, false, false}, array1, "Booleans not correctly appended");
		//The boolean null value cannot be tested directly because in Java side you can only see true or false values
		Assertions.assertTrue(qrs.checkBooleanIsNull(1,5), "Booleans nulls not working");

		String[] array2 = new String[5];
		qrs.getStringColumnByIndex(2, array2);
		Assertions.assertArrayEquals(append2, array2, "Strings not correctly appended");

		byte[] array3 = new byte[5];
		qrs.getByteColumnByIndex(3, array3);
		Assertions.assertArrayEquals(append3, array3, "Tinyints not correctly appended");

		short[] array4 = new short[5];
		qrs.getShortColumnByIndex(4, array4);
		Assertions.assertArrayEquals(append4, array4, "Smallints not correctly appended");

		int[] array5 = new int[5];
		qrs.getIntColumnByIndex(5, array5);
		Assertions.assertArrayEquals(append5, array5, "Integers not correctly appended");

		long[] array6 = new long[5];
		qrs.getLongColumnByIndex(6, array6);
		Assertions.assertArrayEquals(append6, array6, "Integers not correctly appended");

		float[] array7 = new float[5];
		qrs.getFloatColumnByIndex(7, array7);
		Assertions.assertArrayEquals(append7, array7, 0.01f, "Floats not correctly appended");

		double[] array8 = new double[5];
		qrs.getDoubleColumnByIndex(8, array8);
		Assertions.assertArrayEquals(append8, array8, 0.01d, "Doubles not correctly appended");

		String[] array9 = new String[5];
		qrs.getOidColumnByIndex(9, array9);
		Assertions.assertArrayEquals(append9, array9, "Oids not correctly appended");

		qrs.close();
		connection.executeUpdate("DROP TABLE test5;");
	}

	@Test
	@DisplayName("Test appending decimals into a table")
	void testAppendDecimals() throws MonetDBEmbeddedException {
		connection.executeUpdate("CREATE TABLE test5 (a decimal);");
		MonetDBTable test5 = connection.getMonetDBTable("sys", "test5");

		BigDecimal[] append1 = new BigDecimal[]{new BigDecimal("1.600"), new BigDecimal("12.000"),
				NullMappings.getObjectNullConstant(), new BigDecimal("900.000"), new BigDecimal("-1.232") };
		Object[] appends = new Object[]{append1};
		test5.appendColumns(appends);

		QueryResultSet qrs = connection.executeQuery("SELECT * FROM test5;");
		int numberOfRows = qrs.getNumberOfRows(), numberOfColumns = qrs.getNumberOfColumns();
		Assertions.assertEquals(5, numberOfRows, "The number of rows should be 5, got " + numberOfRows + " instead");
		Assertions.assertEquals(1, numberOfColumns, "The number of columns should be 1, got " + numberOfColumns + " instead");

		BigDecimal[] array1 = new BigDecimal[numberOfRows];
		qrs.getDecimalColumnByIndex(1, array1);
		Assertions.assertArrayEquals(append1, array1, "Decimals not correctly appended");

		qrs.close();
		connection.executeUpdate("DROP TABLE test5;");
	}

	@Test
	@Disabled("Who is brave enough to deal with timezones?")
	@DisplayName("Test appending dates into a table")
	void testAppendDates() throws MonetDBEmbeddedException, ParseException {
		SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		SimpleDateFormat timeFormater = new SimpleDateFormat("HH:mm:ss", Locale.ENGLISH);
		SimpleDateFormat timestampFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);

		connection.executeUpdate("CREATE TABLE test6 (a date, b time, c time with time zone, d timestamp, e timestamp with time zone, f INTERVAL year to month, g INTERVAL minute to second);");
		MonetDBTable test6 = connection.getMonetDBTable("sys", "test6");

		Date[] append1 = new Date[]{new Date(dateFormater.parse("2016-01-01").getTime()),
				new Date(dateFormater.parse("1998-10-27").getTime()),
				new Date(dateFormater.parse("2014-02-02").getTime()),
				new Date(dateFormater.parse("1971-12-12").getTime()),
				NullMappings.getObjectNullConstant()};

		Time[] append2 = new Time[]{new Time(timeFormater.parse("23:10:47").getTime()),
				new Time(timeFormater.parse("00:10:47").getTime()),
				new Time(timeFormater.parse("10:10:47").getTime()),
				new Time(timeFormater.parse("20:10:47").getTime()),
				NullMappings.getObjectNullConstant()};

		Time[] append3 = new Time[]{new Time(timeFormater.parse("20:10:47").getTime()),
				new Time(timeFormater.parse("21:10:47").getTime()),
				new Time(timeFormater.parse("11:10:47").getTime()),
				new Time(timeFormater.parse("02:10:47").getTime()),
				NullMappings.getObjectNullConstant()};

		Timestamp[] append4 = new Timestamp[]{
				new Timestamp(timestampFormater.parse("2016-11-04 08:59:44").getTime()),
				new Timestamp(timestampFormater.parse("2016-11-04 08:59:44").getTime()),
				new Timestamp(timestampFormater.parse("2016-11-04 08:59:44Z").getTime()),
				new Timestamp(timestampFormater.parse("2016-11-04 08:59:44").getTime()),
				NullMappings.getObjectNullConstant()};

		Timestamp[] append5 = new Timestamp[]{
				new Timestamp(timestampFormater.parse("2016-11-04 08:59:44").getTime()),
				new Timestamp(timestampFormater.parse("2016-11-04 08:59:44").getTime()),
				new Timestamp(timestampFormater.parse("2016-11-04 08:59:44").getTime()),
				new Timestamp(timestampFormater.parse("2016-11-04 08:59:44").getTime()),
				NullMappings.getObjectNullConstant()};

		int[] append6 = new int[]{2, 3, -1122100, -23123, NullMappings.getIntNullConstant()};
		long[] append7 = new long[]{635L, 123L, -11400L, -23133L, NullMappings.getLongNullConstant()};
		Object[] appends = new Object[]{append1, append2, append3, append4, append5, append6, append7};
		test6.appendColumns(appends);

		QueryResultSet qrs = connection.executeQuery("SELECT * FROM test6;");
		int numberOfRows = qrs.getNumberOfRows(), numberOfColumns = qrs.getNumberOfColumns();
		Assertions.assertEquals(5, numberOfRows, "The number of rows should be 5, got " + numberOfRows + " instead");
		Assertions.assertEquals(7, numberOfColumns, "The number of columns should be 7, got " + numberOfColumns + " instead");

		Date[] array1 = new Date[5];
		qrs.getDateColumnByIndex(1, array1);
		Assertions.assertArrayEquals(append1, array1, "Dates not correctly appended");

		Time[] array2 = new Time[5];
		qrs.getTimeColumnByIndex(2, array2);
		Assertions.assertArrayEquals(append2, array2, "Times not correctly appended");

		Time[] array3 = new Time[5];
		qrs.getTimeColumnByIndex(3, array3);
		Assertions.assertArrayEquals(append3, array3, "Times with timezones not correctly appended");

		Timestamp[] array4 = new Timestamp[5];
		qrs.getTimestampColumnByIndex(4, array4);
		Assertions.assertArrayEquals(append4, array4, "Timestamps not correctly appended");

		Timestamp[] array5 = new Timestamp[5];
		qrs.getTimestampColumnByIndex(5, array5);
		Assertions.assertArrayEquals(append5, array5, "Timestamps with timezone not correctly appended");

		int[] array6 = new int[5];
		qrs.getIntColumnByIndex(6, array6);
		Assertions.assertArrayEquals(append6, array6, "Month intervals not correctly appended");

		long[] array7 = new long[5];
		qrs.getLongColumnByIndex(7, array7);
		Assertions.assertArrayEquals(append7, array7, "Second intervals not correctly appended");

		qrs.close();
		connection.executeUpdate("DROP TABLE test6;");
	}

	@Test
	@DisplayName("Test appending BLOBs into a table")
	void testAppendBlobs() throws Exception {
		connection.executeUpdate("CREATE TABLE test7 (a blob);");
		MonetDBTable test7 = connection.getMonetDBTable("sys", "test7");

		byte[][] append1 = new byte[][]{new byte[]{1,2,-3,4,5,6,7,8,-90,10,13,14,15,16}, new byte[]{-1,-2,-3,-4,-5,-6},
				new byte[]{127}, new byte[]{0,0,0,0,0,34,66,0,0,0,0}, NullMappings.getObjectNullConstant()};
		Object[] appends = new Object[]{append1};
		test7.appendColumns(appends);

		QueryResultSet qrs = connection.executeQuery("SELECT * FROM test7;");
		int numberOfRows = qrs.getNumberOfRows(), numberOfColumns = qrs.getNumberOfColumns();
		Assertions.assertEquals(5, numberOfRows, "The number of rows should be 5, got " + numberOfRows + " instead");
		Assertions.assertEquals(1, numberOfColumns, "The number of columns should be 1, got " + numberOfColumns + " instead");

		byte[][] array1 = new byte[numberOfRows][];
		qrs.getBlobColumnByIndex(1, array1);
		Assertions.assertArrayEquals(append1, array1, "Blobs not correctly appended");

		qrs.close();
		connection.executeUpdate("DROP TABLE test7;");
	}

	@Test
	@DisplayName("Test the number of rows returned by update queries")
	void testUpdates() throws MonetDBEmbeddedException {
		int rows1 = connection.executeUpdate("CREATE TABLE testupdates (val int);");
		Assertions.assertEquals(-2, rows1, "The creation should have affected no rows");

		int rows2 = connection.executeUpdate("INSERT INTO testupdates VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9);");
		Assertions.assertEquals(9, rows2, "It should have affected 9 rows");

		int rows3 = connection.executeUpdate("UPDATE testupdates SET val=2 WHERE val<3;");
		Assertions.assertEquals(2, rows3, "It should have affected 2 rows");

		int rows4 = connection.executeUpdate("UPDATE testupdates SET val=10 WHERE val>5;");
		Assertions.assertEquals(4, rows4, "It should have affected 4 rows");

		int rows5 = connection.executeUpdate("UPDATE testupdates SET val=2;");
		Assertions.assertEquals(9, rows5, "It should have affected 9 rows");

		int rows6 = connection.executeUpdate("DROP TABLE testupdates;");
		Assertions.assertEquals(-2, rows6, "The deletion should have affected no rows");
	}

	@Test
	@DisplayName("Test prepared statement in the regular API")
	void testPreparedStatements() throws MonetDBEmbeddedException {
		connection.executeUpdate("CREATE TABLE testPrepared (a int, b clob);");

		MonetDBEmbeddedPreparedStatement statement1 = connection.prepareStatement("INSERT INTO testPrepared VALUES (?, ?);");
		statement1.setInt(1, 12);
		statement1.setString(2, "lekker");
		Assertions.assertEquals(1, statement1.executeUpdate(), "The insertion should have affected one row");

		Assertions.assertThrows(MonetDBEmbeddedException.class, statement1::executeQuery);

		statement1.setInt(1, 13);
		statement1.setString(2, "smullen");
		Assertions.assertFalse(statement1.execute(), "It should have returned false");

		statement1.setInt(1, 14);
		statement1.setString(2, "heerlijk");
		statement1.executeAndIgnore();

		statement1.close();

		MonetDBEmbeddedPreparedStatement statement2 = connection.prepareStatement("SELECT * FROM testPrepared WHERE a=?;");
		statement2.setInt(1, 13);

		QueryResultSet qrs = statement2.executeQuery();
		Assertions.assertEquals(1, qrs.getNumberOfRows(), "The result set should have returned one row");
		Assertions.assertEquals("smullen", qrs.getStringByColumnIndexAndRow(2, 1), "The string value is not the inserted one");
		qrs.close();

		Assertions.assertThrows(MonetDBEmbeddedException.class, statement2::executeUpdate);

		statement2.setInt(1, 13);
		Assertions.assertTrue(statement2.execute(), "It should have returned true");

		statement2.setInt(1, 14);
		statement2.executeAndIgnore();

		statement2.close();

		MonetDBEmbeddedPreparedStatement statement3 = connection.prepareStatement("SELECT * FROM testPrepared WHERE 1=0;");
		Assertions.assertThrows(MonetDBEmbeddedException.class, () -> statement3.setInt(1, 50));

		QueryResultSet qrs2 = statement3.executeQuery();
		Assertions.assertEquals(0, qrs2.getNumberOfRows(), "The result set should have no rows");
		qrs2.close();
		statement3.close();

		connection.executeUpdate("DROP TABLE testPrepared;");
	}

	@Test
	@DisplayName("Test regular expressions (we removed the pcre dependency from MonetDBLite recently)")
	void testRegexes() throws MonetDBEmbeddedException {
		QueryResultSet qrs = connection.executeQuery("SELECT name from tables where name LIKE '%chemas'");
		String schemas = qrs.getStringByColumnIndexAndRow(1,1);
		Assertions.assertEquals("schemas", schemas, "The regex is not working properly?");
		qrs.close();
	}

	@Test
	@DisplayName("Test another process attempting to perform a concurrent access into the database directory")
	void testProcessConcurrentAccessIntoTheDatabase() throws MonetDBEmbeddedException, IOException, InterruptedException {
		String databaseDirectory = MonetDBEmbeddedDatabase.getDatabaseDirectory();
		Map<String, String> environment = new HashMap<>();
		environment.put(TryStartMonetDBEmbeddedDatabase.DATABASE_ENV, databaseDirectory);

		int status = ForkJavaProcess.exec(TryStartMonetDBEmbeddedDatabase.class, environment);
		Assertions.assertEquals(0, status, "The other process should have failed to lock the database");
	}

	@Test
	@DisplayName("Test COPY FROM and COPY INTO")
	void testCSVParser() throws MonetDBEmbeddedException, IOException, InterruptedException {
		//For the import we are are going to create a temporary file
		final File csvImportFile = Files.createTempFile("testingmonetdbjavaliteimport",".csv").toFile();
		final String csvImportFilePath = csvImportFile.getAbsolutePath();
		final String csvImportFilePathTrimmed = csvImportFilePath.replace('\\', '/');
		//For the export the file must be inexistent, so we have to delete it first
		final String csvExportFilePath =
				Paths.get(System.getProperty("java.io.tmpdir"), "testingmonetdbjavaliteexport.csv")
						.toAbsolutePath().toString();
		final String csvExportFilePathTrimmed = csvExportFilePath.replace('\\', '/');
		Files.deleteIfExists(Paths.get(csvExportFilePath));

		String[] column1 = new String[]{"testingCSVParser", null, "Great test", "Have a nice day! :)"};
		int[] column2 = new int[]{0, -3545, NullMappings.getIntNullConstant(), 33};
		float[] column3 = new float[]{3.75f, -7.858f, 535.432f, NullMappings.getFloatNullConstant()};
		String lineSep = System.lineSeparator();

		//We could use a Java 8 stream with the StringJoin aggregator, but let's be simple...
		String csvToImport =
				"\"" + column1[0] + "\"," + column2[0] + "," + column3[0] + lineSep +
					   "null"     + ","   + column2[1] + "," + column3[1] + lineSep +
				"\"" + column1[2] + "\"," + "null"     + "," + column3[2] + lineSep +
				"\"" + column1[3] + "\"," + column2[3] + "," + "null"     + lineSep;
		PrintWriter pw = new PrintWriter(csvImportFile);
		pw.write(csvToImport);
		pw.flush();
		pw.close();

		int rows1 = connection.executeUpdate("CREATE TABLE testCSV (a TEXT, b INT, c real);");
		Assertions.assertEquals(-2, rows1, "The creation should have affected no rows");
		connection.executeUpdate("COPY INTO testCSV FROM '" + csvImportFilePathTrimmed + "' USING DELIMITERS ',','\n','\"';");

		QueryResultSet qrs = connection.executeQuery("SELECT a, b, c FROM testCSV;");
		int numberOfRows = qrs.getNumberOfRows(), numberOfColumns = qrs.getNumberOfColumns();
		Assertions.assertEquals(4, numberOfRows, "The number of rows should be 4, got " + numberOfRows + " instead");
		Assertions.assertEquals(3, numberOfColumns, "The number of columns should be 3, got " + numberOfColumns + " instead");

		String[] array1 = new String[4];
		qrs.getStringColumnByIndex(1, array1);
		Assertions.assertArrayEquals(column1, array1, "CSV not working with Strings");

		int[] array2 = new int[4];
		qrs.getIntColumnByIndex(2, array2);
		Assertions.assertArrayEquals(column2, array2, "CSV not working with Integers");

		float[] array3 = new float[4];
		qrs.getFloatColumnByIndex(3, array3);
		Assertions.assertArrayEquals(column3, array3, 0.01f, "CSV not working with Floats");
		qrs.close();

		connection.executeUpdate("COPY SELECT * FROM testCSV INTO '" + csvExportFilePathTrimmed + "' USING DELIMITERS ',','\n';");

		//Compare the two files!
		String csvExported = new String(Files.readAllBytes(Paths.get(csvExportFilePath)), StandardCharsets.UTF_8);
		Assertions.assertEquals(csvToImport, csvExported, "The CSVs are not equal");

		int rows2 = connection.executeUpdate("DROP TABLE testCSV;");
		Assertions.assertEquals(-2, rows2, "The deletion should have affected no rows");
	}

	@Test
	@DisplayName("Test binary imports")
	void testBinaryImport() throws IOException, MonetDBEmbeddedException {
		final File binImport1 = Files.createTempFile("binimportint",".bin").toFile();
		final String binImport1FilePath = binImport1.getAbsolutePath();
		final String binImport1FilePathTrimmed = binImport1FilePath.replace('\\', '/');
		final File binImport2 = Files.createTempFile("binimporstring",".bin").toFile();
		final String binImport2FilePath = binImport2.getAbsolutePath();
		final String binImport2FilePathTrimmed = binImport2FilePath.replace('\\', '/');
		final File binImport3 = Files.createTempFile("binimportfloat",".bin").toFile();
		final String binImport3FilePath = binImport3.getAbsolutePath();
		final String binImport3FilePathTrimmed = binImport3FilePath.replace('\\', '/');
		final File binImport4 = Files.createTempFile("binimportlong",".bin").toFile();
		final String binImport4FilePath = binImport4.getAbsolutePath();
		final String binImport4FilePathTrimmed = binImport4FilePath.replace('\\', '/');
		final File binImport5 = Files.createTempFile("binimportdouble",".bin").toFile();
		final String binImport5FilePath = binImport5.getAbsolutePath();
		final String binImport5FilePathTrimmed = binImport5FilePath.replace('\\', '/');
		final File binImport6 = Files.createTempFile("binimportbyte",".bin").toFile();
		final String binImport6FilePath = binImport6.getAbsolutePath();
		final String binImport6FilePathTrimmed = binImport6FilePath.replace('\\', '/');

		int[] data1 = {137, -89, 82, 0, -54};
		DataOutputStream file1 = new DataOutputStream(new FileOutputStream(binImport1FilePath));
		for (int aData1 : data1)
			file1.writeInt(aData1);
		file1.close();

		String[] data2 = {"a", "binary", "import", "test", ""};
		PrintWriter file2 = new PrintWriter(binImport2FilePath);
		for (String aData2 : data2)
			file2.println(aData2);
		file2.close();

		float[] data3 = {0.0f, 1.2f, -0.32f, 124.2f, -67.12f};
		DataOutputStream file3 = new DataOutputStream(new FileOutputStream(binImport3FilePath));
		for (float aData3 : data3)
			file3.writeFloat(aData3);
		file3.close();

		long[] data4 = {0L, 1L, 10000L, -2414124L, 1212L};
		DataOutputStream file4 = new DataOutputStream(new FileOutputStream(binImport4FilePath));
		for (long aData4 : data4)
			file4.writeLong(aData4);
		file4.close();

		double[] data5 = {0.0f, 1.2f, -0.32f, 124.2f, -67.12f};
		DataOutputStream file5 = new DataOutputStream(new FileOutputStream(binImport5FilePath));
		for (double aData5 : data5)
			file5.writeDouble(aData5);
		file5.close();

		byte[] data6 = {0, 1, -2, -4, 5};
		DataOutputStream file6 = new DataOutputStream(new FileOutputStream(binImport6FilePath));
		for (byte aData6 : data6)
			file6.writeByte(aData6);
		file6.close();

		int rows1 = connection.executeUpdate("CREATE TABLE testBinImp (aa int, bb clob, cc real, dd bigint, ee double, ff tinyint);");
		Assertions.assertEquals(-2, rows1, "The creation should have affected no rows");
		int rows2 = connection.executeUpdate("COPY BINARY INTO testBinImp FROM ('" + binImport1FilePathTrimmed + "', '" +
				binImport2FilePathTrimmed + "', '" + binImport3FilePathTrimmed + "', '" + binImport4FilePathTrimmed + "', '" +
				binImport5FilePathTrimmed + "', '" + binImport6FilePathTrimmed + "');");
		Assertions.assertEquals(5, rows2, "The copy binary into should have affected 5 rows");

		QueryResultSet qrs = connection.executeQuery("SELECT aa, bb, cc, dd, ee, ff FROM testBinImp;");
		int numberOfRows = qrs.getNumberOfRows(), numberOfColumns = qrs.getNumberOfColumns();
		Assertions.assertEquals(5, numberOfRows, "The number of rows should be 5, got " + numberOfRows + " instead");
		Assertions.assertEquals(6, numberOfColumns, "The number of columns should be 6, got " + numberOfColumns + " instead");

		int[] array1 = new int[5];
		qrs.getIntColumnByIndex(1, array1);
		Assertions.assertArrayEquals(data1, array1, "Binary import not working with Integers");

		String[] array2 = new String[5];
		qrs.getStringColumnByIndex(2, array2);
		Assertions.assertArrayEquals(data2, array2, "Binary import not working with Strings");

		float[] array3 = new float[5];
		qrs.getFloatColumnByIndex(3, array3);
		Assertions.assertArrayEquals(data3, array3, "Binary import not working with Floating-points");

		long[] array4 = new long[5];
		qrs.getLongColumnByIndex(4, array4);
		Assertions.assertArrayEquals(data4, array4, "Binary import not working with Longs");

		double[] array5 = new double[5];
		qrs.getDoubleColumnByIndex(5, array5);
		Assertions.assertArrayEquals(data5, array5, "Binary import not working with Doubles");

		byte[] array6 = new byte[5];
		qrs.getByteColumnByIndex(6, array6);
		Assertions.assertArrayEquals(data6, array6, "Binary import not working with Bytes");

		qrs.close();
		int rows3 = connection.executeUpdate("DROP TABLE testBinImp;");
		Assertions.assertEquals(-2, rows3, "The deletion should have affected no rows");
	}

	@Test
	@DisplayName("Test autocommit consistent")
	void testAutoCommitAgain() throws MonetDBEmbeddedException {
		Assertions.assertThrows(MonetDBEmbeddedException.class, () -> connection.executeUpdate("DROP TABLE dontexist;"));
		connection.executeUpdate("CREATE TABLE beok (a int);");
		connection.executeUpdate("DROP TABLE beok;");
	}

	@Test
	@DisplayName("Test schemas")
	void testSchemas() throws MonetDBEmbeddedException {
		int numberOfRows, numberOfColumns;
		connection.executeUpdate("create schema bar;");
		connection.executeUpdate("set schema bar;");
		connection.executeUpdate("create table foo (a int, b int);");
		connection.executeUpdate("insert into foo values (1, 2);");

		QueryResultSet qrs = connection.executeQuery("select * from foo;");
		numberOfRows = qrs.getNumberOfRows();
		numberOfColumns = qrs.getNumberOfColumns();
		Assertions.assertEquals(1, numberOfRows, "The number of rows should be 1, got " + numberOfRows + " instead");
		Assertions.assertEquals(2, numberOfColumns, "The number of columns should be 2, got " + numberOfColumns + " instead");
		qrs.close();

		QueryResultSet qrs2 = connection.executeQuery("select * from bar.foo;");
		numberOfRows = qrs2.getNumberOfRows();
		numberOfColumns = qrs2.getNumberOfColumns();
		Assertions.assertEquals(1, numberOfRows, "The number of rows should be 1, got " + numberOfRows + " instead");
		Assertions.assertEquals(2, numberOfColumns, "The number of columns should be 2, got " + numberOfColumns + " instead");
		qrs2.close();

		connection.executeUpdate("set schema sys;");
		connection.executeUpdate("drop schema bar cascade;");

		Assertions.assertThrows(MonetDBEmbeddedException.class, () -> connection.executeUpdate("set schema bar;"));
		Assertions.assertThrows(MonetDBEmbeddedException.class, () -> connection.executeQuery("select * from bar.foo;"));

		connection.executeUpdate("create schema other;");
		connection.executeUpdate("drop schema other;");
		Assertions.assertThrows(MonetDBEmbeddedException.class, () -> connection.executeUpdate("set schema other;"));
	}

	@AfterAll
	@DisplayName("Shutdown database at the end")
	static void shutDatabase() throws MonetDBEmbeddedException, IOException {
		connection.close();
		MonetDBJavaLiteTesting.shutdownDatabase();
		Assertions.assertFalse(MonetDBEmbeddedDatabase::isDatabaseRunning, "The database should be closed");
		Assertions.assertTrue(connection::isClosed, "The connection should be closed");

		//If the database is closed, then the connection will close as well
		Assertions.assertThrows(MonetDBEmbeddedException.class, () -> connection.executeQuery("SELECT 1;"));
		//Stop the database again also shouldn't work
		Assertions.assertThrows(MonetDBEmbeddedException.class, MonetDBEmbeddedDatabase::stopDatabase);

		//start again the database in another directory and stop it
		Path otherPath = Files.createTempDirectory("monetdbtestother");
		MonetDBEmbeddedDatabase.startDatabase(otherPath.toString(), true, false);
		MonetDBEmbeddedDatabase.createConnection().close();
		MonetDBEmbeddedDatabase.stopDatabase();
	}
}
