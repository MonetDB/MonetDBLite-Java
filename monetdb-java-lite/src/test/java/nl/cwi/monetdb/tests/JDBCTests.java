/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

package nl.cwi.monetdb.tests;

import nl.cwi.monetdb.tests.helpers.MonetDBJavaLiteTesting;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * Test JDBC stuff in MonetDBJavaLite :)
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class JDBCTests extends MonetDBJavaLiteTesting {

	static {
		try {
			Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		} catch (ClassNotFoundException e) { }
	}

	private static Connection createJDBCEmbeddedConnection() throws SQLException {
		return DriverManager.getConnection("jdbc:monetdb:embedded:" + MonetDBJavaLiteTesting.getDirectoryPath().toString());
	}

	@Test
	@DisplayName("Test updates in JDBC")
	void testJDBCUpdates() throws SQLException {
		Connection conn = createJDBCEmbeddedConnection();
		Statement stmt = conn.createStatement();
		int rows1 = stmt.executeUpdate("CREATE TABLE testjdbcupdates (val int);");
		Assertions.assertEquals(-2, rows1, "The creation should have affected no rows");

		int rows2 = stmt.executeUpdate("INSERT INTO testjdbcupdates VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9);");
		Assertions.assertEquals(9, rows2, "It should have affected 9 rows");

		int rows3 = stmt.executeUpdate("UPDATE testjdbcupdates SET val=2 WHERE val<3;");
		Assertions.assertEquals(2, rows3, "It should have affected 2 rows");

		int rows4 = stmt.executeUpdate("UPDATE testjdbcupdates SET val=10 WHERE val>5;");
		Assertions.assertEquals(4, rows4, "It should have affected 4 rows");

		int rows5 = stmt.executeUpdate("UPDATE testjdbcupdates SET val=2;");
		Assertions.assertEquals(9, rows5, "It should have affected 9 rows");

		int rows6 = stmt.executeUpdate("DROP TABLE testjdbcupdates;");
		Assertions.assertEquals(-2, rows6, "The deletion should have affected no rows");

		stmt.close();
		conn.close();
	}

	@Test
	@DisplayName("Test a simple result set in JDBC")
	void testSimpleTable() throws SQLException {
		Connection conn = createJDBCEmbeddedConnection();
		Statement stmt = conn.createStatement();
		int rows1 = stmt.executeUpdate("CREATE TABLE test1 (a smallint, b varchar(32), c real, d int, e boolean, f oid);");
		Assertions.assertEquals(-2, rows1, "The creation should have affected no rows");
		int rows2 = stmt.executeUpdate("INSERT INTO test1 VALUES (1, 'one', 3.223, 100, 'true', 1);");
		Assertions.assertEquals(1, rows2, "The creation should have affected 1 row");
		int rows3 = stmt.executeUpdate("INSERT INTO test1 VALUES (2, NULL, NULL, -2032, 'false', 0);");
		Assertions.assertEquals(1, rows3, "The creation should have affected 1 row");
		int rows4 = stmt.executeUpdate("INSERT INTO test1 VALUES (-3545, 'test', -7.858, 423423, NULL, NULL);");
		Assertions.assertEquals(1, rows4, "The creation should have affected 1 row");

		short[] column1 = new short[]{1, 2, -3545};
		String[] column2 = new String[]{"one", null, "test"};
		float[] column3 = new float[]{3.223f, 0.0f, -7.858f};
		int[] column4 = new int[]{100, -2032, 423423};
		boolean[] column5 = new boolean[]{true, false, false};
		String[] column6 = new String[]{"1@0", "0@0", null};

		ResultSet rs = stmt.executeQuery("SELECT * from test1;");
		for(int i = 0 ; i < 3; i++) {
			rs.next();
			Assertions.assertEquals(column1[i], rs.getShort(1), "Problems in the JDBC result set");
			Assertions.assertEquals(column2[i], rs.getString(2), "Problems in the JDBC result set");
			Assertions.assertEquals(column3[i], rs.getFloat(3), 0.01f, "Problems in the JDBC result set");
			Assertions.assertEquals(column4[i], rs.getInt(4), "Problems in the JDBC result set");
			Assertions.assertEquals(column5[i], rs.getBoolean(5), "Problems in the JDBC result set");
			Assertions.assertEquals(column6[i], rs.getString(6), "Problems in the JDBC result set");
		}
		rs.close();
		int rows5 = stmt.executeUpdate("DROP TABLE test1;");
		Assertions.assertEquals(-2, rows5, "The deletion should have affected no rows");
		stmt.close();
		conn.close();
	}

	@Test
	@DisplayName("Test objects retrieval in the result set")
	void testObjectResultSet() throws SQLException {
		Connection conn = createJDBCEmbeddedConnection();
		Statement stmt = conn.createStatement();
		int rows1 = stmt.executeUpdate("CREATE TABLE test2 (a int, b boolean, c real, d text)");
		Assertions.assertEquals(-2, rows1, "The creation should have affected no rows");
		int rows2 = stmt.executeUpdate("INSERT INTO test2 VALUES (1, 'false', 3.2, 'hola');");
		Assertions.assertEquals(1, rows2, "The insertion should have affected 1 row");

		ResultSet rs = stmt.executeQuery("SELECT * from test2;");
		rs.next();
		Assertions.assertEquals(1, rs.getObject(1), "Problems in the JDBC result set");
		Assertions.assertEquals(false, rs.getObject(2), "Problems in the JDBC result set");
		Assertions.assertEquals(3.2f, rs.getObject(3), "Problems in the JDBC result set");
		Assertions.assertEquals("hola", rs.getObject(4), "Problems in the JDBC result set");
		rs.close();

		int rows5 = stmt.executeUpdate("DROP TABLE test2;");
		Assertions.assertEquals(-2, rows5, "The deletion should have affected no rows");
		stmt.close();
		conn.close();
	}

	@Test
	@DisplayName("Create some concurrent JDBC connections")
	void timeToStress() throws InterruptedException {
		int stress = 16;
		Thread[] stressers = new Thread[stress];
		String[] messages = new String[stress];

		for (int i = 0; i < stress; i++) {
			final int threadID = i;
			final int j = i;
			Thread t = new Thread(() -> {
				Connection con = null;
				Statement stmt = null;
				ResultSet rs = null;
				try {
					con = createJDBCEmbeddedConnection();
					stmt = con.createStatement();
					rs = stmt.executeQuery("SELECT " + threadID);
					if (!rs.next()) {
						messages[j] = "No response from the server in one of the Threads";
					} else {
						int intt = rs.getInt(1);
						if(threadID != intt) {
							messages[j] = "No response from the server in one of the Threads";
						}
					}
				} catch (SQLException e) {
					messages[j] = e.getMessage();
				}
				if(rs != null) {
					try {
						rs.close();
					} catch (SQLException e1) {
						if(messages[j] == null) {
							messages[j] = e1.getMessage();
						}
					}
				}
				if(stmt != null) {
					try {
						stmt.close();
					} catch (SQLException e2) {
						if(messages[j] == null) {
							messages[j] = e2.getMessage();
						}
					}
				}
				if(con != null) {
					try {
						con.close();
					} catch (SQLException e3) {
						if(messages[j] == null) {
							messages[j] = e3.getMessage();
						}
					}
				}
			});
			t.start();
			stressers[j] = t;
		}

		for (Thread t : stressers) {
			t.join();
		}
		for(String ss : messages) {
			if(ss != null) {
				Assertions.fail(ss);
			}
		}
	}

	@Test
	@DisplayName("Test more JDBC concurrent connections")
	void otherStressTest() {
		int stress = 8;
		String message = null;
		Connection[] cons = new Connection[stress]; //Create many simultaneous connections
		try {
			for (int i = 0; i < stress; i++) {
				Connection con = createJDBCEmbeddedConnection();
				con.setAutoCommit(false);
				cons[i] = con;
			}
		} catch (SQLException e1) {
			message = e1.getMessage();
		}
		for (int i = 0; i < stress; i++) {
			try {
				if(cons[i] != null) {
					cons[i].setAutoCommit(true);
					cons[i].close();
				}
			} catch (SQLException e1) {
				if(message == null) {
					message = e1.getMessage();
				}
			}
		}
		if(message != null) {
			Assertions.fail(message);
		}
	}

	@Test
	@DisplayName("Test autocommit feature between two connections")
	void testAutocommit() throws SQLException {
		Connection con1 = createJDBCEmbeddedConnection();
		Connection con2 = createJDBCEmbeddedConnection();
		Statement stmt1 = con1.createStatement();
		Statement stmt2 = con2.createStatement();
		ResultSet rs;

		Assertions.assertTrue(con1.getAutoCommit(), "Auto-commit not working?");
		Assertions.assertTrue(con2.getAutoCommit(), "Auto-commit not working?");

		int rows1 = stmt1.executeUpdate("CREATE TABLE testAutoCommit (id int)");
		Assertions.assertEquals(-2, rows1, "The creation should have affected no rows");

		stmt2.executeQuery("SELECT * FROM testAutoCommit");

		// turn off auto commit
		con1.setAutoCommit(false);
		con2.setAutoCommit(false);

		Assertions.assertFalse(con1.getAutoCommit(), "Auto-commit not working?");
		Assertions.assertFalse(con2.getAutoCommit(), "Auto-commit not working?");

		int rows2 = stmt2.executeUpdate("DROP TABLE testAutoCommit");
		Assertions.assertEquals(-2, rows2, "The deletion should have affected no rows");
		rs = stmt1.executeQuery("SELECT * FROM testAutoCommit");
		rs.close();
		con2.commit();
		rs = stmt1.executeQuery("SELECT * FROM testAutoCommit");
		rs.close();
		con1.commit();

		stmt1.close();
		stmt2.close();
		con1.close();
		con2.close();
	}

	@Test
	@DisplayName("Test that a SQL exception is thrown")
	void testSQLException() throws SQLException {
		Connection conn = createJDBCEmbeddedConnection();
		Statement stmt = conn.createStatement();
		conn.setAutoCommit(false);
		Assertions.assertThrows(SQLException.class, () -> stmt.execute("SELECT COUNT(*) FROM YouAreGoingToFail;"));
		Assertions.assertTrue(conn.isValid(30), "Auto-commit not working?");

		conn.rollback(); //we can rollback still
		stmt.close();
		conn.close();
	}

	@Test
	@DisplayName("Test transaction management in JDBC")
	void testTransaction() throws SQLException {
		Connection conn = createJDBCEmbeddedConnection();
		Statement stmt = conn.createStatement();

		Assertions.assertTrue(conn.getAutoCommit(), "Auto-commit not working?");
		Assertions.assertThrows(SQLException.class, conn::commit);

		conn.setAutoCommit(false);
		Assertions.assertFalse(conn.getAutoCommit(), "Auto-commit not working?");

		conn.commit();
		conn.rollback();

		conn.setAutoCommit(true);
		Assertions.assertTrue(conn.getAutoCommit(), "Auto-commit not working?");

		int rows1 = stmt.executeUpdate("START TRANSACTION");
		Assertions.assertEquals(-2, rows1, "The value returned by a transaction query is -2");
		conn.commit();
		Assertions.assertTrue(conn.getAutoCommit(), "Auto-commit not working?");

		int rows2 = stmt.executeUpdate("START TRANSACTION");
		Assertions.assertEquals(-2, rows2, "The value returned by a transaction query is -2");
		conn.rollback();
		Assertions.assertTrue(conn.getAutoCommit(), "Auto-commit not working?");
		Assertions.assertThrows(SQLException.class, conn::commit);

		stmt.close();
		conn.close();
	}

	@Test
	@DisplayName("Test savepoints in JDBC")
	void testSavePoints() throws SQLException {
		Connection con = createJDBCEmbeddedConnection();
		Statement stmt = con.createStatement();
		ResultSet rs;
		int i = 0;

		Assertions.assertTrue(con.getAutoCommit(), "Auto-commit not working?");
		Assertions.assertThrows(SQLException.class, con::setSavepoint);

		con.setAutoCommit(false);
		Assertions.assertFalse(con.getAutoCommit(), "Auto-commit not working?");

		//start with an empty table
		con.setSavepoint();

		int rows1 = stmt.executeUpdate("CREATE TABLE testSavePoints (id int)");
		Assertions.assertEquals(-2, rows1, "The creation should have affected no rows");
		Savepoint sp1 = con.setSavepoint("empty table");
		rs = stmt.executeQuery("SELECT id FROM testSavePoints");
		while (rs.next()) {
			i++;
		}
		Assertions.assertEquals(0, i, "Savepoints not working?");
		rs.close();

		//now add three values
		int rows2 = stmt.executeUpdate("INSERT INTO testSavePoints VALUES (1), (2), (3)");
		Assertions.assertEquals(3, rows2, "The number of rows affected should be 3");
		Savepoint sp2 = con.setSavepoint("three values");
		rs = stmt.executeQuery("SELECT id FROM testSavePoints");
		i = 0;
		while (rs.next()) {
			i++;
		}
		Assertions.assertEquals(3, i, "Savepoints not working?");
		rs.close();

		//set the savepoint and test again
		con.releaseSavepoint(sp2);
		rs = stmt.executeQuery("SELECT id FROM testSavePoints");
		i = 0;
		while (rs.next()) {
			i++;
		}
		Assertions.assertEquals(3, i, "Savepoints not working?");
		rs.close();

		//release the save point and check that the table is empty
		con.rollback(sp1);
		rs = stmt.executeQuery("SELECT id FROM testSavePoints");
		i = 0;
		while (rs.next()) {
			i++;
		}
		Assertions.assertEquals(0, i, "Savepoints not working?");
		rs.close();

		stmt.close();
		con.rollback();
		con.close();
	}

	@Test
	@DisplayName("Test positions in the JDBC resultset")
	void testPositions() throws SQLException {
		Connection con = createJDBCEmbeddedConnection();
		Statement stmt = con.createStatement();
		DatabaseMetaData dbmd = con.getMetaData();
		ResultSet rs = stmt.executeQuery("SELECT 1");

		Assertions.assertTrue(rs.isBeforeFirst(), "Row positions not working?");
		Assertions.assertFalse(rs.isFirst(), "Row positions not working?");
		Assertions.assertTrue(rs.next(), "Row positions not working?");
		Assertions.assertFalse(rs.isBeforeFirst(), "Row positions not working?");
		Assertions.assertTrue(rs.isFirst(), "Row positions not working?");
		Assertions.assertFalse(rs.isAfterLast(), "Row positions not working?");
		Assertions.assertTrue(rs.isLast(), "Row positions not working?");
		Assertions.assertFalse(rs.next(), "Row positions not working?");
		Assertions.assertTrue(rs.isAfterLast(), "Row positions not working?");
		Assertions.assertFalse(rs.isLast(), "Row positions not working?");
		Assertions.assertFalse(rs.next(), "Row positions not working?");
		Assertions.assertTrue(rs.isAfterLast(), "Row positions not working?");
		rs.close();

		rs = dbmd.getTableTypes(); // try the same with a 'virtual' result set

		Assertions.assertTrue(rs.isBeforeFirst(), "Row positions not working?");
		Assertions.assertFalse(rs.isFirst(), "Row positions not working?");
		Assertions.assertTrue(rs.next(), "Row positions not working?");
		Assertions.assertFalse(rs.isBeforeFirst(), "Row positions not working?");
		Assertions.assertTrue(rs.isFirst(), "Row positions not working?");

		rs.last(); // move to last row

		Assertions.assertFalse(rs.isAfterLast(), "Row positions not working?");
		Assertions.assertTrue(rs.isLast(), "Row positions not working?");
		Assertions.assertFalse(rs.next(), "Row positions not working?");
		Assertions.assertTrue(rs.isAfterLast(), "Row positions not working?");
		Assertions.assertFalse(rs.isLast(), "Row positions not working?");
		Assertions.assertFalse(rs.next(), "Row positions not working?");
		Assertions.assertTrue(rs.isAfterLast(), "Row positions not working?");
		rs.close();

		stmt.close();
		con.close();
	}

	@Test
	@DisplayName("Test prepared statements - Just inserting")
	void testPreparedStatements() throws SQLException {
		Connection con = createJDBCEmbeddedConnection();
		con.setAutoCommit(false);
		Assertions.assertFalse(con.getAutoCommit(), "Auto-commit not working?");

		Statement stmt = con.createStatement();
		int rows1 = stmt.executeUpdate("CREATE TABLE htmtest (htmid bigint NOT NULL, ra double, decl double," +
						" dra double, ddecl double, flux double, dflux double, freq double, bw double, " +
						" type decimal(1,0), imageurl varchar(100), comment varchar(100)," +
						" CONSTRAINT htmtest_htmid_pkey PRIMARY KEY (htmid))");
		Assertions.assertEquals(-2, rows1, "The creation should have affected no rows");
		int rows2 = stmt.executeUpdate("CREATE INDEX htmid ON htmtest (htmid)");
		Assertions.assertEquals(-2, rows2, "The index creation should have affected no rows");
		stmt.close();

		PreparedStatement pstmt = con.prepareStatement("INSERT INTO HTMTEST (HTMID,RA,DECL,FLUX,COMMENT) VALUES (?,?,?,?,?)");
		pstmt.setLong(1, 1L);
		pstmt.setFloat(2, (float)1.2);
		pstmt.setDouble(3, 2.4);
		pstmt.setDouble(4, 3.2);
		pstmt.setString(5, "vlavbla");
		int nrows1 = pstmt.executeUpdate();
		pstmt.close();
		Assertions.assertEquals(1, nrows1, "It should have affected 1 row");

		pstmt = con.prepareStatement("UPDATE HTMTEST set COMMENT=?, TYPE=? WHERE HTMID=?");
		pstmt.setString(1, "some update");
		pstmt.setObject(2, (float)3.2);
		pstmt.setLong(3, 1L);
		int nrows2 = pstmt.executeUpdate();
		pstmt.close();
		Assertions.assertEquals(1, nrows2, "It should have affected 1 row");

		con.rollback();
		con.close();
	}

	@Test
	@DisplayName("Test prepared statements - Checking the result set")
	void testPreparedStatementsAndResultSets() throws SQLException {
		Connection con = createJDBCEmbeddedConnection();
		con.setAutoCommit(false);
		Assertions.assertFalse(con.getAutoCommit(), "Auto-commit not working?");

		Statement stmt = con.createStatement();
		int rows1 = stmt.executeUpdate("CREATE TABLE table_Test_PSgetObject (ti tinyint, si smallint, i int, bi bigint, vvc varchar(32), bbol boolean, fpp double)");
		stmt.close();
		Assertions.assertEquals(-2, rows1, "The creation should have affected no rows");

		byte[] column1 = new byte[]{-35};
		short[] column2 = new short[]{2};
		int[] column3 = new int[]{323123123};
		long[] column4 = new long[]{44534534354L};
		String[] column5 = new String[]{"testing"};
		boolean[] column6 = new boolean[]{true};
		double[] column7 = new double[]{12.345f};

		PreparedStatement pstmt = con.prepareStatement("INSERT INTO table_Test_PSgetObject (ti,si,i,bi,vvc,bbol,fpp) VALUES (?,?,?,?,?,?,?)");
		pstmt.setByte(1, column1[0]);
		pstmt.setShort(2, column2[0]);
		pstmt.setInt(3, column3[0]);
		pstmt.setLong(4, column4[0]);
		pstmt.setString(5, column5[0]);
		pstmt.setBoolean(6, column6[0]);
		pstmt.setDouble(7, column7[0]);
		Assertions.assertFalse(pstmt.execute(), "The value returned should be false in a update query");
		pstmt.close();

		pstmt = con.prepareStatement("SELECT ti,si,i,bi,vvc,bbol,fpp FROM table_Test_PSgetObject ORDER BY ti,si,i,bi,vvc,bbol,fpp");
		ResultSet rs = pstmt.executeQuery();

		rs.next();
		Assertions.assertEquals(column1[0], rs.getByte(1), "Problems in the JDBC prepared statements");
		Assertions.assertEquals(column2[0], rs.getShort(2), "Problems in the JDBC prepared statements");
		Assertions.assertEquals(column3[0], rs.getInt(3), "Problems in the JDBC prepared statements");
		Assertions.assertEquals(column4[0], rs.getLong(4), "Problems in the JDBC prepared statements");
		Assertions.assertEquals(column5[0], rs.getString(5), "Problems in the JDBC prepared statements");
		Assertions.assertEquals(column6[0], rs.getBoolean(6), "Problems in the JDBC prepared statements");
		Assertions.assertEquals(column7[0], rs.getDouble(7), 0.1f, "Problems in the JDBC prepared statements");
		rs.close();
		pstmt.close();

		pstmt = con.prepareStatement("SELECT ti,si,i,bi,vvc,bbol,fpp FROM table_Test_PSgetObject ORDER BY ti,si,i,bi,vvc,bbol,fpp");
		Assertions.assertTrue(pstmt.execute(), "The value returned should be true in a result set query");
		pstmt.close();

		con.rollback();
		con.close();
	}

	@Test
	@DisplayName("Test batch processing in JDBC")
	void testBatchProcessingUpdates() throws SQLException {
		Connection con = createJDBCEmbeddedConnection();
		con.setAutoCommit(false);

		Statement st = con.createStatement();
		int res1 = st.executeUpdate("CREATE TABLE jdbcTest (justAnInteger int, justAString varchar(32));");
		Assertions.assertEquals(-2, res1, "The creation should have affected no rows");

		st.addBatch("INSERT INTO jdbcTest VALUES (3, 'abc')");
		st.addBatch("INSERT INTO jdbcTest VALUES (-10, 'def'), (0, 'ghi'), (100, 'jkl')");
		st.addBatch("UPDATE jdbcTest SET justAString = 'something' WHERE justAnInteger < 3");
		st.addBatch("DELETE FROM jdbcTest WHERE justAnInteger = '100'");
		st.addBatch("SELECT 1");

		Assertions.assertArrayEquals(new int[]{1, 3, 2, 1, -3}, st.executeBatch(), "The response codes are incorrect");

		int[] column1 = new int[]{3, -10, 0};
		String[] column2 = new String[]{"abc", "something", "something"};

		ResultSet rs = st.executeQuery("SELECT justAnInteger, justAString from jdbcTest;");
		for(int i = 0 ; i < 3; i++) {
			rs.next();
			Assertions.assertEquals(column1[i], rs.getInt(1), "Problems in the JDBC result set");
			Assertions.assertEquals(column2[i], rs.getString(2), "Problems in the JDBC result set");
		}
		Assertions.assertFalse(rs.next(), "Incorrect number of rows retrieved");
		rs.close();

		int res2 = st.executeUpdate("DELETE FROM jdbcTest");
		Assertions.assertEquals(3, res2, "The deletion should have affected 3 rows");
		st.close();

		int[] column11 = new int[]{100, 2000, -8984};
		String[] column22 = new String[]{"12another", "test", "to pass on"};

		PreparedStatement pst = con.prepareStatement("INSERT INTO jdbcTest VALUES (?, ?)");
		pst.setInt(1, column11[0]);
		pst.setString(2, column22[0]);
		pst.addBatch();

		pst.setInt(1, column11[1]);
		pst.setString(2, column22[1]);
		pst.addBatch();

		pst.setInt(1, column11[2]);
		pst.setString(2, column22[2]);
		pst.addBatch();

		Assertions.assertArrayEquals(new int[]{1, 1, 1}, pst.executeBatch(), "The response codes are incorrect");
		pst.close();

		rs = st.executeQuery("SELECT justAnInteger, justAString from jdbcTest;");
		for(int i = 0 ; i < 3; i++) {
			rs.next();
			Assertions.assertEquals(column11[i], rs.getInt(1), "Problems in the JDBC result set");
			Assertions.assertEquals(column22[i], rs.getString(2), "Problems in the JDBC result set");
		}
		Assertions.assertFalse(rs.next(), "Incorrect number of rows retrieved");
		rs.close();

		con.rollback();
		con.close();
	}

	@Test
	@DisplayName("Test getObject method")
	void testGetObject() throws SQLException {
		Connection conn = createJDBCEmbeddedConnection();
		Statement stmt = conn.createStatement();
		int rows1 = stmt.executeUpdate("CREATE TABLE testgo (a boolean, b int, c string, d real, e double, f smallint, g bigint, h decimal);");
		Assertions.assertEquals(-2, rows1, "The creation should have affected no rows");
		int rows2 = stmt.executeUpdate("INSERT INTO testgo VALUES ('true', '122', 'ola', '1.22', '1.24', '-5000', '231', '-6500'), (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), ('false', '0', 'piña', '0', '0', '0', '0', '0');");
		Assertions.assertEquals(3, rows2, "The insertion should have affected 3 rows");

		DecimalFormat df = new DecimalFormat();
		df.setMaximumFractionDigits(3);
		df.setMinimumFractionDigits(3);

		Boolean[] column1 = new Boolean[]{true, null, false};
		Integer[] column2 = new Integer[]{122, null, 0};
		String[] column3 = new String[]{"ola", null, "piña"};
		Float[] column4 = new Float[]{1.22f, null, 0f};
		Double[] column5 = new Double[]{1.24d, null, 0d};
		Short[] column6 = new Short[]{-5000, null, 0};
		Long[] column7 = new Long[]{231L, null, 0L};
		BigDecimal[] column8 = new BigDecimal[]{new BigDecimal(-6500), null, BigDecimal.ZERO};

		ResultSet rs = stmt.executeQuery("SELECT * from testgo;");
		for(int i = 0 ; i < 3; i++) {
			rs.next();
			Assertions.assertEquals(column1[i], rs.getObject(1), "Problems in the JDBC result set");
			Assertions.assertEquals(column2[i], rs.getObject(2), "Problems in the JDBC result set");
			Assertions.assertEquals(column3[i], rs.getObject(3), "Problems in the JDBC result set");
			Assertions.assertEquals((i == 1) ? column1[i] : column1[i].toString(), rs.getString(1), "Problems in the JDBC result set");
			Assertions.assertEquals((i == 1) ? column2[i] : column2[i].toString(), rs.getString(2), "Problems in the JDBC result set");
			Assertions.assertEquals((i == 1) ? column3[i] : column3[i].toString(), rs.getString(3), "Problems in the JDBC result set");

			if(i == 1) {
				Assertions.assertEquals(column4[i], rs.getObject(4), "Problems in the JDBC result set");
				Assertions.assertEquals(column4[i], rs.getString(4), "Problems in the JDBC result set");
				Assertions.assertEquals(column5[i], rs.getObject(5), "Problems in the JDBC result set");
				Assertions.assertEquals(column5[i], rs.getString(5), "Problems in the JDBC result set");
			} else {
				Float f41 = (Float) rs.getObject(4);
				Float f42 = Float.parseFloat(rs.getString(4));
				Double f51 = (Double) rs.getObject(5);
				Double f52 = Double.parseDouble(rs.getString(5));
				if(Math.abs(f41 - column4[i]) > 0.1) {
					Assertions.fail("Problems in the JDBC result set");
				}
				if(Math.abs(f42 - column4[i]) > 0.1) {
					Assertions.fail("Problems in the JDBC result set");
				}
				if(Math.abs(f51 - column5[i]) > 0.1) {
					Assertions.fail("Problems in the JDBC result set");
				}
				if(Math.abs(f52 - column5[i]) > 0.1) {
					Assertions.fail("Problems in the JDBC result set");
				}
			}

			Assertions.assertEquals(column6[i], rs.getObject(6), "Problems in the JDBC result set");
			Assertions.assertEquals(column7[i], rs.getObject(7), "Problems in the JDBC result set");
			Assertions.assertEquals((i == 1) ? column6[i] : column6[i].toString(), rs.getString(6), "Problems in the JDBC result set");
			Assertions.assertEquals((i == 1) ? column7[i] : column7[i].toString(), rs.getString(7), "Problems in the JDBC result set");

			if(i == 1) {
				Assertions.assertEquals(column8[i], rs.getObject(8), "Problems in the JDBC result set");
				Assertions.assertEquals(column8[i], rs.getString(8), "Problems in the JDBC result set");
				Assertions.assertEquals(column8[i], rs.getBigDecimal(8), "Problems in the JDBC result set");
			} else {
				Assertions.assertEquals(df.format(column8[i]), df.format(rs.getObject(8)), "Problems in the JDBC result set");
				Assertions.assertEquals(df.format(column8[i]), df.format(new BigDecimal(rs.getString(8))), "Problems in the JDBC result set");
				Assertions.assertEquals(df.format(column8[i]), df.format(rs.getBigDecimal(8)), "Problems in the JDBC result set");
			}
		}
		rs.close();
		int rows5 = stmt.executeUpdate("DROP TABLE testgo;");
		Assertions.assertEquals(-2, rows5, "The deletion should have affected no rows");
		stmt.close();
		conn.close();
	}

	@Test
	@DisplayName("Test getObject method with dates")
	void testGetObjectTime() throws SQLException, ParseException {
		Connection conn = createJDBCEmbeddedConnection();
		Statement stmt = conn.createStatement();

		SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		SimpleDateFormat timeFormater = new SimpleDateFormat("HH:mm:ss", Locale.ENGLISH);
		SimpleDateFormat timestampFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);

		int rows1 = stmt.executeUpdate("CREATE TABLE testdates (a time, b date, c timestamp);");
		Assertions.assertEquals(-2, rows1, "The creation should have affected no rows");
		int rows2 = stmt.executeUpdate("INSERT INTO testdates VALUES ('23:10:46', '2015-01-01', '2017-06-30T00:02:44'), (NULL, NULL, NULL), ('10:10:46', '2019-12-12', '2001-05-01T21:02:00');");
		Assertions.assertEquals(3, rows2, "The insertion should have affected 3 rows");

		Time[] column1 = new Time[]{new Time(timeFormater.parse("23:10:46").getTime()),
				null, new Time(timeFormater.parse("10:10:46").getTime())};
		Date[] column2 = new Date[]{new Date(dateFormater.parse("2015-01-01").getTime()),
				null, new Date(dateFormater.parse("2019-12-12").getTime())};
		Timestamp[] column3 = new Timestamp[]{new Timestamp(timestampFormater.parse("2017-06-30 00:02:44").getTime()),
				null, new Timestamp(timestampFormater.parse("2001-05-01 21:02:00").getTime())};

		ResultSet rs = stmt.executeQuery("SELECT * from testdates;");
		for(int i = 0 ; i < 3; i++) {
			rs.next();
			if(i == 1) {
				Assertions.assertEquals(column1[i], rs.getTime(1), "Problems in the JDBC result set");
				Assertions.assertEquals(column1[i], rs.getObject(1), "Problems in the JDBC result set");
				Assertions.assertEquals(column1[i], rs.getString(1), "Problems in the JDBC result set");
				Assertions.assertEquals(column2[i], rs.getDate(2), "Problems in the JDBC result set");
				Assertions.assertEquals(column2[i], rs.getObject(2), "Problems in the JDBC result set");
				Assertions.assertEquals(column2[i], rs.getString(2), "Problems in the JDBC result set");
				Assertions.assertEquals(column3[i], rs.getTimestamp(3), "Problems in the JDBC result set");
				Assertions.assertEquals(column3[i], rs.getObject(3), "Problems in the JDBC result set");
				Assertions.assertEquals(column3[i], rs.getString(3), "Problems in the JDBC result set");
			} else {
				Assertions.assertEquals(column1[i].toString(), rs.getTime(1).toString(), "Problems in the JDBC result set");
				Assertions.assertEquals(column1[i].toString(), rs.getObject(1).toString(), "Problems in the JDBC result set");
				Assertions.assertEquals(column1[i].toString(), rs.getString(1), "Problems in the JDBC result set");
				Assertions.assertEquals(column2[i].toString(), rs.getDate(2).toString(), "Problems in the JDBC result set");
				Assertions.assertEquals(column2[i].toString(), rs.getObject(2).toString(), "Problems in the JDBC result set");
				Assertions.assertEquals(column2[i].toString(), rs.getString(2), "Problems in the JDBC result set");
				//TODO who dares to deal with timezones?
				//Assertions.assertEquals(column3[i].toString(), rs.getTimestamp(3).toString(), "Problems in the JDBC result set");
				//Assertions.assertEquals(column3[i].toString(), rs.getObject(3).toString(), "Problems in the JDBC result set");
				//Assertions.assertEquals(column3[i].toString(), rs.getString(3).toString(), "Problems in the JDBC result set");
			}
		}
		rs.close();
		int rows5 = stmt.executeUpdate("DROP TABLE testdates;");
		Assertions.assertEquals(-2, rows5, "The deletion should have affected no rows");
		stmt.close();
		conn.close();
	}
}
