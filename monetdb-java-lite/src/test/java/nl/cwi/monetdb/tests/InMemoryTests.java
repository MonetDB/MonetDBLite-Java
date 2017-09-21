package nl.cwi.monetdb.tests;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedConnection;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedDatabase;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;
import nl.cwi.monetdb.embedded.resultset.QueryResultSet;
import nl.cwi.monetdb.tests.helpers.MonetDBJavaLiteTesting;
import org.junit.jupiter.api.*;

import java.sql.*;

/**
 * In memory tests for MonetDBLite.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class InMemoryTests extends MonetDBJavaLiteTesting {

	static {
		try {
			Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		} catch (ClassNotFoundException e) { }
	}

	private static MonetDBEmbeddedConnection connection;

	@BeforeAll
	@DisplayName("Startup the database in-memory")
	static void startupDatabase() throws MonetDBEmbeddedException {
		MonetDBJavaLiteTesting.startupDatabase(null);
		connection = MonetDBEmbeddedDatabase.createConnection();
	}

	@Test
	@DisplayName("Is the database running in-memory")
	void testRunningMemory() throws MonetDBEmbeddedException {
		boolean inMemory = MonetDBEmbeddedDatabase.isDatabaseRunningInMemory();
		Assertions.assertTrue(inMemory);
	}

	@Test
	@DisplayName("Testing a table creation, update, select and delete in-memory")
	void testTableLife() throws MonetDBEmbeddedException {
		int rows1 = connection.executeUpdate("CREATE TABLE tableMem (aaa int, bbb varchar(32));");
		Assertions.assertEquals(-2, rows1, "The creation should have affected no rows!");

		int rows2 = connection.executeUpdate("INSERT INTO tableMem VALUES (1, 'a'), (2, 'b'), (3, 'c')");
		Assertions.assertEquals(3, rows2, "The insertion should have affected 3 rows!");

		QueryResultSet qrs = connection.executeQuery("SELECT aaa, bbb FROM tableMem;");
		int numberOfRows = qrs.getNumberOfRows(), numberOfColumns = qrs.getNumberOfColumns();
		Assertions.assertEquals(3, numberOfRows, "The number of rows should be 3, got " + numberOfRows + " instead!");
		Assertions.assertEquals(2, numberOfColumns, "The number of columns should be 2, got " + numberOfColumns + " instead!");

		int[] array1 = new int[3];
		qrs.getIntColumnByIndex(1, array1);
		Assertions.assertArrayEquals(new int[]{1, 2, 3}, array1, "Integers not correctly retrieved!");

		String[] array2 = new String[3];
		qrs.getStringColumnByName("bbb", array2);
		Assertions.assertArrayEquals(new String[]{"a", "b", "c"}, array2, "Strings not correctly retrieved!");
		qrs.close();

		int rows3 = connection.executeUpdate("DROP TABLE tableMem;");
		Assertions.assertEquals(-2, rows3, "The deletion should have affected no rows!");
	}

	@Test
	@DisplayName("Testing transactions in-memory")
	void testTransactions() throws MonetDBEmbeddedException {
		connection.executeUpdate("CREATE TABLE tableTrans (anint int);");

		connection.startTransaction();
		connection.executeUpdate("INSERT INTO tableTrans VALUES (1);");
		connection.rollback();

		connection.startTransaction();
		connection.executeUpdate("INSERT INTO tableTrans VALUES (2);");
		connection.commit();

		QueryResultSet qrs = connection.executeQuery("SELECT anint FROM tableTrans;");
		int numberOfRows = qrs.getNumberOfRows();
		Assertions.assertEquals(1, numberOfRows, "The number of rows should be 1, got " + numberOfRows + " instead!");
		int retrieved = qrs.getIntegerByColumnIndexAndRow(1, 1);
		Assertions.assertEquals(2, retrieved, "The value should be 2, got " + retrieved + " instead!");

		connection.executeUpdate("DROP TABLE tableTrans;");
	}

	@Test
	@DisplayName("Testing JDBC statements in-memory")
	void testJDBCInMemory() throws SQLException {
		Connection jdbcConnection = DriverManager.getConnection("jdbc:monetdb:embedded::memory:");
		//allow another JDBC connection
		Connection jdbcAnother = DriverManager.getConnection("jdbc:monetdb:embedded:");
		jdbcAnother.close();

		Statement stmt = jdbcConnection.createStatement();
		int rows1 = stmt.executeUpdate("CREATE TABLE jdbcMem (abc int);");
		Assertions.assertEquals(-2, rows1, "The creation should have affected no rows!");

		int rows2 = stmt.executeUpdate("INSERT INTO jdbcMem VALUES (1), (2), (3);");
		Assertions.assertEquals(3, rows2, "The creation should have affected 3 rows!");

		ResultSet rs = stmt.executeQuery("SELECT count(*) from jdbcMem;");
		rs.next();
		Assertions.assertEquals(3, rs.getShort(1), "Problems in the JDBC result set!");
		rs.close();

		int rows3 = stmt.executeUpdate("DROP TABLE jdbcMem;");
		Assertions.assertEquals(-2, rows3, "The deletion should have affected no rows!");
		stmt.close();
		jdbcConnection.close();
	}

	@AfterAll
	@DisplayName("Shutdown the database in-memory")
	static void shutdownDatabaseInMemory() throws MonetDBEmbeddedException {
		MonetDBJavaLiteTesting.shutdownDatabase();
		Assertions.assertFalse(MonetDBEmbeddedDatabase::isDatabaseRunning, "The database should be closed!");
		Assertions.assertTrue(connection::isClosed, "The connection should be closed!");

		//If the database is closed, then the connection will close as well
		Assertions.assertThrows(MonetDBEmbeddedException.class, () -> connection.executeQuery("SELECT 2;"));
		//Stop the database again also shouldn't work
		Assertions.assertThrows(MonetDBEmbeddedException.class, MonetDBEmbeddedDatabase::stopDatabase);
	}
}
