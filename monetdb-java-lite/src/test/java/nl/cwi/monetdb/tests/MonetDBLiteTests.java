/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.tests;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedConnection;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedDatabase;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;
import nl.cwi.monetdb.embedded.resultset.QueryResultSet;
import nl.cwi.monetdb.tests.helpers.MonetDBJavaLiteTesting;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Testing the MonetDBJavaLite environment
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class MonetDBLiteTests extends MonetDBJavaLiteTesting {

	private static MonetDBEmbeddedConnection connection;

	@BeforeAll
	@DisplayName("Startup the database")
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
			Assertions.fail("The MonetDBEmbeddedException should be thrown!");
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
		int stress = 30;
		List<Thread> otherStressers = new ArrayList<>(stress);

		for (int i = 0; i < stress; i++) {
			Thread t = new Thread(() -> {
				try {
					MonetDBEmbeddedConnection con = MonetDBEmbeddedDatabase.createConnection();
					QueryResultSet rs = con.executeQuery("SELECT * from tables;");
					rs.close();
					con.close();
				} catch (MonetDBEmbeddedException e) {
					Assertions.fail(e.getMessage());
				}
			});
			t.start();
			otherStressers.add(t);
		}

		for (Thread t : otherStressers) {
			t.join();
		}
	}

	@Test
	@DisplayName("Empty result sets")
	void testEmptyResulSets() throws MonetDBEmbeddedException {
		MonetDBEmbeddedConnection con = MonetDBEmbeddedDatabase.createConnection();
		QueryResultSet qrs = con.executeQuery("SELECT id from types WHERE 1=0;");
		Assertions.assertThrows(ArrayIndexOutOfBoundsException.class, () -> qrs.getIntegerByColumnIndexAndRow(1, 1));
		int numberOfRows = qrs.getNumberOfRows();
		Assertions.assertEquals(0, numberOfRows, "The number of rows should be 0, got " + numberOfRows + " instead!");
		qrs.close();
		con.close();
	}

	@Test
	@DisplayName("SELECT NULL")
	void selectNull() throws MonetDBEmbeddedException {
		MonetDBEmbeddedConnection con = MonetDBEmbeddedDatabase.createConnection();
		QueryResultSet qrs = con.executeQuery("SELECT NULL AS stresser;");
		int numberOfRows = qrs.getNumberOfRows(), numberOfColumns = qrs.getNumberOfColumns();
		Assertions.assertEquals(1, numberOfRows, "The number of rows should be 1, got " + numberOfRows + " instead!");
		Assertions.assertEquals(1, numberOfColumns, "The number of columns should be 1, got " + numberOfColumns + " instead!");
		qrs.close();
		con.close();
	}

	@AfterAll
	@DisplayName("Shutdown database at the end")
	static void shutDatabase() throws MonetDBEmbeddedException, IOException {
		MonetDBJavaLiteTesting.shutdownDatabase();
		Assertions.assertFalse(MonetDBEmbeddedDatabase::isDatabaseRunning, "The database should be closed!");
		Assertions.assertTrue(connection::isClosed, "The connection should be closed!");

		//If the database is closed, then the connection will close as well
		Assertions.assertThrows(MonetDBEmbeddedException.class, () -> connection.executeQuery("SELECT 1;"));
		//Stop the database again also shouldn't work
		Assertions.assertThrows(MonetDBEmbeddedException.class, MonetDBEmbeddedDatabase::stopDatabase);

		//start again the database in another directory and stop it
		Path otherPath = Files.createTempDirectory("monetdbtestother");
		MonetDBEmbeddedDatabase.startDatabase(otherPath.toString(), true, false);
		MonetDBEmbeddedDatabase.createConnection();
		MonetDBEmbeddedDatabase.stopDatabase();
	}
}
