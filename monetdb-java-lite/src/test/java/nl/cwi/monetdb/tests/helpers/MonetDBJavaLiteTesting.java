/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.tests.helpers;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedDatabase;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Bootstrapper class for the tests.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class MonetDBJavaLiteTesting {

	private static Path directoryPath = null;

	static {
		try {
			directoryPath = Files.createTempDirectory("monetdbtest");
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public static Path getDirectoryPath() {
		return directoryPath;
	}

	public static void startupDatabase(String directory) throws MonetDBEmbeddedException {
		MonetDBEmbeddedDatabase.startDatabase(directory, true, false);
		Assertions.assertTrue(MonetDBEmbeddedDatabase::isDatabaseRunning, "The database should be running!");
	}

	public static void shutdownDatabase() throws MonetDBEmbeddedException {
		MonetDBEmbeddedDatabase.stopDatabase();
		Assertions.assertFalse(MonetDBEmbeddedDatabase::isDatabaseRunning, "The database should be closed!");
	}
}
