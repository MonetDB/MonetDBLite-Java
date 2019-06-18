/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.env;

import nl.cwi.monetdb.embedded.jdbc.JDBCEmbeddedConnection;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An embedded version of a MonetDB database.
 * Communication between Java and native C is done via JNI. The MonetDB's JNI library must be successfully loaded in
 * order to the other methods work.
 * <br>
 * <strong>Note</strong>: You can have only one Embedded MonetDB database running per JVM process.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public final class MonetDBEmbeddedDatabase {

	/** The MonetDBEmbeddedDatabase instance as only one database is allowed per JVM process. */
	private static MonetDBEmbeddedDatabase monetDBEmbeddedDatabase = null;

	/**
	 * The hook thread to be called when the JVM shuts down.
	 */
	private static class MonetDBEmbeddedHook extends Thread {
		@Override
		public void run() {
			try {
				MonetDBEmbeddedDatabase.stopDatabase();
			} catch (Exception e) {}
		}
	}

	/**  A hook to be called once the JVM shuts down */
	private static final MonetDBEmbeddedHook MonetDBHook;

	/**  The hook's thread ID */
	private static final long MonetDBHookID;

	/**  Check if the database is closed or not. */
	private static volatile boolean isClosed = true;

	/**  A ReadWriteLock to avoid racing conditions. */
	private static final ReentrantReadWriteLock locker;

	static {
		locker = new ReentrantReadWriteLock();
		MonetDBHook = new MonetDBEmbeddedHook();
		MonetDBHookID = MonetDBHook.getId();
		Runtime.getRuntime().addShutdownHook(MonetDBHook);
	}

	/**
	 * Check if the database is still running or not.
	 *
	 * @return A boolean indicating if the database is running
	 */
	public static boolean isDatabaseRunning() {
		locker.readLock().lock();
		boolean res = !isClosed;
		locker.readLock().unlock();
		return res;
	}

	/**
	 * Check if the database is running in-memory.
	 *
	 * @throws MonetDBEmbeddedException If the database is not running
	 * @return A boolean indicating if the database is running in-memory
	 */
	public static boolean isDatabaseRunningInMemory() throws MonetDBEmbeddedException {
		locker.readLock().lock();
		try {
			if(monetDBEmbeddedDatabase == null) {
				throw new MonetDBEmbeddedException("The MonetDB Embedded database is not running");
			}
			boolean res = monetDBEmbeddedDatabase.databaseDirectory == null;
			locker.readLock().unlock();
			return res;
		} catch (Exception ex) {
			locker.readLock().unlock();
			throw ex;
		}
	}

	/**
	 * Starts a MonetDB database on the given farm with debugging parameters if desired. This method should be used by
	 * experts only :)
	 *
	 * @param dbDirectory The full path of the farm
	 * @param silentFlag A boolean if silent mode will be turned on or not
	 * @param sequentialFlag A boolean indicating if the sequential pipeline will be set or not
	 * @throws MonetDBEmbeddedException If the JNI library has not been loaded yet or an error in the database occurred
	 */
	public static void startDatabase(String dbDirectory, boolean silentFlag, boolean sequentialFlag)
			throws MonetDBEmbeddedException {
		if (dbDirectory != null && (dbDirectory.trim().isEmpty() || dbDirectory.equals(":memory:")))
			dbDirectory = null;
		locker.writeLock().lock();
		try {
			if(monetDBEmbeddedDatabase != null) {
				throw new MonetDBEmbeddedException("The MonetDB Embedded database is still running");
			} else {
				MonetDBJavaLiteLoader.loadMonetDBJavaLite();
				monetDBEmbeddedDatabase = startDatabaseInternal(dbDirectory, silentFlag, sequentialFlag);
				isClosed = false;
			}
			locker.writeLock().unlock();
		} catch (Exception ex) {
			locker.writeLock().unlock();
			throw ex;
		}
	}

	/**
	 * Starts a MonetDB database on the given farm.
	 *
	 * @param dbDirectory The full path of the farm
	 * @throws MonetDBEmbeddedException If the JNI library has not been loaded yet or an error in the database occurred
	 */
	public static void startDatabase(String dbDirectory) throws MonetDBEmbeddedException {
		MonetDBEmbeddedDatabase.startDatabase(dbDirectory, true, false);
	}

	/**
	 * Gets the database's farm directory.
	 *
	 * @throws MonetDBEmbeddedException If the database is not running
	 * @return A String representing the database's farm directory
	 */
	public static String getDatabaseDirectory() throws MonetDBEmbeddedException {
		locker.readLock().lock();
		try {
			if(monetDBEmbeddedDatabase == null) {
				throw new MonetDBEmbeddedException("The MonetDB Embedded database is not running");
			}
			String res = monetDBEmbeddedDatabase.databaseDirectory;
			locker.readLock().unlock();
			return res;
		} catch (Exception ex) {
			locker.readLock().unlock();
			throw ex;
		}
	}

	/**
	 * Gets the current total number of connections to the database.
	 *
	 * @throws MonetDBEmbeddedException If the database is not running
	 * @return The total number of connections to the database
	 */
	public static int getNumberOfConnections() throws MonetDBEmbeddedException {
		locker.readLock().lock();
		try {
			if(monetDBEmbeddedDatabase == null) {
				throw new MonetDBEmbeddedException("The MonetDB Embedded database is not running");
			}
			int res = monetDBEmbeddedDatabase.connections.size();
			locker.readLock().unlock();
			return res;
		} catch (Exception ex) {
			locker.readLock().unlock();
			throw ex;
		}
	}

	/**
	 * Checks if the Silent Flag was set while creating the database.
	 *
	 * @throws MonetDBEmbeddedException If the database is not running
	 * @return The Silent Flag
	 */
	public static boolean isSilentFlagSet() throws MonetDBEmbeddedException {
		locker.readLock().lock();
		try {
			if(monetDBEmbeddedDatabase == null) {
				throw new MonetDBEmbeddedException("The MonetDB Embedded database is not running");
			}
			boolean res = monetDBEmbeddedDatabase.silentFlag;
			locker.readLock().unlock();
			return res;
		} catch (Exception ex) {
			locker.readLock().unlock();
			throw ex;
		}
	}

	/**
	 * Checks if the Sequential Flag was set while creating the database.
	 *
	 * @throws MonetDBEmbeddedException If the database is not running
	 * @return The Sequential Flag
	 */
	public static boolean isSequentialFlagSet() throws MonetDBEmbeddedException {
		locker.readLock().lock();
		try {
			if(monetDBEmbeddedDatabase == null) {
				throw new MonetDBEmbeddedException("The MonetDB Embedded database is not running");
			}
			boolean res = monetDBEmbeddedDatabase.sequentialFlag;
			locker.readLock().unlock();
			return res;
		} catch (Exception ex) {
			locker.readLock().unlock();
			throw ex;
		}
	}

	/**
	 * Stops the database. All the pending connections will be shut down as well.
	 *
	 * @throws MonetDBEmbeddedException If the database is not running or an error in the database occurred
	 */
	public static void stopDatabase() throws MonetDBEmbeddedException {
		long currentID = Thread.currentThread().getId();
		if(currentID != MonetDBHookID) {
			locker.writeLock().lock();
		}
		try {
			if(monetDBEmbeddedDatabase == null) {
				throw new MonetDBEmbeddedException("The MonetDB Embedded database is not running");
			} else {
				for(MonetDBEmbeddedConnection mdbec : monetDBEmbeddedDatabase.connections.values()) {
					if(!mdbec.isClosed()) {
						mdbec.closeConnectionImplementation();
					}
				}
				monetDBEmbeddedDatabase.connections.clear();
				monetDBEmbeddedDatabase.stopDatabaseInternal();
				monetDBEmbeddedDatabase = null;
				isClosed = true;
			}
			if(currentID != MonetDBHookID) {
				locker.writeLock().unlock();
			}
		} catch (Exception ex) {
			if(currentID != MonetDBHookID) {
				locker.writeLock().unlock();
			}
			throw ex;
		}
	}

	/**
	 * Creates a connection on the database, set on the default schema.
	 *
	 * @return A MonetDBEmbeddedConnection instance
	 * @throws MonetDBEmbeddedException If the database is not running or an error in the database occurred
	 */
	public static MonetDBEmbeddedConnection createConnection() throws MonetDBEmbeddedException {
		locker.writeLock().lock();
		try {
			if(monetDBEmbeddedDatabase == null) {
				throw new MonetDBEmbeddedException("The MonetDB Embedded database is not running");
			} else {
				MonetDBEmbeddedConnection con = monetDBEmbeddedDatabase.createConnectionInternal();
				monetDBEmbeddedDatabase.connections.put(con.getRandomIdentifier(), con);
				locker.writeLock().unlock();
				return con;
			}
		} catch (Exception ex) {
			locker.writeLock().unlock();
			throw ex;
		}
	}

	/**
	 * Creates a JDBC embedded connection in the directory.
	 *
	 * @param directory The full path of the farm
	 * @param silentFlag A boolean if silent mode will be turned on or not
	 * @param sequentialFlag A boolean indicating if the sequential pipeline will be set or not
	 * @return A JDBCEmbeddedConnection instance
	 * @throws MonetDBEmbeddedException If the database is not running or an error in the database occurred
	 */
	public static JDBCEmbeddedConnection createJDBCEmbeddedConnection(String directory, boolean silentFlag,
																	  boolean sequentialFlag)
			throws MonetDBEmbeddedException {
		if (directory != null && (directory.trim().isEmpty() || directory.equals(":memory:")))
			directory = null;
		locker.writeLock().lock();
		try {
			if(monetDBEmbeddedDatabase != null) { //the database if running, it must be always in the same directory
				if(monetDBEmbeddedDatabase.databaseDirectory == null && directory != null) {
					throw new MonetDBEmbeddedException("The embedded database is already running in memory");
				} else if(monetDBEmbeddedDatabase.databaseDirectory != null && directory == null) {
					throw new MonetDBEmbeddedException("The embedded database is already running in a directory");
				} else if(monetDBEmbeddedDatabase.databaseDirectory != null &&
						!monetDBEmbeddedDatabase.databaseDirectory.equals(directory)) {
					throw new MonetDBEmbeddedException("The embedded database is already running in a different directory");
				} else if(monetDBEmbeddedDatabase.silentFlag != silentFlag) {
					throw new MonetDBEmbeddedException("The embedded database is already running with a different silentFlag");
				} else if(monetDBEmbeddedDatabase.sequentialFlag != sequentialFlag) {
					throw new MonetDBEmbeddedException("The embedded database is already running with a different optimizer pipeline");
				}
			}
			if(monetDBEmbeddedDatabase == null) { //don't start the database again on a new JDBC connection
				MonetDBJavaLiteLoader.loadMonetDBJavaLite();
				monetDBEmbeddedDatabase = startDatabaseInternal(directory, silentFlag, sequentialFlag);
			}
			JDBCEmbeddedConnection con = monetDBEmbeddedDatabase.createJDBCEmbeddedConnectionInternal();
			monetDBEmbeddedDatabase.connections.put(con.getRandomIdentifier(), con);
			locker.writeLock().unlock();
			return con;
		} catch (Exception ex) {
			locker.writeLock().unlock();
			throw ex;
		}
	}

	/**
	 * Creates a JDBC embedded connection in the directory.
	 *
	 * @param directory The full path of the farm
	 * @return A JDBCEmbeddedConnection instance
	 * @throws MonetDBEmbeddedException If the database is not running or an error in the database occurred
	 */
	public static JDBCEmbeddedConnection createJDBCEmbeddedConnection(String directory)
			throws MonetDBEmbeddedException {
		return createJDBCEmbeddedConnection(directory, true, false);
	}

	/**
	 * Removes a connection from the database.
	 *
	 * @param con The connection to remove
	 * @param toShutDown If true, if there are no more connections in the database after the removal, the database is
	 *                   shut down.
	 * @throws MonetDBEmbeddedException If the database is not running or an error in the database occurred
	 */
	public static void removeConnection(MonetDBEmbeddedConnection con, boolean toShutDown)
			throws MonetDBEmbeddedException {
		locker.writeLock().lock();
		try {
			monetDBEmbeddedDatabase.connections.remove(con.getRandomIdentifier());
			if(toShutDown && monetDBEmbeddedDatabase.connections.isEmpty()) {
				monetDBEmbeddedDatabase.stopDatabaseInternal();
				monetDBEmbeddedDatabase = null;
				isClosed = true;
			}
			locker.writeLock().unlock();
		} catch (Exception ex) {
			locker.writeLock().unlock();
			throw ex;
		}
	}

	/** The database's farm directory */
	private final String databaseDirectory;

	/** If the silent flag is set */
	private final boolean silentFlag;

	/** If the sequential flag is set */
	private final boolean sequentialFlag;

	/** Hash table of embedded connections. */
	private final HashMap<Long, MonetDBEmbeddedConnection> connections = new HashMap<>();

	private MonetDBEmbeddedDatabase(String dbDirectory, boolean silentFlag, boolean sequentialFlag) {
		this.databaseDirectory = dbDirectory;
		this.silentFlag = silentFlag;
		this.sequentialFlag = sequentialFlag;
	}

	/**
	 * Internal implementation to start a database.
	 */
	private static native MonetDBEmbeddedDatabase startDatabaseInternal(String dbDirectory, boolean silentFlag,
																		boolean sequentialFlag)
			throws MonetDBEmbeddedException;

	/**
	 * Internal implementation to stop a database.
	 */
	private native void stopDatabaseInternal() throws MonetDBEmbeddedException;

	/**
	 * Internal implementation to create a connection on this database.
	 */
	private native MonetDBEmbeddedConnection createConnectionInternal() throws MonetDBEmbeddedException;

	/**
	 * Internal implementation to create a JDBC embeddded connection on this database.
	 */
	private native JDBCEmbeddedConnection createJDBCEmbeddedConnectionInternal() throws MonetDBEmbeddedException;
}
