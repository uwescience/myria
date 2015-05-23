package edu.washington.escience.myria.util;

import java.io.File;
import java.io.IOException;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Type;

/**
 * Util methods for SQLite.
 * 
 * */
public final class SQLiteUtils {
  /**
   * Helper utility for creating SQLite CREATE TABLE statements.
   * 
   * @param type a Myria column type.
   * @return the name of the SQLite type that matches the given Myria type.
   */
  public static String typeToSQLiteType(final Type type) {
    switch (type) {
      case BOOLEAN_TYPE:
        return "BOOLEAN";
      case DOUBLE_TYPE:
        return "DOUBLE";
      case FLOAT_TYPE:
        return "DOUBLE";
      case INT_TYPE:
        return "INTEGER";
      case LONG_TYPE:
        return "INTEGER";
      case STRING_TYPE:
        return "TEXT";
      case DATETIME_TYPE:
        return "INTEGER"; // SQLLite does not support date type. use long to store the difference, measured in
                          // milliseconds, between the date time and
                          // midnight, January 1, 1970 UTC.

      default:
        throw new UnsupportedOperationException("Type " + type + " is not supported");
    }
  }

  /**
   * util classes are not instantiable.
   * */
  private SQLiteUtils() {
  }

  /**
   * Create a SQLite table. This method always creates an empty table, so if the table already exists, all its contents
   * will be removed.
   * 
   * @throws SQLiteException if SQLite error occur
   * @throws IOException if any IO error occur
   * @param dbFileAbsolutePath the SQLite file absolute path
   * @param relationKey the relation key to create
   * @param sqlSchemaString schema as a string
   * @param replaceExisting replace existing table with a new empty table
   * @param swipeData swipe existing data in the table.
   * */
  public static void createTable(final String dbFileAbsolutePath, final RelationKey relationKey,
      final String sqlSchemaString, final boolean replaceExisting, final boolean swipeData) throws IOException,
      SQLiteException {
    SQLiteConnection sqliteConnection = null;
    SQLiteStatement statement = null;
    try {
      final File f = new File(dbFileAbsolutePath);

      if (!f.getParentFile().exists()) {
        f.getParentFile().mkdirs();
      }

      /* Connect to the database */
      sqliteConnection = new SQLiteConnection(f);
      sqliteConnection.open(true);

      statement = sqliteConnection.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?");
      statement.bind(1, relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE));
      if (statement.step()) {
        // existing
        statement.dispose();
        if (replaceExisting) {
          statement = sqliteConnection.prepare("Drop table ? ");
          statement.bind(1, relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE));
          statement.step();
          statement.dispose();
        } else if (swipeData) {
          /* Clear table data in case it already exists */
          statement =
              sqliteConnection.prepare("delete from " + relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE));
          statement.step();
          statement.reset();
          return;
        } else {
          return;
        }
      }
      statement.dispose();
      /* Create the table if not exist */
      statement =
          sqliteConnection.prepare("create table " + relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + " ("
              + sqlSchemaString + ");");

      statement.step();
      statement.reset();

    } finally {
      if (statement != null) {
        statement.dispose();
      }
      if (sqliteConnection != null) {
        sqliteConnection.dispose();
      }
    }
  }

  /**
   * Deletes a relation in SQLite.
   * 
   * @throws SQLiteException if SQLite error occur
   * @param dbFileAbsolutePath the SQLite file absolute path
   * @param relationKey the relation key to delete
   * */
  public static void deleteTable(final String dbFileAbsolutePath, final RelationKey relationKey) throws IOException,
      SQLiteException {
    SQLiteConnection sqliteConnection = null;
    SQLiteStatement statement = null;
    try {
      final File f = new File(dbFileAbsolutePath);

      if (!f.getParentFile().exists()) {
        f.getParentFile().mkdirs();
      }

      /* Connect to the database */
      sqliteConnection = new SQLiteConnection(f);
      sqliteConnection.open(true);

      statement =
          sqliteConnection
              .prepare("drop table if exists " + relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE));
      statement.step();

    } finally {
      if (statement != null) {
        statement.dispose();
      }
      if (sqliteConnection != null) {
        sqliteConnection.dispose();
      }
    }
  }

  /**
   * Checks if a relation exists in the SQLite database
   * 
   * @throws SQLiteException if SQLite error occur
   * @param dbFileAbsolutePath the SQLite file absolute path
   * @param relationKey the relation key to check
   * */
  public static boolean existsTable(final String dbFileAbsolutePath, final RelationKey relationKey) throws IOException,
      SQLiteException {
    boolean exists = false;
    SQLiteConnection sqliteConnection = null;
    SQLiteStatement statement = null;
    try {
      final File f = new File(dbFileAbsolutePath);

      if (!f.getParentFile().exists()) {
        f.getParentFile().mkdirs();
      }

      /* Connect to the database */
      sqliteConnection = new SQLiteConnection(f);
      sqliteConnection.open(true);
      statement =
          sqliteConnection.prepare("SELECT * FROM sqlite_master WHERE type='table' AND name="
              + relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE));
      if (statement.step()) {
        return true;
      }

    } finally {
      if (statement != null) {
        statement.dispose();
      }
      if (sqliteConnection != null) {
        sqliteConnection.dispose();
      }
    }
    return exists;
  }
}
