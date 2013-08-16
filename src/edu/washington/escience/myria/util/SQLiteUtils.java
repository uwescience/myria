package edu.washington.escience.myria.util;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.accessmethod.SQLiteAccessMethod;
import edu.washington.escience.myria.accessmethod.SQLiteInfo;

/**
 * Util methods for SQLite.
 * */
public final class SQLiteUtils {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SQLiteUtils.class);

  /**
   * Generates a SQLite CREATE TABLE statement for a table of the given Schema and name.
   * 
   * @param schema the Schema of the table to be created.
   * @param relationKey the name of the table to be created.
   * @return a SQLite CREATE TABLE statement for a table of the given Schema and name.
   */
  public static String createStatementFromSchema(final Schema schema, final RelationKey relationKey) {
    final StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ").append(relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE)).append(" (");
    for (int i = 0; i < schema.numColumns(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append('[').append(schema.getColumnName(i)).append("] ").append(typeToSQLiteType(schema.getColumnType(i)));
    }
    sb.append(");");
    return sb.toString();
  }

  /**
   * Generates a SQLite CREATE TABLE statement for a table of the given Schema and name.
   * 
   * @param schema the Schema of the table to be created.
   * @param relationKey the table to be created.
   * @return a SQLite CREATE TABLE statement for a table of the given Schema and name.
   */
  public static String createIfNotExistsStatementFromSchema(final Schema schema, final RelationKey relationKey) {
    final StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ").append(relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE)).append(
        " (\n");
    for (int i = 0; i < schema.numColumns(); ++i) {
      if (i > 0) {
        sb.append(",\n");
      }
      sb.append("    [").append(schema.getColumnName(i)).append("] ").append(typeToSQLiteType(schema.getColumnType(i)));
    }
    sb.append(");");
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Create if not exists: " + sb.toString());
    }
    return sb.toString();
  }

  /**
   * insert a TupleBatch into SQLite.
   * 
   * @param inputSchema the schema of the input TupleBatch
   * @param relationKey the relation to insert into
   * @param sqliteInfo the database connection information
   * @param data the data.
   * @throws DbException if there is an error in the database.
   * */
  public static void insertIntoSQLite(final Schema inputSchema, final RelationKey relationKey,
      final SQLiteInfo sqliteInfo, final TupleBatch data) throws DbException {

    final List<String> fieldNames = inputSchema.getColumnNames();
    final String[] placeHolders = new String[inputSchema.numColumns()];
    for (int i = 0; i < inputSchema.numColumns(); ++i) {
      placeHolders[i] = "?";
    }

    SQLiteAccessMethod.tupleBatchInsert(sqliteInfo, "insert into "
        + relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + " ([" + StringUtils.join(fieldNames, "],[")
        + "] ) values ( " + StringUtils.join(placeHolders, ',') + " )", data);
  }

  /**
   * Generates a SQLite INSERT statement for a table of the given Schema and name.
   * 
   * @param schema the Schema of the table to be created.
   * @param relationKey the key of the table to be created.
   * @return a SQLite INSERT statement for a table of the given Schema and name.
   */
  public static String insertStatementFromSchema(final Schema schema, final RelationKey relationKey) {
    final StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ").append(relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE)).append(" ([");
    sb.append(StringUtils.join(schema.getColumnNames(), "],["));
    sb.append("]) VALUES (");
    for (int i = 0; i < schema.numColumns(); ++i) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append('?');
    }
    sb.append(");");
    return sb.toString();
  }

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
   * Creates a SQLite "DROP TABLE IF EXISTS" statement.
   * 
   * @param relationKey the table to be dropped.
   * @return "DROP TABLE IF EXISTS <tt>relationKey.getCanonicalName()</tt>;"
   */
  public static String dropTableIfExistsStatement(final RelationKey relationKey) {
    StringBuilder sb = new StringBuilder();
    sb.append("DROP TABLE IF EXISTS ").append(relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE)).append(';');
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Drop if exists: " + sb.toString());
    }
    return sb.toString();
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
}
