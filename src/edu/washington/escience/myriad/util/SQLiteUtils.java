package edu.washington.escience.myriad.util;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.accessmethod.SQLiteAccessMethod;

/**
 * Util methods for SQLite.
 * */
public final class SQLiteUtils {
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
    System.out.println(sb.toString());
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
    System.out.println(sb.toString());
    return sb.toString();
  }

  /**
   * insert a TupleBatch into SQLite.
   * 
   * @param inputSchema the schema of the input TupleBatch
   * @param relationKey the relation to insert into
   * @param dbFilePath the location of the SQLite DB file
   * @param data the data.
   * @throws DbException if there is an error in the database.
   * */
  public static void insertIntoSQLite(final Schema inputSchema, final RelationKey relationKey, final String dbFilePath,
      final TupleBatch data) throws DbException {

    final List<String> fieldNames = inputSchema.getColumnNames();
    final String[] placeHolders = new String[inputSchema.numColumns()];
    for (int i = 0; i < inputSchema.numColumns(); ++i) {
      placeHolders[i] = "?";
    }

    SQLiteAccessMethod.tupleBatchInsert(dbFilePath, "insert into "
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
   * @param type a Myriad column type.
   * @return the name of the SQLite type that matches the given Myriad type.
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
    return "DROP TABLE IF EXISTS " + relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + ";";
  }
}
