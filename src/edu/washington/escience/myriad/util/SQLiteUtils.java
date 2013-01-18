package edu.washington.escience.myriad.util;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableList;

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
   * @param name the name of the table to be created.
   * @return a SQLite CREATE TABLE statement for a table of the given Schema and name.
   */
  public static String createStatementFromSchema(final Schema schema, final String name) {
    final StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ").append(name).append(" (");
    for (int i = 0; i < schema.numFields(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(schema.getFieldName(i)).append(" ").append(typeToSQLiteType(schema.getFieldType(i)));
    }
    sb.append(");");
    return sb.toString();
  }

  public static void insertIntoSQLite(final Schema inputSchema, final String tableName, final String dbFilePath,
      final TupleBatch data) {

    final ImmutableList<String> fieldNames = inputSchema.getFieldNames();
    final String[] placeHolders = new String[inputSchema.numFields()];
    for (int i = 0; i < inputSchema.numFields(); ++i) {
      placeHolders[i] = "?";
    }

    SQLiteAccessMethod.tupleBatchInsert(dbFilePath, "insert into " + tableName + " ( "
        + StringUtils.join(fieldNames, ',') + " ) values ( " + StringUtils.join(placeHolders, ',') + " )", data);
  }

  /**
   * Generates a SQLite INSERT statement for a table of the given Schema and name.
   * 
   * @param schema the Schema of the table to be created.
   * @param name the name of the table to be created.
   * @return a SQLite INSERT statement for a table of the given Schema and name.
   */
  public static String insertStatementFromSchema(final Schema schema, final String name) {
    final StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ").append(name).append(" (");
    sb.append(StringUtils.join(schema.getFieldNames(), ','));
    sb.append(") VALUES (");
    for (int i = 0; i < schema.numFields(); ++i) {
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

}
