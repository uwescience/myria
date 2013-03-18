package edu.washington.escience.myriad.util;

import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;

/**
 * Util methods for JDBC.
 * */
public final class JdbcUtils {
  /**
   * Generates a JDBC CREATE TABLE statement for a table of the given Schema and name.
   * 
   * @param schema the Schema of the table to be created.
   * @param relationKey the name of the table to be created.
   * @param dbms the DBMS, e.g., "mysql".
   * @return a JDBC CREATE TABLE statement for a table of the given Schema and name.
   */
  public static String createStatementFromSchema(final Schema schema, final RelationKey relationKey, final String dbms) {
    final StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ").append(relationKey.toString(dbms)).append(" (");
    for (int i = 0; i < schema.numColumns(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(schema.getColumnName(i)).append(" ").append(typeToDbmsType(schema.getColumnType(i), dbms));
    }
    sb.append(");");
    return sb.toString();
  }

  /**
   * Generates a JDBC CREATE TABLE statement for a table of the given Schema and name.
   * 
   * @param schema the Schema of the table to be created.
   * @param relationKey the table to be created.
   * @param dbms the DBMS, e.g., "mysql".
   * @return a JDBC CREATE TABLE statement for a table of the given Schema and name.
   * @throws UnsupportedOperationException if the DBMS does not support this operation.
   */
  public static String createIfNotExistsStatementFromSchema(final Schema schema, final RelationKey relationKey,
      final String dbms) throws UnsupportedOperationException {
    Objects.requireNonNull(schema);
    Objects.requireNonNull(relationKey);
    Objects.requireNonNull(dbms);
    /* See if the DBMS supports IF NOT EXISTS statements. */
    if (!dbms.equalsIgnoreCase("mysql")) {
      throw new UnsupportedOperationException(dbms + " does not support CREATE TABLE IF NOT EXISTS");
    }
    final StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ").append(relationKey).append(" (\n");
    for (int i = 0; i < schema.numColumns(); ++i) {
      if (i > 0) {
        sb.append(",\n");
      }
      sb.append("    ").append(schema.getColumnName(i)).append(" ").append(
          typeToDbmsType(schema.getColumnType(i), dbms));
    }
    sb.append(");");
    return sb.toString();
  }

  /**
   * Generates a JDBC INSERT statement for a table of the given Schema and name.
   * 
   * @param schema the Schema of the table to be created.
   * @param relationKey the key of the table to be created.
   * @param dbms the DBMS, e.g., "mysql".
   * @return a JDBC INSERT statement for a table of the given Schema and name.
   */
  public static String insertStatementFromSchema(final Schema schema, final RelationKey relationKey, final String dbms) {
    Objects.requireNonNull(schema);
    Objects.requireNonNull(relationKey);
    final StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ").append(relationKey.toString(dbms)).append(" (");
    sb.append(StringUtils.join(schema.getColumnNames(), ','));
    sb.append(") VALUES (");
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
   * Helper utility for creating JDBC CREATE TABLE statements.
   * 
   * @param type a Myriad column type.
   * @param dbms the description of the DBMS, e.g., "mysql".
   * @return the name of the DBMS type that matches the given Myriad type.
   */
  public static String typeToDbmsType(final Type type, final String dbms) {
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
  private JdbcUtils() {
  }

  /**
   * Creates a JDBC "DROP TABLE IF EXISTS" statement.
   * 
   * @param relationKey the table to be dropped.
   * @return "DROP TABLE IF EXISTS <tt>relationKey.getCanonicalName()</tt>;"
   */
  public static String dropTableIfExistsStatement(final RelationKey relationKey) {
    return "DROP TABLE IF EXISTS " + relationKey + ";";
  }
}
