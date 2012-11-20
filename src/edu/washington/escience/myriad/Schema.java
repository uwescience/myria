package edu.washington.escience.myriad;

import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Objects;

import com.almworks.sqlite4java.SQLiteConstants;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

/**
 * Schema describes the schema of a tuple.
 */
public final class Schema implements Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The types of the fields in this relation. */
  private final Type[] fieldTypes;
  /** The names of the fields in this relation. */
  private final String[] fieldNames;

  /**
   * Converts a JDBC ResultSetMetaData object into a Schema.
   * 
   * @param rsmd the input ResultSetMetaData.
   * @return the output Schema.
   * @throws SQLException if JDBC throws a SQLException.
   */
  public static Schema fromResultSetMetaData(final ResultSetMetaData rsmd) throws SQLException {
    /* How many columns in this result set? */
    final int columnCount = rsmd.getColumnCount();

    /* Allocate space for the type and string arrays */
    final Type[] columnTypes = new Type[columnCount];
    final String[] columnNames = new String[columnCount];

    /* Fill them out */
    for (int i = 0; i < columnCount; ++i) {
      /* JDBC numbers columns from 1. Yes, really. */
      // Type
      final int type = rsmd.getColumnType(i + 1);
      switch (type) {
        case java.sql.Types.BOOLEAN:
          columnTypes[i] = Type.BOOLEAN_TYPE;
          break;
        case java.sql.Types.FLOAT:
          columnTypes[i] = Type.FLOAT_TYPE;
          break;
        case java.sql.Types.DOUBLE:
          columnTypes[i] = Type.DOUBLE_TYPE;
          break;
        case java.sql.Types.INTEGER:
          columnTypes[i] = Type.INT_TYPE;
          break;
        case java.sql.Types.BIGINT:
          /* Yes, really. http://dev.mysql.com/doc/refman/5.0/en/numeric-types.html#integer-types */
          columnTypes[i] = Type.LONG_TYPE;
          break;
        case java.sql.Types.VARCHAR:
        case java.sql.Types.CHAR:
          columnTypes[i] = Type.STRING_TYPE;
          break;
        default:
          throw new UnsupportedOperationException("JDBC type (java.SQL.Types) of " + type + " is not supported");
      }
      // Name
      columnNames[i] = rsmd.getColumnName(i + 1);
    }

    return new Schema(columnTypes, columnNames);
  }

  /**
   * Converts a SQLiteStatement object into a Schema.
   * 
   * @param statement the input SQLiteStatement (must have been stepped).
   * @return the output Schema.
   * @throws SQLiteException if SQLite throws an exception.
   */
  public static Schema fromSQLiteStatement(final SQLiteStatement statement) throws SQLiteException {
    assert (statement.hasStepped());

    /* How many columns in this result set? */
    final int columnCount = statement.columnCount();

    /* Allocate space for the type and string arrays */
    final Type[] columnTypes = new Type[columnCount];
    final String[] columnNames = new String[columnCount];

    /* Fill them out */
    for (int i = 0; i < columnCount; ++i) {
      // Type
      final int type = statement.columnType(i);
      switch (type) {
        case SQLiteConstants.SQLITE_INTEGER:
          /*
           * TODO SQLite uses variable-width ints, so there's no way to tell. Default conservatively to long.
           * http://www.sqlite.org/datatype3.html
           */
          columnTypes[i] = Type.LONG_TYPE;
          break;
        case SQLiteConstants.SQLITE_TEXT:
          columnTypes[i] = Type.STRING_TYPE;
          break;
        case SQLiteConstants.SQLITE_FLOAT:
          /* TODO Yes really, see above. */
          columnTypes[i] = Type.DOUBLE_TYPE;
          break;
        default:
          throw new UnsupportedOperationException("SQLite type (SQLiteConstants) " + type + " is not supported");
      }
      // Name
      columnNames[i] = statement.getColumnName(i);
    }

    return new Schema(columnTypes, columnNames);
  }

  /**
   * Merge two Schemas into one. The result has the fields of the first concatenated with the fields of the second.
   * 
   * @param first The Schema with the first fields of the new Schema.
   * @param second The Schema with the last fields of the Schema.
   * @return the new Schema.
   */
  public static Schema merge(final Schema first, final Schema second) {
    final Type[] types = new Type[first.numFields() + second.numFields()];
    final String[] names = new String[types.length];

    for (int i = 0; i < first.numFields(); i++) {
      types[i] = first.getFieldType(i);
      names[i] = first.getFieldName(i);
    }
    for (int i = 0; i < second.numFields(); i++) {
      types[first.numFields() + i] = second.getFieldType(i);
      names[i + first.numFields()] = second.getFieldName(i);
    }
    return new Schema(types, names);
  }

  /**
   * Constructor. Create a new tuple desc with typeAr.length fields with fields of the specified types, with anonymous
   * (unnamed) fields.
   * 
   * @param typeAr array specifying the number of and types of fields in this Schema. It must contain at least one
   *          entry.
   */
  public Schema(final Type[] typeAr) {
    Objects.requireNonNull(typeAr);
    fieldTypes = Arrays.copyOf(typeAr, typeAr.length);
    fieldNames = new String[typeAr.length];
    for (int i = 0; i < typeAr.length; i++) {
      fieldNames[i] = "col" + i;
    }
  }

  /**
   * Create a new Schema with typeAr.length fields with fields of the specified types, with associated named fields.
   * 
   * @param typeAr array specifying the number of and types of fields in this Schema. It must contain at least one
   *          entry.
   * @param fieldAr array specifying the names of the fields. Note that names may be null.
   */
  public Schema(final Type[] typeAr, final String[] fieldAr) {
    Objects.requireNonNull(typeAr);
    Objects.requireNonNull(fieldAr);
    if (typeAr.length != fieldAr.length) {
      throw new IllegalArgumentException("Invalid Schema: must have the same number of field types and field");
    }
    fieldTypes = Arrays.copyOf(typeAr, typeAr.length);
    fieldNames = Arrays.copyOf(fieldAr, fieldAr.length);
  }

  /**
   * Compares the specified object with this Schema for equality. Two Schemas are considered equal if they are the same
   * size and if the n-th type in this Schema is equal to the n-th type in the other.
   * 
   * @param o the Object to be compared for equality with this Schema.
   * @return true if the object is equal to this Schema.
   */
  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof Schema)) {
      return false;
    }
    final Schema other = (Schema) o;

    if (fieldTypes.length != other.fieldTypes.length) {
      return false;
    }
    for (int i = 0; i < fieldTypes.length; i++) {
      if (!fieldTypes[i].equals(other.fieldTypes[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Find the index of the field with a given name.
   * 
   * @param name name of the field.
   * @return the index of the field that is first to have the given name.
   * @throws NoSuchElementException if no field with a matching name is found.
   */
  public int fieldNameToIndex(final String name) {
    for (int i = 0; i < numFields(); i++) {
      if (fieldNames[i] != null && fieldNames[i].equals(name)) {
        return i;
      }
    }
    throw new NoSuchElementException("No field named " + name + " found");
  }

  /**
   * Gets the (possibly null) field name of the ith field of this Schema.
   * 
   * @param index index of the field name to return. It must be a valid index.
   * @return the name of the ith field
   * @throws IndexOutOfBoundsException if index is negative or not less than numFields.
   */
  public String getFieldName(final int index) {
    Preconditions.checkElementIndex(index, fieldNames.length);
    return fieldNames[index];
  }

  /**
   * Gets the type of the ith field of this Schema.
   * 
   * @param index index of the field to get the type of. It must be a valid index.
   * @return the type of the ith field
   * @throws IndexOutOfBoundsException if index is negative or not less than numFields.
   */
  public Type getFieldType(final int index) {
    Preconditions.checkElementIndex(index, fieldTypes.length);
    return fieldTypes[index];
  }

  /**
   * Returns an array containing the types of the columns in this Schema.
   * 
   * @return an array containing the types of the columns in this Schema.
   */
  public Type[] getTypes() {
    return Arrays.copyOf(fieldTypes, fieldTypes.length);
  }

  /**
   * Returns an array containing the names of the columns in this Schema.
   * 
   * @return an array containing the names of the columns in this Schema.
   */
  public String[] getFieldNames() {
    return Arrays.copyOf(fieldNames, fieldNames.length);
  }

  @Override
  public int hashCode() {
    // If you want to use Schema as keys for HashMap, implement this so
    // that equal objects have equals hashCode() results
    throw new UnsupportedOperationException("unimplemented");
  }

  /**
   * @return the number of fields in this Schema
   */
  public int numFields() {
    return fieldTypes.length;
  }

  /**
   * Returns a String describing this descriptor. It should be of the form
   * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although the exact format does not matter.
   * 
   * @return String describing this descriptor.
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fieldTypes.length; ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(fieldNames[i]).append(" (").append(fieldTypes[i]).append(")");
    }
    return sb.toString();
  }
}
