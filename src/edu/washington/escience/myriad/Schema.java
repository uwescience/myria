package edu.washington.escience.myriad;

import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.almworks.sqlite4java.SQLiteConstants;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

/**
 * Schema describes the schema of a tuple.
 */
public final class Schema implements Serializable {

  /**
   * A helper class to facilitate organizing the information of each field.
   */
  private static class TDItem implements Serializable {

    /** Required for Serializable. */
    private static final long serialVersionUID = 1L;

    /** The type of the field. */
    private final Type fieldType;

    /** The name of the field. */
    private final String fieldName;

    /**
     * Creates a new field with the specified type and name.
     * 
     * @param t type of the created field.
     * @param n name of the created field.
     */
    public TDItem(final Type t, final String n) {
      this.fieldName = n;
      this.fieldType = t;
    }

    /**
     * Returns the type of this field.
     * 
     * @return the type of this field.
     */
    public Type getType() {
      return fieldType;
    }

    @Override
    public String toString() {
      return fieldName + "(" + fieldType + ")";
    }
  }

  /** Required for Serializable. */
  private static final long serialVersionUID = 1L;

  /**
   * Converts a JDBC ResultSetMetaData object into a SimpleDB Schema.
   * 
   * @param rsmd the input ResultSetMetaData.
   * @return the output Schema.
   * @throws SQLException if JDBC throws a SQLException.
   */
  public static Schema fromResultSetMetaData(final ResultSetMetaData rsmd) throws SQLException {
    /* How many columns in this result set? */
    int columnCount = rsmd.getColumnCount();

    /* Allocate space for the type and string arrays */
    Type[] columnTypes = new Type[columnCount];
    String[] columnNames = new String[columnCount];

    /* Fill them out */
    for (int i = 0; i < columnCount; ++i) {
      /* JDBC numbers columns from 1. Yes, really. */
      // Type
      int type = rsmd.getColumnType(i + 1);
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
          throw new UnsupportedOperationException("JDBC type (java.SQL.Types) of " + type
              + " is not supported");
      }
      // Name
      columnNames[i] = rsmd.getColumnName(i + 1);
    }

    return new Schema(columnTypes, columnNames);
  }

  /**
   * Converts a SQLiteStatement object into a SimpleDB Schema.
   * 
   * @param statement the input SQLiteStatement (must have been stepped).
   * @return the output Schema.
   * @throws SQLiteException if SQLite throws an exception.
   */
  public static Schema fromSQLiteStatement(final SQLiteStatement statement) throws SQLiteException {
    assert (statement.hasStepped());

    /* How many columns in this result set? */
    int columnCount = statement.columnCount();

    /* Allocate space for the type and string arrays */
    Type[] columnTypes = new Type[columnCount];
    String[] columnNames = new String[columnCount];

    /* Fill them out */
    for (int i = 0; i < columnCount; ++i) {
      // Type
      int type = statement.columnType(i);
      switch (type) {
        case SQLiteConstants.SQLITE_INTEGER:
          /*
           * TODO SQLite uses variable-width ints, so there's no way to tell. Default conservatively
           * to long. http://www.sqlite.org/datatype3.html
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
          throw new UnsupportedOperationException("SQLite type (SQLiteConstants) " + type
              + " is not supported");
      }
      // Name
      columnNames[i] = statement.getColumnName(i);
    }

    return new Schema(columnTypes, columnNames);
  }

  /**
   * Merge two Schemas into one, with td1.numFields + td2.numFields fields, with the first
   * td1.numFields coming from td1 and the remaining from td2.
   * 
   * @param td1 The Schema with the first fields of the new Schema
   * @param td2 The Schema with the last fields of the Schema
   * @return the new Schema
   */
  public static Schema merge(final Schema td1, final Schema td2) {
    Type[] types = new Type[td1.numFields() + td2.numFields()];
    String[] names = new String[types.length];

    for (int i = 0; i < td1.numFields(); i++) {
      types[i] = td1.getFieldType(i);
      names[i] = td1.getFieldName(i);
    }
    for (int i = 0; i < td2.numFields(); i++) {
      types[td1.numFields() + i] = td2.getFieldType(i);
      names[i + td1.numFields()] = td2.getFieldName(i);
    }
    return new Schema(types, names);
  }

  /** The column type/name pairs that define this Schema. */
  private final TDItem[] tdItems;

  /**
   * Constructor. Create a new tuple desc with typeAr.length fields with fields of the specified
   * types, with anonymous (unnamed) fields.
   * 
   * @param typeAr array specifying the number of and types of fields in this Schema. It must
   *          contain at least one entry.
   */
  public Schema(final Type[] typeAr) {
    tdItems = new TDItem[typeAr.length];
    for (int i = 0; i < typeAr.length; i++) {
      tdItems[i] = new TDItem(typeAr[i], "");
    }
  }

  /**
   * Create a new Schema with typeAr.length fields with fields of the specified types, with
   * associated named fields.
   * 
   * @param typeAr array specifying the number of and types of fields in this Schema. It must
   *          contain at least one entry.
   * @param fieldAr array specifying the names of the fields. Note that names may be null.
   */
  public Schema(final Type[] typeAr, final String[] fieldAr) {
    tdItems = new TDItem[typeAr.length];
    for (int i = 0; i < typeAr.length; i++) {
      tdItems[i] = new TDItem(typeAr[i], fieldAr[i]);
    }
  }

  /**
   * Compares the specified object with this Schema for equality. Two Schemas are considered equal
   * if they are the same size and if the n-th type in this Schema is equal to the n-th type in td.
   * 
   * @param o the Object to be compared for equality with this Schema.
   * @return true if the object is equal to this Schema.
   */
  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof Schema)) {
      return false;
    }
    Schema td = (Schema) o;

    if (this.tdItems.length != td.tdItems.length) {
      return false;
    }
    for (int i = 0; i < tdItems.length; i++) {
      if (!tdItems[i].fieldType.equals(td.tdItems[i].fieldType)) {
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
      if (tdItems[i].fieldName != null && tdItems[i].fieldName.equals(name)) {
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
    Preconditions.checkElementIndex(index, tdItems.length);
    return tdItems[index].fieldName;
  }

  /**
   * Gets the type of the ith field of this Schema.
   * 
   * @param index index of the field to get the type of. It must be a valid index.
   * @return the type of the ith field
   * @throws IndexOutOfBoundsException if index is negative or not less than numFields.
   */
  public Type getFieldType(final int index) {
    Preconditions.checkElementIndex(index, tdItems.length);
    return tdItems[index].fieldType;
  }

  /**
   * Returns an array containing the types of the columns in this Schema.
   * 
   * @return an array containing the types of the columns in this Schema.
   */
  public Type[] getTypes() {
    Type[] types = new Type[numFields()];
    for (int fieldIndex = 0; fieldIndex < numFields(); ++fieldIndex) {
      types[fieldIndex] = tdItems[fieldIndex].getType();
    }
    return types;
  }

  @Override
  public int hashCode() {
    // If you want to use Schema as keys for HashMap, implement this so
    // that equal objects have equals hashCode() results
    throw new UnsupportedOperationException("unimplemented");
  }

  /**
   * 
   * @return An iterator which iterates over all the field TDItems that are included in this Schema
   */
  public Iterator<TDItem> iterator() {
    return Arrays.asList(this.tdItems).iterator();
  }

  /**
   * @return the number of fields in this Schema
   */
  public int numFields() {
    return tdItems.length;
  }

  /**
   * Returns a String describing this descriptor. It should be of the form
   * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although the exact format does
   * not matter.
   * 
   * @return String describing this descriptor.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < tdItems.length; i++) {
      sb.append(tdItems[i]);
      sb.append(", ");
    }
    return sb.toString();
  }
}
