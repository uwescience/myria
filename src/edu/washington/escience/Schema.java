package edu.washington.escience;

import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.almworks.sqlite4java.SQLiteConstants;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

/**
 * Schema describes the schema of a tuple.
 */
public class Schema implements Serializable {

  /**
   * A help class to facilitate organizing the information of each field
   * */
  public static class TDItem implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The type of the field
     * */
    public final Type fieldType;

    /**
     * The name of the field
     * */
    public final String fieldName;

    public TDItem(Type t, String n) {
      this.fieldName = n;
      this.fieldType = t;
    }

    public Type getType() {
      return fieldType;
    }

    @Override
    public String toString() {
      return fieldName + "(" + fieldType + ")";
    }
  }

  private static final long serialVersionUID = 1L;

  /**
   * Converts a JDBC ResultSetMetaData object into a SimpleDB Schema.
   * 
   * @param rsmd the input ResultSetMetaData
   * @return the output Schema
   * @throws SQLException
   */
  public static Schema fromResultSetMetaData(ResultSetMetaData rsmd) throws SQLException {
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

      if (type == java.sql.Types.INTEGER)
        columnTypes[i] = Type.INT_TYPE;
      else if (type == java.sql.Types.VARCHAR || type == java.sql.Types.CHAR)
        columnTypes[i] = Type.STRING_TYPE;
      else {
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
   * @param statement the input SQLiteStatement (must have been stepped)
   * @return the output Schema
   * @throws SQLiteException
   */
  public static Schema fromSQLiteStatement(SQLiteStatement statement) throws SQLiteException {
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

      if (type == SQLiteConstants.SQLITE_INTEGER)
        columnTypes[i] = Type.INT_TYPE;
      else if (type == SQLiteConstants.SQLITE_TEXT)
        columnTypes[i] = Type.STRING_TYPE;
      else {
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
  public static Schema merge(Schema td1, Schema td2) {
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

  private final TDItem[] tdItems;

  /**
   * Constructor. Create a new tuple desc with typeAr.length fields with fields of the specified
   * types, with anonymous (unnamed) fields.
   * 
   * @param typeAr array specifying the number of and types of fields in this Schema. It must
   *          contain at least one entry.
   */
  public Schema(Type[] typeAr) {
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
  public Schema(Type[] typeAr, String[] fieldAr) {
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
  public boolean equals(Object o) {
    if (!(o instanceof Schema))
      return false;
    Schema td = (Schema) o;

    if (this.tdItems.length != td.tdItems.length)
      return false;
    for (int i = 0; i < tdItems.length; i++) {
      if (!tdItems[i].fieldType.equals(td.tdItems[i].fieldType))
        return false;
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
  public int fieldNameToIndex(String name) throws NoSuchElementException {
    for (int i = 0; i < numFields(); i++) {
      if (tdItems[i].fieldName != null && tdItems[i].fieldName.equals(name))
        return i;
    }
    throw new NoSuchElementException("No field named " + name + " found");
  }

  /**
   * Gets the (possibly null) field name of the ith field of this Schema.
   * 
   * @param i index of the field name to return. It must be a valid index.
   * @return the name of the ith field
   * @throws NoSuchElementException if i is not a valid field reference.
   */
  public String getFieldName(int i) throws NoSuchElementException {
    return tdItems[i].fieldName;
  }

  /**
   * Gets the type of the ith field of this Schema.
   * 
   * @param i The index of the field to get the type of. It must be a valid index.
   * @return the type of the ith field
   * @throws NoSuchElementException if i is not a valid field reference.
   */
  public Type getFieldType(int i) throws NoSuchElementException {
    return tdItems[i].fieldType;
  }

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
   * @return An iterator which iterates over all the field TDItems that are included in this Schema
   * */
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
