package edu.washington.escience.myria;

import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import net.jcip.annotations.Immutable;

import com.almworks.sqlite4java.SQLiteConstants;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Schema describes the schema of a tuple.
 */
@Immutable
public final class Schema implements Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Converts a JDBC ResultSetMetaData object into a Schema.
   * 
   * @param rsmd the input ResultSetMetaData.
   * @return the output Schema.
   * @throws SQLException if JDBC throws a SQLException.
   */
  @Deprecated
  public static Schema fromResultSetMetaData(final ResultSetMetaData rsmd) throws SQLException {
    /* How many columns in this result set? */
    final int columnCount = rsmd.getColumnCount();

    /* Allocate space for the type and string arrays */
    final ImmutableList.Builder<Type> columnTypes = new ImmutableList.Builder<Type>();
    final ImmutableList.Builder<String> columnNames = new ImmutableList.Builder<String>();

    /* Fill them out */
    for (int i = 0; i < columnCount; ++i) {
      /* JDBC numbers columns from 1. Yes, really. */
      // Type
      final int type = rsmd.getColumnType(i + 1);
      switch (type) {
        case java.sql.Types.BOOLEAN:
          columnTypes.add(Type.BOOLEAN_TYPE);
          break;
        case java.sql.Types.FLOAT:
          columnTypes.add(Type.FLOAT_TYPE);
          break;
        case java.sql.Types.DOUBLE:
          columnTypes.add(Type.DOUBLE_TYPE);
          break;
        case java.sql.Types.INTEGER:
          columnTypes.add(Type.INT_TYPE);
          break;
        case java.sql.Types.BIGINT:
          /* Yes, really. http://dev.mysql.com/doc/refman/5.0/en/numeric-types.html#integer-types */
          columnTypes.add(Type.LONG_TYPE);
          break;
        case java.sql.Types.VARCHAR:
        case java.sql.Types.CHAR:
          columnTypes.add(Type.STRING_TYPE);
          break;
        case java.sql.Types.TIMESTAMP:
          columnTypes.add(Type.DATETIME_TYPE);
          break;
        default:
          throw new UnsupportedOperationException("JDBC type (java.SQL.Types) of " + type + " is not supported");
      }
      // Name
      columnNames.add(rsmd.getColumnName(i + 1));
    }

    return new Schema(columnTypes.build(), columnNames.build());
  }

  /**
   * Converts a SQLiteStatement object into a Schema.
   * 
   * @param statement the input SQLiteStatement (must have been stepped).
   * @return the output Schema.
   * @throws SQLiteException if SQLite throws an exception.
   */
  @Deprecated
  public static Schema fromSQLiteStatement(final SQLiteStatement statement) throws SQLiteException {
    assert (statement.hasStepped());

    /* How many columns in this result set? */
    final int columnCount = statement.columnCount();

    /* Allocate space for the type and string arrays */
    final ImmutableList.Builder<Type> columnTypes = new ImmutableList.Builder<Type>();
    final ImmutableList.Builder<String> columnNames = new ImmutableList.Builder<String>();

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
          columnTypes.add(Type.LONG_TYPE);
          break;
        case SQLiteConstants.SQLITE_TEXT:
          columnTypes.add(Type.STRING_TYPE);
          break;
        case SQLiteConstants.SQLITE_FLOAT:
          /* TODO Yes really, see above. */
          columnTypes.add(Type.DOUBLE_TYPE);
          break;
        default:
          throw new UnsupportedOperationException("SQLite type (SQLiteConstants) " + type + " is not supported");
      }
      // Name
      columnNames.add(statement.getColumnName(i));
    }

    return new Schema(columnTypes.build(), columnNames.build());
  }

  /**
   * Merge two Schemas into one. The result has the columns of the first concatenated with the columns of the second.
   * 
   * @param first The Schema with the first columns of the new Schema.
   * @param second The Schema with the last columns of the Schema.
   * @return the new Schema.
   */
  public static Schema mergeKeepDuplicateNames(final Schema first, final Schema second) {
    return new Schema(first, second);
  }

  /**
   * Merge two Schemas into one. The result has the columns of the first concatenated with the columns of the second.
   * <p>
   * Note that if there are duplicate column names from the two merging schemas, the duplicate columns from the first
   * schema will be automatically renamed by adding a suffix "_1", and the duplicate columns from the second schema will
   * be automatically renamed by adding a suffix "_2".
   * 
   * @param first The Schema with the first columns of the new Schema.
   * @param second The Schema with the last columns of the Schema.
   * @return the new Schema.
   */
  public static Schema merge(final Schema first, final Schema second) {
    final ImmutableList.Builder<Type> types = ImmutableList.builder();
    types.addAll(first.getColumnTypes()).addAll(second.getColumnTypes());

    List<String> names1 = new ArrayList<String>();
    names1.addAll(first.getColumnNames());
    List<String> names2 = new ArrayList<String>();
    names2.addAll(second.getColumnNames());
    for (int i = 0; i < names1.size(); ++i) {
      for (int j = 0; j < names2.size(); ++j) {
        if (names1.get(i).equals(names2.get(j))) {
          names1.set(i, names1.get(i) + "_1");
          names2.set(j, names2.get(j) + "_2");
          break;
        }
      }
    }
    final ImmutableList.Builder<String> names = ImmutableList.builder();
    names.addAll(names1).addAll(names2);

    return new Schema(types.build(), names.build());
  }

  /**
   * Static factory method.
   * 
   * @param types the types of columns in this Schema. It must contain at least one entry.
   * @param names the names of the columns. Note that names may be null.
   * @return a Schema representing the specified column types and names.
   */
  public static Schema of(final List<Type> types, final List<String> names) {
    return new Schema(types, names);
  }

  /** The types of the columns in this relation. */
  @JsonProperty
  private final List<Type> columnTypes;

  /** The names of the columns in this relation. */
  @JsonProperty
  private final List<String> columnNames;

  /**
   * Helper function to build a Schema from builders.
   * 
   * @param types the types of the columns in this Schema.
   * @param names the names of the columns in this Schema.
   */
  public Schema(final ImmutableList.Builder<Type> types, final ImmutableList.Builder<String> names) {
    this(types.build(), names.build());
  }

  /**
   * Create a Schema given an array of column types. Column names will be col0, col1, ....
   * 
   * @param types the types of the columns.
   */
  public Schema(final List<Type> types) {
    Objects.requireNonNull(types);
    columnTypes = ImmutableList.copyOf(types);
    final ImmutableList.Builder<String> names = ImmutableList.builder();
    for (int i = 0; i < types.size(); i++) {
      names.add("col" + i);
    }
    columnNames = names.build();
  }

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private Schema() {
    columnTypes = null;
    columnNames = null;
  }

  /**
   * Create a new Schema with typeAr.length columns with columns of the specified types, with associated named columns.
   * 
   * @param columnTypes array specifying the number of and types of columns in this Schema. It must contain at least one
   *          entry.
   * @param columnNames array specifying the names of the columns.
   */
  public Schema(final List<Type> columnTypes, final List<String> columnNames) {
    Objects.requireNonNull(columnTypes, "Column types cannot be null");
    Objects.requireNonNull(columnNames, "Column names cannot be null");
    if (columnTypes.size() != columnNames.size()) {
      throw new IllegalArgumentException("Invalid Schema: must have the same number of column types and column");
    }
    HashSet<String> uniqueNames = new HashSet<String>(columnNames);
    if (uniqueNames.size() != columnNames.size()) {
      throw new IllegalArgumentException("Invalid Schema: column names must be unique.");
    }
    this.columnTypes = ImmutableList.copyOf(columnTypes);
    this.columnNames = ImmutableList.copyOf(columnNames);
  }

  /**
   * Merge Schema. Schemas generated by operators such as join should be allowed to have duplicate names.
   * 
   * @param first first Schema
   * @param second second Schema
   * */
  private Schema(final Schema first, final Schema second) {
    final ImmutableList.Builder<Type> types = ImmutableList.builder();
    final ImmutableList.Builder<String> names = ImmutableList.builder();

    types.addAll(first.getColumnTypes()).addAll(second.getColumnTypes());
    names.addAll(first.getColumnNames()).addAll(second.getColumnNames());

    columnTypes = types.build();
    columnNames = names.build();
  }

  /**
   * Constructor. Create a new tuple desc with typeAr.length columns with columns of the specified types, with anonymous
   * (unnamed) columns.
   * 
   * @param typeAr array specifying the number of and types of columns in this Schema. It must contain at least one
   *          entry.
   */
  @Deprecated
  public Schema(final Type[] typeAr) {
    this(Arrays.asList(typeAr));
  }

  /**
   * Construct a Schema given a type array and column name arrays.
   * 
   * This function is deprecated as it requires a copy: arrays are mutable and Schema objects are immutable.
   * 
   * @param types the types of the columns in this Schema.
   * @param names the names of the columns in this Schema.
   */
  @Deprecated
  public Schema(final Type[] types, final String[] names) {
    this(Arrays.asList(types), Arrays.asList(names));
  }

  /**
   * Compares the specified object with this Schema for equality. Two Schemas are considered equal if they have the same
   * size, column types, and column names.
   * 
   * @param o the Object to be compared with.
   * @return true if schema is equal to this Schema.
   */
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Schema)) {
      return false;
    }
    final Schema other = (Schema) o;
    return (this == o) || columnTypes.equals(other.columnTypes) && columnNames.equals(other.columnNames);
  }

  /**
   * Find the index of the column with a given name.
   * 
   * @param name name of the column.
   * @return the index of the column that is first to have the given name.
   * @throws NoSuchElementException if no column with a matching name is found.
   */
  public int columnNameToIndex(final String name) {
    final int ret = columnNames.indexOf(name);
    if (ret == -1) {
      throw new NoSuchElementException("No column named " + name + " found");
    }
    return ret;
  }

  /**
   * Return a subset of the current schema.
   * 
   * @param index indices to be selected.
   * @return the subschema.
   */
  public Schema getSubSchema(final int[] index) {
    final ImmutableList.Builder<Type> types = ImmutableList.builder();
    final ImmutableList.Builder<String> names = ImmutableList.builder();
    for (int i : index) {
      Preconditions.checkElementIndex(i, numColumns());
      types.add(getColumnType(i));
      names.add(getColumnName(i));
    }
    return new Schema(types, names);
  }

  /**
   * Gets the (possibly null) column name of the ith column of this Schema.
   * 
   * @param index index of the column name to return. It must be a valid index.
   * @return the name of the ith column
   * @throws IndexOutOfBoundsException if index is negative or not less than numColumns.
   */
  public String getColumnName(final int index) {
    return columnNames.get(index);
  }

  /**
   * Returns a list containing the names of the columns in this Schema.
   * 
   * @return a list containing the names of the columns in this Schema.
   */
  public List<String> getColumnNames() {
    return columnNames;
  }

  /**
   * Gets the type of the ith column of this Schema.
   * 
   * @param index index of the column to get the type of. It must be a valid index.
   * @return the type of the ith column
   * @throws IndexOutOfBoundsException if index is negative or not less than numColumns.
   */
  public Type getColumnType(final int index) {
    return columnTypes.get(index);
  }

  /**
   * Returns an immutable list containing the types of the columns in this Schema.
   * 
   * @return an immutable list containing the types of the columns in this Schema.
   */
  public List<Type> getColumnTypes() {
    return columnTypes;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(new Object[] { columnNames, columnTypes });
  }

  /**
   * @return the number of columns in this Schema
   */
  public int numColumns() {
    return columnTypes.size();
  }

  /**
   * Returns a String describing this descriptor. It should be of the form
   * "columnType[0](columnName[0]), ..., columnType[M](columnName[M])", although the exact format does not matter.
   * 
   * @return String describing this descriptor.
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < columnTypes.size(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(columnNames.get(i)).append(" (").append(columnTypes.get(i)).append(')');
    }
    return sb.toString();
  }

  /**
   * The empty schema.
   * */
  public static final Schema EMPTY_SCHEMA = Schema.of(Arrays.asList(new Type[] {}), Arrays.asList(new String[] {}));

}
