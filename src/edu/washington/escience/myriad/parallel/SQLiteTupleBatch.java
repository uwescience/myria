package edu.washington.escience.myriad.parallel;

import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

// import edu.washington.escience.Predicate.Op;
// import edu.washington.escience.Schema.TDItem;
import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Predicate.Op;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myriad.accessmethod.SQLiteAccessMethod;
import edu.washington.escience.myriad.annotation.ThreadSafe;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.table._TupleBatch;

// Not yet @ThreadSafe
public class SQLiteTupleBatch implements _TupleBatch {

  private static final long serialVersionUID = 1L;

  public static final int BATCH_SIZE = 100;

  private final Schema inputSchema;
  // private final String[] outputColumnNames;
  // private final List<Column> inputColumns;
  // private int numInputTuples;
  // private final BitSet invalidTuples;
  // private final BitSet invalidColumns;
  private final ArrayList<String> filters;
  private final ArrayList<String> projects;
  private final String filepath;
  private int numInputTuples;
  // private final String driverClass;
  private final String tableName;

  // private final String username;
  // private final String password;

  public SQLiteTupleBatch(Schema inputSchema, String filepath, String tableName) {
    /* Take the input arguments directly */
    this.inputSchema = Objects.requireNonNull(inputSchema);
    this.filters = new ArrayList<String>();
    this.projects = new ArrayList<String>();
    this.filepath = filepath;
    this.tableName = tableName;
  }

  public synchronized SQLiteTupleBatch filter(int fieldIdx, Predicate.Op op, Object operand) {
    return this;
  }

  public synchronized boolean getBoolean(int column, int row) {
    // return ((BooleanColumn) inputColumns.get(column)).getBoolean(row);
    return false;
  }

  public synchronized double getDouble(int column, int row) {
    // return ((DoubleColumn) inputColumns.get(column)).getDouble(row);
    return 0d;
  }

  public synchronized float getFloat(int column, int row) {
    // return ((FloatColumn) inputColumns.get(column)).getFloat(row);
    return 0f;
  }

  public synchronized int getInt(int column, int row) {
    // return ((IntColumn) inputColumns.get(column)).getInt(row);
    return 0;
  }

  public Schema inputSchema() {
    return inputSchema;
  }

  public synchronized String getString(int column, int row) {
    // return ((StringColumn) inputColumns.get(column)).getString(row);
    return null;
  }

  @Override
  public synchronized int numInputTuples() {
    return numInputTuples;
  }

  public synchronized SQLiteTupleBatch[] partition(PartitionFunction<?, ?> p) {
    return null;
  }

  public synchronized SQLiteTupleBatch project(int[] remainingColumns) {
    return this;
  }

  protected synchronized int[] outputColumnIndices() {
    int numInputColumns = this.inputSchema.numFields();
    int[] validC = new int[numInputColumns];
    int j = 0;
    for (int i = 0; i < numInputColumns; i++) {
      // operate on index i here
      validC[j++] = i;
    }
    return validC;
  }

  public synchronized Schema outputSchema() {

    int[] columnIndices = this.outputColumnIndices();
    String[] columnNames = new String[columnIndices.length];
    Type[] columnTypes = new Type[columnIndices.length];
    int j = 0;
    for (int columnIndx : columnIndices) {
      columnNames[j] = this.inputSchema.getFieldName(columnIndx);
      columnTypes[j] = this.inputSchema.getFieldType(columnIndx);
      j++;
    }

    return new Schema(columnTypes, columnNames);
  }

  @Override
  public synchronized int numOutputTuples() {
    return this.numInputTuples;
  }

  @Override
  public synchronized _TupleBatch renameColumn(int inputColumnIdx, String newName) {
    return this;
  }

  @Override
  public synchronized _TupleBatch purgeFilters() {
    return this;
  }

  @Override
  public synchronized _TupleBatch purgeProjects() {
    return this;
  }

  @Override
  public synchronized _TupleBatch append(_TupleBatch another) {
    Iterator<Schema.TDItem> it = this.inputSchema.iterator();

    String[] fieldNames = new String[this.inputSchema.numFields()];
    String[] placeHolders = new String[this.inputSchema.numFields()];
    int i = 0;
    while (it.hasNext()) {
      Schema.TDItem item = it.next();
      placeHolders[i] = "?";
      fieldNames[i++] = item.getName();
    }

    SQLiteAccessMethod.tupleBatchInsert(this.filepath, "insert into " + this.tableName + " ( "
        + StringUtils.join(fieldNames, ',') + " ) values ( " + StringUtils.join(placeHolders, ',') + " )",
        new TupleBatch(another.outputSchema(), another.outputRawData(), another.numOutputTuples()));
    return this;
  }

  @Override
  public synchronized _TupleBatch union(_TupleBatch another) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized _TupleBatch intersect(_TupleBatch another) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized _TupleBatch except(_TupleBatch another) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized _TupleBatch distinct() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized _TupleBatch groupby() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized _TupleBatch orderby() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized _TupleBatch join(_TupleBatch other, Predicate p, _TupleBatch output) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Column> outputRawData() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public _TupleBatch[] partition(PartitionFunction<?, ?> p, _TupleBatch[] buffers) {
    // TODO Auto-generated method stub
    return null;
  }

}
