package edu.washington.escience.myriad.parallel;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.accessmethod.SQLiteAccessMethod;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.table._TupleBatch;

// Not yet @ThreadSafe
public final class SQLiteTupleBatch implements _TupleBatch {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private final Schema inputSchema;
  private String databaseFilename;
  private int numInputTuples;
  private final String tableName;

  public SQLiteTupleBatch(final Schema inputSchema, final String databaseFilename, final String tableName) {
    this.inputSchema = Objects.requireNonNull(inputSchema);
    this.databaseFilename = databaseFilename;
    this.tableName = tableName;
  }

  @Override
  public synchronized _TupleBatch append(final _TupleBatch another) {
    insertIntoSQLite(inputSchema, tableName, databaseFilename, another);
    return this;
  }

  public static void insertIntoSQLite(final Schema inputSchema, String tableName, String dbFilePath,
      final _TupleBatch data) {

    final String[] fieldNames = inputSchema.getFieldNames();
    final String[] placeHolders = new String[inputSchema.numFields()];
    for (int i = 0; i < inputSchema.numFields(); ++i) {
      placeHolders[i] = "?";
    }

    SQLiteAccessMethod.tupleBatchInsert(dbFilePath, "insert into " + tableName + " ( "
        + StringUtils.join(fieldNames, ',') + " ) values ( " + StringUtils.join(placeHolders, ',') + " )",
        new TupleBatch(data.outputSchema(), data.outputRawData(), data.numOutputTuples()));
  }

  @Override
  public synchronized _TupleBatch distinct() {
    return null;
  }

  @Override
  public synchronized _TupleBatch except(final _TupleBatch another) {
    return null;
  }

  @Override
  public synchronized SQLiteTupleBatch filter(final int fieldIdx, final Predicate.Op op, final Object operand) {
    return this;
  }

  @Override
  public synchronized boolean getBoolean(final int column, final int row) {
    return false;
  }

  @Override
  public synchronized double getDouble(final int column, final int row) {
    return 0d;
  }

  @Override
  public synchronized float getFloat(final int column, final int row) {
    return 0f;
  }

  @Override
  public synchronized int getInt(final int column, final int row) {
    return 0;
  }

  @Override
  public synchronized long getLong(final int column, final int row) {
    return 0;
  }

  @Override
  public Object getObject(final int column, final int row) {
    return null;
  }

  @Override
  public synchronized String getString(final int column, final int row) {
    return null;
  }

  @Override
  public Set<Pair<Object, TupleBatchBuffer>> groupby(final int groupByColumn,
      final Map<Object, Pair<Object, TupleBatchBuffer>> buffers) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode(final int rowIndx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode(final int rowIndx, final int[] colIndx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Schema inputSchema() {
    return inputSchema;
  }

  @Override
  public synchronized int numInputTuples() {
    return numInputTuples;
  }

  @Override
  public synchronized int numOutputTuples() {
    return numInputTuples;
  }

  protected synchronized int[] outputColumnIndices() {
    final int numInputColumns = inputSchema.numFields();
    final int[] validC = new int[numInputColumns];
    int j = 0;
    for (int i = 0; i < numInputColumns; i++) {
      // operate on index i here
      validC[j++] = i;
    }
    return validC;
  }

  @Override
  public List<Column<?>> outputRawData() {
    return null;
  }

  @Override
  public synchronized Schema outputSchema() {

    final int[] columnIndices = outputColumnIndices();
    final String[] columnNames = new String[columnIndices.length];
    final Type[] columnTypes = new Type[columnIndices.length];
    int j = 0;
    for (final int columnIndx : columnIndices) {
      columnNames[j] = inputSchema.getFieldName(columnIndx);
      columnTypes[j] = inputSchema.getFieldType(columnIndx);
      j++;
    }

    return new Schema(columnTypes, columnNames);
  }

  public synchronized SQLiteTupleBatch[] partition(final PartitionFunction<?, ?> p) {
    return null;
  }

  @Override
  public TupleBatchBuffer[] partition(final PartitionFunction<?, ?> p, final TupleBatchBuffer[] buffers) {
    return null;
  }

  @Override
  public synchronized SQLiteTupleBatch project(final int[] remainingColumns) {
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
  public _TupleBatch remove(final int innerIdx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized _TupleBatch renameColumn(final int inputColumnIdx, final String newName) {
    return this;
  }

  public void reset(final String databaseFilename) {
    this.databaseFilename = databaseFilename;
  }

  @Override
  public synchronized _TupleBatch union(final _TupleBatch another) {
    return null;
  }
}
