package edu.washington.escience.myria.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.MyriaMatrix;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.PrefixColumn;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.proto.TransportProto.TransportMessage;
import edu.washington.escience.myria.util.IPCUtils;
import net.jcip.annotations.ThreadSafe;

/**
 * Container class for a batch of tuples. The goal is to amortize memory management overhead.
 * 
 */
@ThreadSafe
public class TupleBatch implements ReadableTable, Serializable {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The hard-coded number of tuples in a batch. */
  public static final int BATCH_SIZE = 10 * 1000;
  /** Schema of tuples in this batch. */
  private final Schema schema;
  /** Tuple data stored as columns in this batch. */
  private final ImmutableList<? extends Column<?>> columns;
  /** Number of tuples in this TB. */
  private final int numTuples;
  /** Whether this TB is an EOI TB. */
  private final boolean isEOI;

  /**
   * EOI TB constructor.
   * 
   * @param schema schema of the tuples in this batch.
   * @param isEoi whether this TupleBatch is an EOI TupleBatch.
   */
  private TupleBatch(final Schema schema, final boolean isEoi) {
    this.schema = schema;
    numTuples = 0;
    ImmutableList.Builder<Column<?>> b = ImmutableList.builder();
    for (Type type : schema.getColumnTypes()) {
      b.add(Column.emptyColumn(type));
    }
    columns = b.build();
    isEOI = isEoi;
  }

  /**
   * @param columnNames the new column names.
   * @return a shallow copy of the specified TupleBatch with the new column names.
   */
  public TupleBatch rename(final List<String> columnNames) {
    Schema newSchema =
        Schema.of(schema.getColumnTypes(), Objects.requireNonNull(columnNames, "columnNames"));
    return new TupleBatch(newSchema, columns, numTuples, isEOI);
  }

  /**
   * Standard immutable TupleBatch constructor. All fields must be populated before creation and
   * cannot be changed.
   * 
   * @param schema schema of the tuples in this batch. Must match columns.
   * @param columns contains the column-stored data. Must match schema.
   * @param numTuples the number of tuples in this TupleBatch.
   */
  public TupleBatch(final Schema schema, final List<? extends Column<?>> columns,
      final int numTuples) {
    this(schema, columns, numTuples, false);
  }

  /**
   * Constructor that gets the number of tuples from the columns.
   * 
   * @param schema schema of the tuples in this batch. Must match columns.
   * @param columns contains the column-stored data. Must match schema.
   */
  public TupleBatch(final Schema schema, final List<? extends Column<?>> columns) {
    this(schema, columns, columns.get(0).size());
  }

  /**
   * Construct a TupleBatch from the specified components.
   * 
   * @param schema schema of the tuples in this batch. Must match columns.
   * @param columns schema of the tuples in this batch. Must match columns.
   * @param numTuples the number of tuples in this batch. Must match columns.
   * @param isEOI whether this is an EOI TupleBatch.
   */
  public TupleBatch(final Schema schema, final List<? extends Column<?>> columns,
      final int numTuples, final boolean isEOI) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.columns = ImmutableList.copyOf(Objects.requireNonNull(columns, "columns"));
    Preconditions.checkArgument(columns.size() == schema.numColumns(),
        "Number of columns in data must equal the number of fields in schema");
    for (Column<?> column : columns) {
      Preconditions.checkArgument(numTuples == column.size(), "Column %s != %s tuples",
          column.size(), numTuples);
    }
    this.numTuples = numTuples;
    this.isEOI = isEOI;
  }

  /**
   * put the tuple batch into TBB by smashing it into cells and putting them one by one.
   * 
   * @param tbb the TBB buffer.
   */
  public final void compactInto(final TupleBatchBuffer tbb) {
    if (isEOI()) {
      /* an EOI TB has no data */
      tbb.appendTB(this);
      return;
    }
    final int numColumns = columns.size();
    for (int i = 0; i < numTuples; i++) {
      for (int column = 0; column < numColumns; column++) {
        tbb.put(column, columns.get(column), i);
      }
    }
  }

  /**
   * Return a new TupleBatch that contains only the filtered rows of the current dataset. Note that
   * if some of the tuples in this batch are invalid, we will have to map the indices in the
   * specified filter to the "real" indices in the tuple.
   * 
   * @param filter the rows to be retained.
   * @return a TupleBatch that contains only the filtered rows of the current dataset.
   */
  public final TupleBatch filter(final BitSet filter) {
    Preconditions.checkArgument(filter.length() <= numTuples(),
        "Error: trying to filter a TupleBatch of length %s with a filter of length %s", numTuples(),
        filter.length());
    int newNumTuples = filter.cardinality();

    /*
     * Shortcut: the filter is full, so all current tuples are retained. Just return this.
     */
    if (newNumTuples == numTuples) {
      return this;
    }

    ImmutableList.Builder<Column<?>> newColumns = ImmutableList.builder();
    for (Column<?> column : columns) {
      newColumns.add(column.filter(filter));
    }
    return new TupleBatch(schema, newColumns.build(), newNumTuples, isEOI);
  }


  /**
   * Return a new TupleBatch that contains only first <code>prefix</code> rows of this batch.
   * 
   * @param prefix the number of rows in the prefix to be retained.
   * @return a TupleBatch that contains only the filtered rows of the current dataset.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public final TupleBatch prefix(final int prefix) {
    Preconditions.checkArgument(prefix <= numTuples(),
        "Error: cannot take a prefix of length %s from a batch of length %s", prefix, numTuples());
    ImmutableList.Builder<Column<?>> newColumns = ImmutableList.builder();
    for (Column<?> column : columns) {
      newColumns.add(new PrefixColumn(column, prefix));
    }
    return new TupleBatch(schema, newColumns.build(), prefix, isEOI);
  }

  @Override
  public final boolean getBoolean(final int column, final int row) {
    return columns.get(column).getBoolean(row);
  }

  @Override
  public final double getDouble(final int column, final int row) {
    return columns.get(column).getDouble(row);
  }

  @Override
  public final float getFloat(final int column, final int row) {
    return columns.get(column).getFloat(row);
  }

  @Override
  public final int getInt(final int column, final int row) {
    return columns.get(column).getInt(row);
  }

  @Override
  public final long getLong(final int column, final int row) {
    Preconditions.checkArgument(columns.get(column).size() >= numTuples,
        "numTuples %s columnsize %s", numTuples, columns.get(column).size());
    return columns.get(column).getLong(row);
  }

  @Override
  @Deprecated
  public final Object getObject(final int column, final int row) {
    return columns.get(column).getObject(row);
  }

  @Override
  public final Schema getSchema() {
    return schema;
  }

  @Override
  public final String getString(final int column, final int row) {
    return columns.get(column).getString(row);
  }

  @Override
  public final MyriaMatrix getMyriaMatrix(final int column, final int row) {
    return columns.get(column).getMyriaMatrix(row);
  }

  @Override
  public final DateTime getDateTime(final int column, final int row) {
    return columns.get(column).getDateTime(row);
  }

  @Override
  public final int numColumns() {
    return schema.numColumns();
  }

  @Override
  public final int numTuples() {
    return numTuples;
  }

  /**
   * Partition this TB using the partition function. The method is implemented by shallow copy of
   * TupleBatches.
   * 
   * @return an array of TBs. The length of the array is the same as the number of partitions. If no
   *         tuple presents in a partition, say the i'th partition, the i'th element in the result
   *         array is null.
   * @param pf the partition function.
   */
  public final TupleBatch[] partition(final PartitionFunction pf) {
    TupleBatch[] result = new TupleBatch[pf.numPartition()];
    if (isEOI) {
      Arrays.fill(result, this);
      return result;
    }

    final int[] partitions = pf.partition(this);

    BitSet[] resultBitSet = new BitSet[result.length];
    for (int i = 0; i < partitions.length; i++) {
      int p = partitions[i];
      Preconditions.checkElementIndex(p, result.length);
      if (resultBitSet[p] == null) {
        resultBitSet[p] = new BitSet(result.length);
      }
      resultBitSet[p].set(i);
    }

    for (int i = 0; i < result.length; i++) {
      if (resultBitSet[i] != null) {
        result[i] = filter(resultBitSet[i]);
      }
    }
    return result;
  }

  /**
   * Creates a new TupleBatch with only the indicated columns.
   * 
   * Internal implementation of a (non-duplicate-eliminating) PROJECT statement.
   * 
   * @param remainingColumns zero-indexed array of columns to retain.
   * @param resultSchema computing a schema every time is usually not necessary
   * @return a projected TupleBatch.
   */
  public final TupleBatch selectColumns(final int[] remainingColumns, final Schema resultSchema) {
    Objects.requireNonNull(remainingColumns);
    final ImmutableList.Builder<Column<?>> newColumns = new ImmutableList.Builder<Column<?>>();
    for (final int i : remainingColumns) {
      newColumns.add(columns.get(i));
    }
    return new TupleBatch(resultSchema, newColumns.build(), numTuples, isEOI);
  }

  /**
   * @param rows a BitSet flagging the rows to be removed.
   * @return a new TB with the specified rows removed.
   */
  public final TupleBatch filterOut(final BitSet rows) {
    BitSet inverted = (BitSet) rows.clone();
    inverted.flip(0, numTuples);
    return filter(inverted);
  }

  @Override
  public final String toString() {
    if (isEOI) {
      return "EOI";
    }
    final List<Type> columnTypes = schema.getColumnTypes();
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numTuples; i++) {
      sb.append("|\t");
      for (int j = 0; j < schema.numColumns(); j++) {
        sb.append(columnTypes.get(j).toString(columns.get(j), i));
        sb.append("\t|\t");
      }
      sb.append('\n');
    }
    return sb.toString();
  }

  /**
   * @return the data columns.
   */
  public final ImmutableList<? extends Column<?>> getDataColumns() {
    return columns;
  }

  /**
   * @return a TransportMessage encoding the TupleBatch.
   */
  public final TransportMessage toTransportMessage() {
    return IPCUtils.normalDataMessage(columns, numTuples);
  }

  /**
   * Create an EOI TupleBatch.
   * 
   * @param schema schema.
   * @return EOI TB for the schema.
   */
  public static final TupleBatch eoiTupleBatch(final Schema schema) {
    return new TupleBatch(schema, true);
  }

  /**
   * @return if the TupleBatch is an EOI.
   */
  public final boolean isEOI() {
    return isEOI;
  }

  /**
   * Construct a new TupleBatch that equals the current batch with the specified column appended.
   * The number of valid tuples in this batch must be the same as the size of the other batch. If
   * this batch is not dense, then
   * 
   * @param columnName the name of the column to be added.
   * @param column the column to be added.
   * @return a new TupleBatch containing the tuples of this column plus the tuples of the other.
   */
  public TupleBatch appendColumn(final String columnName, final Column<?> column) {
    Preconditions.checkArgument(numTuples() == column.size(),
        "Cannot append column of size %s to batch of size %s", column.size(), numTuples());
    Schema newSchema = Schema.appendColumn(schema, column.getType(), columnName);
    List<Column<?>> newColumns =
        ImmutableList.<Column<?>>builder().addAll(columns).add(column).build();
    return new TupleBatch(newSchema, newColumns, numTuples, isEOI);
  }

  @Override
  public ReadableColumn asColumn(final int column) {
    return columns.get(column);
  }
}
