package edu.washington.escience.myriad;

import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

public class TupleBatchBuffer {
  private final Schema schema;
  private final List<TupleBatch> readyTuples;
  private List<Column> currentColumns;
  private final int numColumns;
  private final BitSet columnsReady;
  private int numColumnsReady;
  private int currentNumTuples;

  protected TupleBatchBuffer(Schema schema) {
    this.schema = schema;
    readyTuples = new LinkedList<TupleBatch>();
    currentColumns = Column.allocateColumns(schema);
    numColumns = schema.numFields();
    columnsReady = new BitSet(numColumns);
    numColumnsReady = 0;
  }

  protected void put(int column, Object value) {
    if (columnsReady.get(column)) {
      throw new RuntimeException(
          "Need to fill up one row of TupleBatchBuffer before starting new one");
    }
    currentColumns.get(column).put(value);
    columnsReady.set(column, true);
    numColumnsReady++;
    if (numColumnsReady == numColumns) {
      currentNumTuples++;
      columnsReady.clear();
      if (currentNumTuples == TupleBatch.BATCH_SIZE) {
        finishBatch();
      }
    }
  }

  private void finishBatch() {
    readyTuples.add(new TupleBatch(schema, currentColumns, currentNumTuples));
    currentColumns = Column.allocateColumns(schema);
    currentNumTuples = 0;
  }
}
