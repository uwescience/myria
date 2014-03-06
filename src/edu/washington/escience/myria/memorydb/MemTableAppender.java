package edu.washington.escience.myria.memorydb;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.TupleBatch;

/**
 * Handle class for appending data into a memory table.
 * */
public final class MemTableAppender implements AutoCloseable {
  /**
   * The destination table.
   * */
  private final MemoryTable tableToInsert;
  /**
   * Placeholder table.
   * 
   * @see {@link MemoryStore.PlaceholderTable}
   * */
  private final MemoryTable placeholder;
  /**
   * Destination table ID.
   * */
  private final RelationKey tableID;
  /**
   * Owner {@link MemoryStore}.
   * */
  private final MemoryStore ownerMemoryStore;
  /**
   * If the appender is closed.
   * */
  private boolean closed = false;

  /**
   * @param ownerMemoryStore Owner {@link MemoryStore}.
   * @param tableID destination table ID.
   * @param table the destination table
   * @param placeholder placeholder {@link MemoryTable}
   * */
  MemTableAppender(final MemoryStore ownerMemoryStore, final RelationKey tableID, final MemoryTable table,
      final MemoryTable placeholder) {
    tableToInsert = table;
    this.placeholder = placeholder;
    this.ownerMemoryStore = ownerMemoryStore;
    this.tableID = tableID;
  }

  /**
   * @param tb the TupleBatch to insert
   * */
  public void add(final TupleBatch tb) {
    if (!closed) {
      tableToInsert.add(tb);
    } else {
      throw new IllegalStateException("Closed");
    }
  }

  /**
   * @return Destination table
   * */
  MemoryTable getTable() {
    return tableToInsert;
  }

  /**
   * @return Placeholder table
   * */
  MemoryTable getPlaceholder() {
    return placeholder;
  }

  /**
   * @return Destination tableID.
   * */
  public RelationKey getRelation() {
    return tableID;
  }

  @Override
  public void close() throws java.util.ConcurrentModificationException {
    closed = true;
    ownerMemoryStore.completeInsert(this);
  }
}