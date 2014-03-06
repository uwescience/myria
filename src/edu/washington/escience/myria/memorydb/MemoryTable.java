package edu.washington.escience.myria.memorydb;

import java.util.Iterator;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.proto.DataProto.DataMessage;

/**
 * A memory table. It stores {@link DataMessage} instead of {@link TupleBatch} to minimize java object memory overhead.
 * 
 * The design here is for single thread data insertion. The semantic of concurrent data insertion is undefined.
 * */
@NotThreadSafe
public abstract class MemoryTable implements Iterable<TupleBatch> {

  /**
   * @param owner the owner {@link MemoryStore}
   * @param schema the table schema.
   * */
  public MemoryTable(final MemoryStore owner, final Schema schema) {
    this.schema = schema;
    this.owner = owner;
    numTuples = 0;
  }

  /**
   * The schema of the destination table.
   * */
  private final Schema schema;

  /**
   * Owner memory store.
   * */
  private final MemoryStore owner;

  /**
   * Number of inserted tuples.
   * */
  private long numTuples = 0;

  /**
   * @return num tuples.
   * */
  public final long getNumTuples() {
    return numTuples;
  }

  /**
   * @return A {@link TupleBatch} iterator over the whole memory table.
   * */
  @Override
  public abstract Iterator<TupleBatch> iterator();

  /**
   * @return the schema of this memory table.
   * */
  public Schema getSchema() {
    return schema;
  }

  /**
   * @return the schema of this memory table.
   * */
  public MemoryStore getOwner() {
    return owner;
  }

  /**
   * Append a TupleBatch at the end of the table.
   * 
   * @param tb data TupleBatch
   * */
  public final void add(final TupleBatch tb) {
    if (tb == null) {
      return;
    }
    if (tb.isEOI()) {
      return;
    }
    Preconditions.checkArgument(tb.getSchema().equals(getSchema()));
    addInner(tb);
    numTuples += tb.numTuples();
  }

  /**
   * Append a TupleBatch at the end of the table.
   * 
   * @param tb data TupleBatch. It is guaranteed to be non-null and not EOI.
   * */
  protected abstract void addInner(@Nonnull final TupleBatch tb);

}
