package edu.washington.escience.myriad.operator;

import java.util.Queue;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;

/**
 * Transparently export the data from child to a {@link Queue<TupleBatch>}.
 * 
 * Do not use a {@link TupleBatchBuffer} here because {@link TupleBatchBuffer} is not thread safe.
 * */
public class TBQueueExporter extends Operator {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  /**
   * The queue to store the TupleBatches.
   * */
  private final Queue<TupleBatch> queueStore;

  /**
   * child.
   * */
  private Operator child;

  /**
   * @param queueStore the queue to store exported {@link TupleBatch}s.
   * @param child the child.
   * */
  public TBQueueExporter(final Queue<TupleBatch> queueStore, final Operator child) {
    this.queueStore = queueStore;
    this.child = child;
  }

  @Override
  public final Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
  }

  @Override
  protected final void cleanup() throws DbException {
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = child.nextReady();

    if (tb != null) {
      queueStore.add(tb);
    }
    return tb;
  }

  @Override
  public final Schema getSchema() {
    return child.getSchema();
  }

  @Override
  public final void setChildren(final Operator[] children) {
    child = children[0];
  }

}
