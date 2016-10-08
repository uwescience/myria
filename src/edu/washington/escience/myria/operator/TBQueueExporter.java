package edu.washington.escience.myria.operator;

import java.util.Queue;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * Transparently export the data from child to a {@link Queue<TupleBatch>}.
 *
 * Do not use a {@link TupleBatchBuffer} here because {@link TupleBatchBuffer} is not thread safe.
 */
public class TBQueueExporter extends UnaryOperator {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * The queue to store the TupleBatches.
   */
  private final Queue<TupleBatch> queueStore;

  /**
   * @param queueStore the queue to store exported {@link TupleBatch}s.
   * @param child the child.
   */
  public TBQueueExporter(final Queue<TupleBatch> queueStore, final Operator child) {
    super(child);
    this.queueStore = queueStore;
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = getChild().nextReady();

    if (tb != null) {
      while (!queueStore.offer(tb)) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        }
      }
    }
    return tb;
  }

  @Override
  protected final Schema generateSchema() {
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    return child.getSchema();
  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.washington.escience.myria.operator.Operator#sendEos()
   */
  @Override
  protected void sendEos() throws DbException {
    // TODO Auto-generated method stub

  }
}
