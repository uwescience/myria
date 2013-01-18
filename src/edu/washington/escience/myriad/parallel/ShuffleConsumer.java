package edu.washington.escience.myriad.parallel;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Objects;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;

/**
 * The consumer part of the Shuffle Exchange operator.
 * 
 * A ShuffleProducer operator sends tuples to all the workers according to some PartitionFunction, while the
 * ShuffleConsumer (this class) encapsulates the methods to collect the tuples received at the worker from multiple
 * source workers' ShuffleProducer.
 * 
 */
public final class ShuffleConsumer extends Consumer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  // Used to remember which of the source workers have sent an end of stream
  // message.
  private final BitSet workerEOS;

  private final int[] sourceWorkers;
  private final HashMap<Integer, Integer> workerIdToIndex;
  private ShuffleProducer child;
  private Schema schema;

  public ShuffleConsumer(final Schema schema, final ExchangePairID operatorID, final int[] workerIDs) {
    super(operatorID);

    Objects.requireNonNull(schema);
    Objects.requireNonNull(operatorID);
    Objects.requireNonNull(workerIDs);

    child = null;
    this.schema = schema;
    sourceWorkers = workerIDs;
    workerIdToIndex = new HashMap<Integer, Integer>();
    int i = 0;
    for (final Integer w : sourceWorkers) {
      workerIdToIndex.put(w, i++);
    }
    workerEOS = new BitSet(workerIDs.length);
  }

  public ShuffleConsumer(final ShuffleProducer child, final ExchangePairID operatorID, final int[] workerIDs) {
    super(operatorID);

    Objects.requireNonNull(child);
    Objects.requireNonNull(operatorID);
    Objects.requireNonNull(workerIDs);

    this.child = child;
    schema = child.getSchema();
    sourceWorkers = workerIDs;
    workerIdToIndex = new HashMap<Integer, Integer>();
    int i = 0;
    for (final Integer w : sourceWorkers) {
      workerIdToIndex.put(w, i++);
    }
    workerEOS = new BitSet(workerIDs.length);
  }

  @Override
  public void cleanup() {
    workerEOS.clear();
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
    try {
      return getTuples(true);
    } catch (final InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
      throw new DbException(e.getLocalizedMessage());
    }
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    if (!eos()) {
      try {
        return getTuples(false);
      } catch (final InterruptedException e) {
        e.printStackTrace();
        // retain the interrupted bit
        Thread.currentThread().interrupt();
        throw new DbException(e.getLocalizedMessage());
      }
    }
    return null;
  }

  @Override
  public Operator[] getChildren() {
    if (child != null) {
      return new Operator[] { child };
    }
    return null;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  /**
   * 
   * Retrieve a batch of tuples from the buffer of ExchangeMessages. Wait if the buffer is empty.
   * 
   * @param blocking if blocking then return only if there's actually a TupleBatch to return or null if EOS. If not
   *          blocking then return null immediately if there's no data in the input buffer.
   * 
   * @return Iterator over the new tuples received from the source workers. Return <code>null</code> if all source
   *         workers have sent an end of file message.
   * 
   * @throws InterruptedException a
   */
  TupleBatch getTuples(final boolean blocking) throws InterruptedException {
    int timeToWait = -1;
    if (!blocking) {
      timeToWait = 0;
    }

    ExchangeData tb = null;

    while (workerEOS.nextClearBit(0) < sourceWorkers.length) {
      tb = take(timeToWait);

      if (tb != null) {
        if (tb.isEos()) {
          workerEOS.set(workerIdToIndex.get(tb.getWorkerID()));
        } else {
          return tb.getRealData();
        }
      } else {
        // if blocking=true, no null should be got from take
        return null;
      }
    }
    // have received all the eos message from all the workers
    setEOS();

    return null;
  }

  @Override
  public void init() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = (ShuffleProducer) children[0];
    schema = child.getSchema();
  }

}
