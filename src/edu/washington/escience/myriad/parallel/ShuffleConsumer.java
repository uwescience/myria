package edu.washington.escience.myriad.parallel;

import java.util.BitSet;
import java.util.HashMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * The consumer part of the Shuffle Exchange operator.
 * 
 * A ShuffleProducer operator sends tuples to all the workers according to some PartitionFunction, while the
 * ShuffleConsumer (this class) encapsulates the methods to collect the tuples received at the worker from multiple
 * source workers' ShuffleProducer.
 * 
 */
public class ShuffleConsumer extends Consumer {

  private static final long serialVersionUID = 1L;

  // private boolean finish;

  // Used to remember which of the source workers have sent an end of stream
  // message.
  private final BitSet workerEOS;

  private final int[] sourceWorkers;
  private final HashMap<Integer, Integer> workerIdToIndex;
  private ShuffleProducer child;
  private Schema schema;

  public ShuffleConsumer(final ShuffleProducer child, final ExchangePairID operatorID, final int[] workerIDs) {
    super(operatorID);
    this.child = child;
    sourceWorkers = workerIDs;
    workerIdToIndex = new HashMap<Integer, Integer>();
    int i = 0;
    for (final Integer w : sourceWorkers) {
      workerIdToIndex.put(w, i++);
    }
    workerEOS = new BitSet(workerIDs.length);
    // finish = false;
  }

  @Override
  public void cleanup() {
    workerEOS.clear();
  }

  @Override
  protected _TupleBatch fetchNext() throws DbException {
    if (!eos()) {
      try {
        return getTuples(true);
      } catch (final InterruptedException e) {
        e.printStackTrace();
        throw new DbException(e.getLocalizedMessage());
      }
    }
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public String getName() {
    return "shuffle_c";
  }

  @Override
  public Schema getSchema() throws DbException {
    if (child != null) {
      return child.getSchema();
    } else {
      return schema;
    }
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
  final _TupleBatch getTuples(final boolean blocking) throws InterruptedException {
    int timeToWait = -1;
    if (!blocking) {
      timeToWait = 0;
    }

    ExchangeTupleBatch tb = null;

    while (workerEOS.nextClearBit(0) < sourceWorkers.length) {
      tb = take(timeToWait);

      if (tb != null) {
        if (tb.isEos()) {
          workerEOS.set(workerIdToIndex.get(tb.getWorkerID()));
        } else {
          return tb;
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
  }

  @Override
  public _TupleBatch fetchNextReady() throws DbException {
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

}
