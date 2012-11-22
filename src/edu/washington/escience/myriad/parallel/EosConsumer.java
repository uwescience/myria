package edu.washington.escience.myriad.parallel;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * The EosConsumer is a consumer that ignores all its input and simply waits for all its producer children to finish.
 * 
 * @author dhalperi
 */
public final class EosConsumer extends Consumer {

  /** The logger for this class. Defaults to myriad level, but could be set to a finer granularity if needed. */
  private static final Logger LOGGER = LoggerFactory.getLogger("edu.washington.escience.myriad");

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The number of producers. */
  private final int numWorkers;
  /** A list of which workers have reached the end of their streams. */
  private final BitSet workerEOS;
  /** A mapping from worker ID to worker index in the workerEOS bitmap. */
  private final Map<Integer, Integer> workerIdToIndex;

  /**
   * Construct an EosConsumer that waits until all the specified workers are terminated.
   * 
   * @param operatorID the identifier of the pipeline between this EosConsumer and all its workers.
   * @param workerIDs the identifiers of all the workers that must finish before this EosConsumer terminates.
   */
  public EosConsumer(final ExchangePairID operatorID, final int[] workerIDs) {
    super(operatorID);
    numWorkers = workerIDs.length;
    workerIdToIndex = new HashMap<Integer, Integer>();
    int idx = 0;
    for (final int w : workerIDs) {
      workerIdToIndex.put(w, idx++);
    }
    workerEOS = new BitSet(workerIDs.length);
  }

  @Override
  public void cleanup() {
    workerEOS.clear();
  }

  @Override
  protected _TupleBatch fetchNext() throws DbException {
    try {
      /* Pull tuple batches off the stream until all workers have send Eos messages. */
      while (workerEOS.nextClearBit(0) < numWorkers) {
        final ExchangeTupleBatch tb = take(-1);
        if (tb.isEos()) {
          workerEOS.set(workerIdToIndex.get(tb.getWorkerID()));
          LOGGER.debug("EOS received in CollectConsumer. From WorkerID:" + tb.getWorkerID());
        }
      }

      /* At this point, we have received an Eos message from all the workers. */
      setEOS();

      return null;
    } catch (final InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
      throw new DbException(e);
    }
  }

  @Override
  public _TupleBatch fetchNextReady() throws DbException {
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return null;
  }

  @Override
  public Schema getSchema() {
    return null;
  }

  @Override
  public void init() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    throw new UnsupportedOperationException();
  }
}
