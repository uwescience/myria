package edu.washington.escience.myriad.parallel;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.operator.LeafOperator;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;

public class Consumer extends LeafOperator {

  /** The logger for this class. Defaults to myriad level, but could be set to a finer granularity if needed. */
  private static final Logger LOGGER = LoggerFactory.getLogger("edu.washington.escience.myriad");

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /**
   * The buffer for receiving ExchangeMessages. This buffer should be assigned by the Worker. Basically, buffer =
   * Worker.inBuffer.get(this.getOperatorID())
   */
  private transient volatile LinkedBlockingQueue<ExchangeData> inputBuffer;

  private final ExchangePairID operatorID;
  private final Schema schema;
  private final BitSet workerEOS;
  private final BitSet workerEOI;
  private final Map<Integer, Integer> workerIdToIndex;

  public Consumer(final Schema schema, final ExchangePairID operatorID, final int[] workerIDs) {
    this.operatorID = operatorID;
    this.schema = schema;
    workerIdToIndex = new HashMap<Integer, Integer>();
    int idx = 0;
    for (final int w : workerIDs) {
      workerIdToIndex.put(w, idx++);
    }
    workerEOS = new BitSet(workerIDs.length);
    workerEOI = new BitSet(workerIDs.length);
    LOGGER.trace("created Consumer for ExchangePairId=" + operatorID);
  }

  @Override
  public void cleanup() {
    setInputBuffer(null);
    workerEOS.clear();
    workerEOI.clear();
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
    try {
      return getTuples(true);
    } catch (final InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
      throw new DbException(e);
    }
  }

  @Override
  public final Schema getSchema() {
    return schema;
  }

  @Override
  public void init() throws DbException {
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    if (!eos()) {
      try {
        return getTuples(false);
      } catch (final InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
        throw new DbException(e.getLocalizedMessage());
      }
    }
    return null;
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
    tb = take(timeToWait);
    if (tb != null) {
      if (tb.isEos()) {
        workerEOS.set(workerIdToIndex.get(tb.getWorkerID()));
        return null;
      } else if (tb.isEoi()) {
        workerEOI.set(workerIdToIndex.get(tb.getWorkerID()));
        return null;
      } else {
        return tb.getRealData();
      }
    } else {
      // i.e. blocking = false. if blocking = true then tb is either a TupleBatch or a message
      return null;
    }
  }

  @Override
  public void checkEOSAndEOI() {
    if (workerEOS.nextClearBit(0) == workerIdToIndex.size()) {
      setEOS(true);
      return;
    }
    BitSet tmp = (BitSet) workerEOI.clone();
    tmp.or(workerEOS);
    // EOS could be used as an EOI
    if (tmp.nextClearBit(0) == workerIdToIndex.size()) {
      setEOI(true);
      workerEOI.clear();
    }
  }

  public ExchangePairID getOperatorID() {
    return operatorID;
  }

  public void setInputBuffer(final LinkedBlockingQueue<ExchangeData> buffer) {
    inputBuffer = buffer;
  }

  /**
   * Read a single ExchangeMessage from the queue that buffers incoming ExchangeMessages.
   * 
   * @param timeout Wait for at most timeout milliseconds. If the timeout is negative, wait until an element arrives.
   */
  public ExchangeData take(final int timeout) throws InterruptedException {
    if (timeout >= 0) {
      return inputBuffer.poll(timeout, TimeUnit.MILLISECONDS);
    } else {
      return inputBuffer.take();
    }
  }
}
