package edu.washington.escience.myriad.parallel;

import java.util.BitSet;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.operator.LeafOperator;
import gnu.trove.impl.unmodifiable.TUnmodifiableIntIntMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

public abstract class Consumer extends LeafOperator {

  /** The logger for this class. Defaults to myriad level, but could be set to a finer granularity if needed. */
  private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /**
   * The buffer for receiving ExchangeMessages. This buffer should be assigned by the Worker. Basically, buffer =
   * Worker.inBuffer.get(this.getOperatorID())
   */
  transient volatile InputBuffer<TupleBatch, ExchangeData> inputBuffer;

  private final ExchangePairID operatorID;
  private final Schema schema;

  private transient BitSet workerEOS;
  private transient BitSet workerEOI;
  private transient TIntIntMap workerIdToIndex;
  private final int[] sourceWorkers;
  public transient long[] recentSeqNum;
  public transient LinkedList<Integer> inputGlobalOrder; // recording data message only, list of worker IDs
  public transient boolean inRewinding;
  public transient ConsumerChannel[] exchangeChannels;

  @Override
  public void rewind(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    inRewinding = true;
    for (int i = 0; i < sourceWorkers.length; i++) {
      recentSeqNum[i] = -1;
    }
    workerEOS = new BitSet(sourceWorkers.length);
    workerEOI = new BitSet(sourceWorkers.length);
  }

  /**
   * If there's no child operator, a Schema is needed.
   */
  protected Consumer(final Schema schema, final ExchangePairID operatorID, final int[] workerIDs) {
    this.operatorID = operatorID;
    this.schema = schema;
    sourceWorkers = workerIDs;
    LOGGER.trace("created Consumer for ExchangePairId=" + operatorID);
  }

  @Override
  public void cleanup() {
    setInputBuffer(null);
    workerEOS.clear();
    workerEOI.clear();
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execUnitEnv) throws DbException {
    workerEOS = new BitSet(sourceWorkers.length);
    workerEOI = new BitSet(sourceWorkers.length);

    recentSeqNum = new long[sourceWorkers.length];

    inRewinding = false;
    inputGlobalOrder = new LinkedList<Integer>();

    TIntIntMap tmp = new TIntIntHashMap();
    int idx = 0;
    for (int i = 0; i < sourceWorkers.length; i++) {
      tmp.put(sourceWorkers[i], idx++);
      recentSeqNum[i] = -1;
    }
    workerIdToIndex = new TUnmodifiableIntIntMap(tmp);
  }

  @Override
  protected TupleBatch fetchNext() throws DbException, InterruptedException {
    return getTuplesNormal(true);
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
  TupleBatch getTuplesNormal(final boolean blocking) throws InterruptedException {
    int timeToWait = -1;
    if (!blocking) {
      timeToWait = 0;
    }

    ExchangeData tb = null;
    TupleBatch result = null;
    while ((tb = take(timeToWait)) != null) {
      int sourceWorkerIdx = workerIdToIndex.get(tb.getSourceIPCID());

      if (tb.getData() != null) {
        if (tb.seqNum <= recentSeqNum[sourceWorkerIdx]) {
          if (LOGGER.isInfoEnabled()) {
            LOGGER.info(
                "Duplicate TupleBatch, received from worker:{} to Operator:{}. seqNum:{}, expected:{} Drop directly.",
                tb.getSourceIPCID(), tb.getOperatorID(), tb.seqNum, recentSeqNum[sourceWorkerIdx] + 1);
          }

          continue;
        }
      }
      if (tb.isEos()) {
        workerEOS.set(sourceWorkerIdx);
        checkEOSAndEOI();
        if (eos() || eoi()) {
          break;
        }
      } else if (tb.isEoi()) {
        workerEOI.set(sourceWorkerIdx);
        checkEOSAndEOI();
        if (eoi()) {
          break;
        }
      } else {
        result = tb.getData();
        break;
      }
    }

    return result;
  }

  // only for non-blocking
  TupleBatch getTuplesRewind() throws InterruptedException {

    while (!inputGlobalOrder.isEmpty()) {
      Integer expectedRemote = inputGlobalOrder.getFirst();
      ExchangeData actual = inputBuffer.pickFirst(expectedRemote);
      if (actual != null) {
        int sourceWorkerIdx = workerIdToIndex.get(actual.getSourceIPCID());
        if (actual.getData() != null) {
          // data message
          if (actual.seqNum <= recentSeqNum[sourceWorkerIdx]) {
            if (LOGGER.isInfoEnabled()) {
              LOGGER
                  .info(
                      "Duplicate TupleBatch, received from worker:{} to Operator:{}. seqNum:{}, expected:{} Drop directly.",
                      actual.getSourceIPCID(), actual.getOperatorID(), actual.seqNum, recentSeqNum[sourceWorkerIdx] + 1);
            }
            continue;
          }
        }
        inputGlobalOrder.remove(); // a legal input

        if (actual.isEos()) {
          workerEOS.set(sourceWorkerIdx);
          checkEOSAndEOI();
          if (eos() || eoi()) {
            break;
          }
        }
        return actual.getData();
      }
      return null;
    }

    inRewinding = false;
    return getTuplesNormal(false);
  }

  @Override
  public void checkEOSAndEOI() {

    if (workerEOS.nextClearBit(0) >= sourceWorkers.length) {
      setEOS();
      return;
    }
    BitSet tmp = (BitSet) workerEOI.clone();
    tmp.or(workerEOS);
    // EOS could be used as an EOI
    if (tmp.nextClearBit(0) >= sourceWorkers.length) {
      setEOI(true);
      workerEOI.clear();
    }
  }

  public final ExchangePairID getOperatorID() {
    return operatorID;
  }

  /**
   * @param myWorkerID for parsing self-references.
   * */
  public final int[] getSourceWorkers(final int myWorkerID) {
    int[] result = new int[sourceWorkers.length];
    int idx = 0;
    for (int workerID : sourceWorkers) {
      if (workerID >= 0) {
        result[idx++] = workerID;
      } else {
        result[idx++] = myWorkerID;
      }
    }

    return result;
  }

  public final void setInputBuffer(final InputBuffer<TupleBatch, ExchangeData> buffer) {
    inputBuffer = buffer;
  }

  /**
   * Read a single ExchangeMessage from the queue that buffers incoming ExchangeMessages.
   * 
   * @param timeout Wait for at most timeout milliseconds. If the timeout is negative, wait until an element arrives.
   */
  private ExchangeData take(final int timeout) throws InterruptedException {
    ExchangeData result = null;
    if (timeout == 0) {
      result = inputBuffer.poll();
    } else if (timeout > 0) {
      result = inputBuffer.poll(timeout, TimeUnit.MILLISECONDS);
    } else {
      result = inputBuffer.take();
    }

    return result;
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    try {
      if (inRewinding) {
        return getTuplesRewind();
      } else {
        return getTuplesNormal(false);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return null;
  }

  @Override
  public final Schema getSchema() {
    return schema;
  }

}
