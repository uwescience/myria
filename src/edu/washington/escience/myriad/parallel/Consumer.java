package edu.washington.escience.myriad.parallel;

import java.util.BitSet;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.operator.LeafOperator;
import edu.washington.escience.myriad.parallel.Worker.QueryExecutionMode;
import edu.washington.escience.myriad.parallel.ipc.StreamIOChannelID;
import gnu.trove.impl.unmodifiable.TUnmodifiableIntIntMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

/**
 * A Consumer is the counterpart of a producer. It collects data from Producers through IPC. A Consumer can have a
 * single Producer data source or multiple Producer data sources.
 * */
public class Consumer extends LeafOperator {

  /** The logger for this class. Defaults to myriad level, but could be set to a finer granularity if needed. */
  private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /**
   * The buffer for receiving ExchangeMessages. This buffer should be assigned by the Worker. Basically, buffer =
   * Worker.inBuffer.get(this.getOperatorID())
   */
  private transient volatile InputBuffer<TupleBatch, ExchangeData> inputBuffer;

  /**
   * The operatorID of this Consumer.
   * */
  private final ExchangePairID operatorID;

  /**
   * The output schema.
   * */
  private final Schema schema;
  /**
   * Recording the worker EOS status.
   * */
  private transient BitSet workerEOS;
  /**
   * Recording the worker EOI status.
   * */
  private transient BitSet workerEOI;
  /**
   * workerID to index.
   * */
  private transient TIntIntMap workerIdToIndex;
  /**
   * From which workers to receive data.
   * */
  private final int[] sourceWorkers;

  /**
   * if current query execution is in non-blocking mode.
   * */
  private transient boolean nonBlockingExecution;

  /**
   * @return my exchange channels.
   * @param myWorkerID for parsing self-references.
   */
  public final ImmutableSet<StreamIOChannelID> getExchangeChannels(final int myWorkerID) {
    ImmutableSet.Builder<StreamIOChannelID> ecB = ImmutableSet.builder();
    for (int wID : sourceWorkers) {
      if (wID < 0) {
        ecB.add(new StreamIOChannelID(operatorID.getLong(), myWorkerID));
      } else {
        ecB.add(new StreamIOChannelID(operatorID.getLong(), wID));
      }
    }
    return ecB.build();
  }

  /**
   * @param schema output schema.
   * @param operatorID {@link Consumer#operatorID}
   * @param workerIDs {@link Consumer#sourceWorkers}
   * */
  public Consumer(final Schema schema, final ExchangePairID operatorID, final int[] workerIDs) {
    this.operatorID = operatorID;
    this.schema = schema;
    sourceWorkers = workerIDs;
    LOGGER.trace("created Consumer for ExchangePairId=" + operatorID);
  }

  /**
   * @param schema output schema.
   * @param operatorID {@link Consumer#operatorID}
   * */
  public Consumer(final Schema schema, final ExchangePairID operatorID) {
    this.operatorID = operatorID;
    this.schema = schema;
    sourceWorkers = new int[] { -1 };
    LOGGER.trace("created Consumer for ExchangePairId=" + operatorID);
  }

  @Override
  public final void cleanup() {
    setInputBuffer(null);
    workerEOS.clear();
    workerEOI.clear();
  }

  @Override
  protected final void init(final ImmutableMap<String, Object> execUnitEnv) throws DbException {
    workerEOS = new BitSet(sourceWorkers.length);
    workerEOI = new BitSet(sourceWorkers.length);

    TIntIntMap tmp = new TIntIntHashMap();
    int idx = 0;
    for (int sourceWorker : sourceWorkers) {
      tmp.put(sourceWorker, idx++);
    }
    workerIdToIndex = new TUnmodifiableIntIntMap(tmp);

    QueryExecutionMode executionMode = (QueryExecutionMode) execUnitEnv.get(MyriaConstants.EXEC_ENV_VAR_EXECUTION_MODE);
    nonBlockingExecution = (executionMode == QueryExecutionMode.NON_BLOCKING);

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
  final TupleBatch getTuplesNormal(final boolean blocking) throws InterruptedException {
    int timeToWait = -1;
    if (!blocking) {
      timeToWait = 0;
    }

    ExchangeData tb = null;
    TupleBatch result = null;
    while ((tb = take(timeToWait)) != null) {
      int sourceWorkerIdx = workerIdToIndex.get(tb.getSourceIPCID());

      if (tb.isEos()) {
        workerEOS.set(sourceWorkerIdx);
        checkEOSAndEOI();
        if (eos() || eoi()) {
          break;
        }
      } else if (tb.isEoi()) {
        workerEOI.set(sourceWorkerIdx);
        checkEOSAndEOI();
        if (eos() || eoi()) {
          break;
        }
      } else {
        result = tb.getData();
        break;
      }
    }

    return result;
  }

  @Override
  public final void checkEOSAndEOI() {

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

  /**
   * @return my IPC operatorID.
   * */
  public final ExchangePairID getOperatorID() {
    return operatorID;
  }

  /**
   * @param myWorkerID for parsing self-references.
   * @return source worker IDs with self-reference parsed.
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

  /**
   * set the input buffer.
   * 
   * @param buffer my input buffer.
   * */
  public final void setInputBuffer(final InputBuffer<TupleBatch, ExchangeData> buffer) {
    if (inputBuffer != null) {
      inputBuffer.detached();
      inputBuffer = null;
    }
    if (buffer != null) {
      buffer.attach(operatorID);
      inputBuffer = buffer;
    }
  }

  /**
   * @return my input buffer.
   * */
  public final InputBuffer<TupleBatch, ExchangeData> getInputBuffer() {
    return inputBuffer;
  }

  /**
   * Read a single ExchangeMessage from the queue that buffers incoming ExchangeMessages.
   * 
   * @param timeout Wait for at most timeout milliseconds. If the timeout is negative, wait until an element arrives.
   * @return received data.
   * @throws InterruptedException if interrupted.
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

  /**
   * @return if there's any message buffered.
   * */
  public final boolean hasNext() {
    return !inputBuffer.isEmpty();
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    try {
      return getTuplesNormal(!nonBlockingExecution);
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
