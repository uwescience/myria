package edu.washington.escience.myria.operator.network;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.gs.collections.impl.map.mutable.primitive.IntIntHashMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMode;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.LeafOperator;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.LocalFragmentResourceManager;
import edu.washington.escience.myria.parallel.QueryExecutionMode;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.IPCMessage;
import edu.washington.escience.myria.parallel.ipc.StreamIOChannelID;
import edu.washington.escience.myria.parallel.ipc.StreamInputBuffer;
import edu.washington.escience.myria.storage.ExchangeTupleBatch;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * A Consumer is the counterpart of a producer. It collects data from Producers through IPC. A Consumer can have a
 * single Producer data source or multiple Producer data sources.
 * */
public class Consumer extends LeafOperator {

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The buffer for receiving input data.
   */
  private transient volatile StreamInputBuffer<TupleBatch> inputBuffer;

  /**
   * The operatorID of this Consumer.
   * */
  private final ExchangePairID operatorID;

  /**
   * The output schema.
   */
  private Schema schema;
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
  private transient IntIntHashMap workerIdToIndex;
  /**
   * From which workers to receive data.
   * */
  private final ImmutableSet<Integer> sourceWorkers;

  /**
   * if current query execution is in non-blocking mode.
   * */
  private transient boolean nonBlockingExecution;

  /**
   * The worker this operator is located at.
   *
   */
  private transient LocalFragmentResourceManager taskResourceManager;

  /**
   * @return my exchange channels.
   * @param myWorkerID for parsing self-references.
   */
  public final ImmutableSet<StreamIOChannelID> getInputChannelIDs(final int myWorkerID) {
    ImmutableSet.Builder<StreamIOChannelID> ecB = ImmutableSet.builder();
    for (int wID : sourceWorkers) {
      if (wID == IPCConnectionPool.SELF_IPC_ID) {
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
    this(
        schema,
        operatorID,
        MyriaArrayUtils.checkSet(org.apache.commons.lang3.ArrayUtils.toObject(workerIDs)));
  }

  /**
   * @param schema output schema.
   * @param operatorID {@link Consumer#operatorID}
   * @param workerIDs {@link Consumer#sourceWorkers}
   * */
  public Consumer(
      final Schema schema, final ExchangePairID operatorID, final Set<Integer> workerIDs) {
    this.operatorID = operatorID;
    this.schema = schema;
    sourceWorkers = ImmutableSet.copyOf(workerIDs);
    LOGGER.trace("created Consumer for ExchangePairId=" + operatorID);
  }

  /**
   * @param schema output schema.
   * @param operatorID {@link Consumer#operatorID}
   * */
  public Consumer(final Schema schema, final ExchangePairID operatorID) {
    this(schema, operatorID, ImmutableSet.of(IPCConnectionPool.SELF_IPC_ID));
  }

  @Override
  public final void cleanup() {
    taskResourceManager.releaseInputBuffer(this);
    inputBuffer = null;
    workerEOS.clear();
    workerEOI.clear();
  }

  @Override
  protected final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    workerEOS = new BitSet(sourceWorkers.size());
    workerEOI = new BitSet(sourceWorkers.size());

    int idx = 0;
    workerIdToIndex = new IntIntHashMap();
    for (int sourceWorker : sourceWorkers) {
      workerIdToIndex.put(sourceWorker, idx++);
    }

    taskResourceManager =
        (LocalFragmentResourceManager)
            execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_FRAGMENT_RESOURCE_MANAGER);
    nonBlockingExecution =
        (QueryExecutionMode) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_EXECUTION_MODE)
            == QueryExecutionMode.NON_BLOCKING;

    inputBuffer = taskResourceManager.getInputBuffer(this);
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

    IPCMessage.StreamData<TupleBatch> tb = null;
    TupleBatch result = null;
    while ((tb = take(timeToWait)) != null) {
      int sourceWorkerIdx = workerIdToIndex.get(tb.getRemoteID());
      TupleBatch ttbb = tb.getPayload();
      if (ttbb != null) {
        ttbb = ExchangeTupleBatch.wrap(ttbb, tb.getRemoteID());
      }

      if (ttbb == null) {
        // EOS
        workerEOS.set(sourceWorkerIdx);
        checkEOSAndEOI();
        if (eos() || eoi()) {
          break;
        }
      } else if (ttbb.isEOI()) {
        workerEOI.set(sourceWorkerIdx);
        checkEOSAndEOI();
        if (eos() || eoi()) {
          break;
        }
      } else {
        result = ttbb;
        break;
      }
    }

    return result;
  }

  @Override
  public final void checkEOSAndEOI() {

    int numExpecting = sourceWorkers.size();

    if (taskResourceManager.getFragment().getLocalSubQuery().getFTMode().equals(FTMode.ABANDON)) {
      Set<Integer> expectingWorkers = new HashSet<Integer>();
      expectingWorkers.addAll(sourceWorkers);
      expectingWorkers.removeAll(
          taskResourceManager.getFragment().getLocalSubQuery().getMissingWorkers());
      numExpecting = expectingWorkers.size();
    }

    if (workerEOS.cardinality() >= numExpecting) {
      setEOS();
      return;
    }
    BitSet tmp = (BitSet) workerEOI.clone();
    tmp.or(workerEOS);
    // EOS could be used as an EOI
    if (tmp.cardinality() >= numExpecting) {
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
    int[] result = new int[sourceWorkers.size()];
    int idx = 0;
    for (int workerID : sourceWorkers) {
      if (workerID != IPCConnectionPool.SELF_IPC_ID) {
        result[idx++] = workerID;
      } else {
        result[idx++] = myWorkerID;
      }
    }

    return result;
  }

  /**
   * @return my input buffer.
   * */
  public final StreamInputBuffer<TupleBatch> getInputBuffer() {
    return inputBuffer;
  }

  /**
   * Read a single ExchangeMessage from the queue that buffers incoming ExchangeMessages.
   *
   * @param timeout Wait for at most timeout milliseconds. If the timeout is negative, wait until an element arrives.
   * @return received data.
   * @throws InterruptedException if interrupted.
   */
  private IPCMessage.StreamData<TupleBatch> take(final int timeout) throws InterruptedException {
    IPCMessage.StreamData<TupleBatch> result = null;
    Verify.verifyNotNull(inputBuffer, "inputBuffer should not be null");
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
  protected final Schema generateSchema() {
    return schema;
  }

  /**
   * @param schema the schema to set
   * */
  public final void setSchema(final Schema schema) {
    this.schema = schema;
  }
}
