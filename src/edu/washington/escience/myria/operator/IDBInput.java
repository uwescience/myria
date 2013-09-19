package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.parallel.Consumer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.TaskResourceManager;
import edu.washington.escience.myria.parallel.ipc.StreamOutputChannel;

/**
 * Together with the EOSController, the IDBInput controls what to serve into an iteration and when to stop an iteration.
 * */
public class IDBInput extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(IDBInput.class.getName());

  /**
   * Initial IDB input.
   * */
  private Operator initialIDBInput;

  /**
   * input from iteration.
   * */
  private Operator iterationInput;

  /**
   * the Consumer who is responsible for receiving EOS notification from the EOSController.
   * */
  private Consumer eosControllerInput;

  /**
   * The workerID where the EOSController is running.
   * */
  private final int controllerWorkerID;

  /**
   * The operator ID to which the EOI report should be sent.
   * */
  private final ExchangePairID controllerOpID;

  /**
   * The index of this IDBInput. This is to differentiate the IDBInput operators in the same worker. Note that this
   * number is the index, it must start from 0 and to (The number of IDBInput operators in a worker -1)
   * */
  private final int selfIDBIdx;

  /**
   * Indicating if the initial input is ended.
   * */
  private transient boolean initialInputEnded;

  /**
   * Indicating if the number of tuples received from either the initialInput child or the iteration input child is 0
   * since last EOI.
   * */
  private transient boolean emptyDelta;
  /**
   * For IPC communication. Specifically, for doing EOI report.
   * */
  private transient TaskResourceManager resourceManager;
  /**
   * The same as in DupElim.
   * */
  private transient HashMap<Integer, List<Integer>> uniqueTupleIndices;
  /**
   * The same as in DupElim.
   * */
  private transient TupleBatchBuffer uniqueTuples;
  /**
   * The IPC channel for EOI report.
   * */
  private transient StreamOutputChannel<TupleBatch> eoiReportChannel;

  /**
   * @param selfIDBIdx see the corresponding field comment.
   * @param controllerOpID see the corresponding field comment.
   * @param controllerWorkerID see the corresponding field comment.
   * @param initialIDBInput see the corresponding field comment.
   * @param iterationInput see the corresponding field comment.
   * @param eosControllerInput see the corresponding field comment.
   * */
  public IDBInput(final int selfIDBIdx, final ExchangePairID controllerOpID, final int controllerWorkerID,
      final Operator initialIDBInput, final Operator iterationInput, final Consumer eosControllerInput) {
    Preconditions.checkNotNull(selfIDBIdx);
    Preconditions.checkNotNull(controllerOpID);
    Preconditions.checkNotNull(controllerWorkerID);

    this.selfIDBIdx = selfIDBIdx;
    this.controllerOpID = controllerOpID;
    this.controllerWorkerID = controllerWorkerID;
    this.initialIDBInput = initialIDBInput;
    this.iterationInput = iterationInput;
    this.eosControllerInput = eosControllerInput;
  }

  /**
   * Check if a tuple in uniqueTuples equals to the comparing tuple (cntTuple).
   * 
   * @param index the index in uniqueTuples
   * @param cntTuple a list representation of a tuple to compare
   * @return true if equals.
   * */
  private boolean tupleEquals(final int index, final List<Object> cntTuple) {
    for (int i = 0; i < cntTuple.size(); ++i) {
      if (!(uniqueTuples.get(i, index).equals(cntTuple.get(i)))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Do duplicate elimination for tb.
   * 
   * @param tb the TupleBatch for performing dupelim.
   * @return the duplicate eliminated TB.
   * */
  protected final TupleBatch doDupElim(final TupleBatch tb) {
    final int numTuples = tb.numTuples();
    if (numTuples <= 0) {
      return tb;
    }
    final BitSet toRemove = new BitSet(numTuples);
    final List<Object> cntTuple = new ArrayList<Object>();
    for (int i = 0; i < numTuples; ++i) {
      cntTuple.clear();
      for (int j = 0; j < tb.numColumns(); ++j) {
        cntTuple.add(tb.getObject(j, i));
      }
      final int nextIndex = uniqueTuples.numTuples();
      final int cntHashCode = tb.hashCode(i);
      List<Integer> tupleIndexList = uniqueTupleIndices.get(cntHashCode);
      if (tupleIndexList == null) {
        for (int j = 0; j < tb.numColumns(); ++j) {
          uniqueTuples.put(j, cntTuple.get(j));
        }
        tupleIndexList = new ArrayList<Integer>();
        tupleIndexList.add(nextIndex);
        uniqueTupleIndices.put(cntHashCode, tupleIndexList);
        continue;
      }
      boolean unique = true;
      for (final int oldTupleIndex : tupleIndexList) {
        if (tupleEquals(oldTupleIndex, cntTuple)) {
          unique = false;
          break;
        }
      }
      if (unique) {
        for (int j = 0; j < tb.numColumns(); ++j) {
          uniqueTuples.put(j, cntTuple.get(j));
        }
        tupleIndexList.add(nextIndex);
      } else {
        toRemove.set(i);
      }
    }
    return tb.remove(toRemove);
  }

  @Override
  public final TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb;
    if (!initialInputEnded) {
      while ((tb = initialIDBInput.nextReady()) != null) {
        tb = doDupElim(tb);
        if (tb.numTuples() > 0) {
          emptyDelta = false;
          return tb;
        }
      }
      return null;
    }

    while ((tb = iterationInput.nextReady()) != null) {
      tb = doDupElim(tb);
      if (tb.numTuples() > 0) {
        emptyDelta = false;
        return tb;
      }
    }

    return null;
  }

  @Override
  public final void checkEOSAndEOI() {
    if (!initialInputEnded) {
      if (initialIDBInput.eos()) {
        setEOI(true);
        emptyDelta = true;
        initialInputEnded = true;
      }
    } else {
      try {
        if (eosControllerInput.hasNext()) {
          eosControllerInput.nextReady();
        }

        if (eosControllerInput.eos()) {
          setEOS();
          eoiReportChannel.release();
          // notify the EOSController to end.
        } else if (iterationInput.eoi()) {
          iterationInput.setEOI(false);
          setEOI(true);
          final TupleBatchBuffer buffer = new TupleBatchBuffer(EOI_REPORT_SCHEMA);
          buffer.putInt(0, selfIDBIdx);
          buffer.putBoolean(1, emptyDelta);
          eoiReportChannel.write(buffer.popAny());
          emptyDelta = true;
        }
      } catch (DbException e) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Unknown error. ", e);
        }
      }
    }
  }

  @Override
  public final Operator[] getChildren() {
    return new Operator[] { initialIDBInput, iterationInput, eosControllerInput };
  }

  @Override
  public final Schema getSchema() {
    return initialIDBInput.getSchema();
  }

  /**
   * the schema of EOI report.
   * */
  public static final Schema EOI_REPORT_SCHEMA;

  static {
    final ImmutableList<Type> types = ImmutableList.of(Type.INT_TYPE, Type.BOOLEAN_TYPE);
    final ImmutableList<String> columnNames = ImmutableList.of("idbID", "isDeltaEmpty");
    final Schema schema = new Schema(types, columnNames);
    EOI_REPORT_SCHEMA = schema;
  }

  @Override
  public final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    initialInputEnded = false;
    emptyDelta = true;
    uniqueTupleIndices = new HashMap<Integer, List<Integer>>();
    uniqueTuples = new TupleBatchBuffer(getSchema());
    resourceManager = (TaskResourceManager) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_TASK_RESOURCE_MANAGER);
    eoiReportChannel = resourceManager.startAStream(controllerWorkerID, controllerOpID);
  }

  @Override
  public final void setChildren(final Operator[] children) {
    Preconditions.checkArgument(children.length == 3);
    Preconditions.checkNotNull(children[0]);
    Preconditions.checkNotNull(children[1]);
    Preconditions.checkNotNull(children[2]);
    initialIDBInput = children[0];
    iterationInput = children[1];
    eosControllerInput = (Consumer) children[2];
    Preconditions.checkArgument(initialIDBInput.getSchema().equals(iterationInput.getSchema()));
  }

  @Override
  protected final void cleanup() throws DbException {
    uniqueTupleIndices = null;
    uniqueTuples = null;
    eoiReportChannel.release();
    eoiReportChannel = null;
    resourceManager = null;
  }

  /**
   * @return the operator ID of the EOI receiving Consumer of the EOSController.
   * */
  public final ExchangePairID getControllerOperatorID() {
    return controllerOpID;
  }

  /**
   * @return the workerID where the EOSController is running.
   * */
  public final int getControllerWorkerID() {
    return controllerWorkerID;
  }

}
