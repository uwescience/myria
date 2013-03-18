package edu.washington.escience.myriad.operator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.parallel.Consumer;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myriad.util.IPCUtils;

public class IDBInput extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private Operator initialIDBInput, iterationInput;
  private Consumer eosControllerInput;

  private final int controllerWorkerID;
  private final ExchangePairID controllerOpID;
  private final int selfIDBIdx;

  private transient boolean child1Ended;
  private transient boolean emptyDelta;
  private transient IPCConnectionPool connectionPool;
  private transient HashMap<Integer, List<Integer>> uniqueTupleIndices;
  private transient TupleBatchBuffer uniqueTuples;
  private transient Channel eoiReportChannel;

  /**
   * The logger for this class.
   * */
  private static final Logger LOGGER = LoggerFactory.getLogger(IDBInput.class.getName());

  public IDBInput(final int selfIDBIdx, final ExchangePairID controllerOpID, final int controllerWorkerID,
      final Operator initialIDBInput, final Operator iterationInput, final Consumer eosControllerInput) {

    Objects.requireNonNull(controllerOpID);
    Objects.requireNonNull(initialIDBInput);
    Objects.requireNonNull(iterationInput);
    Objects.requireNonNull(eosControllerInput);
    Objects.requireNonNull(controllerWorkerID);
    Preconditions.checkArgument(initialIDBInput.getSchema().equals(iterationInput.getSchema()));

    this.controllerOpID = controllerOpID;
    this.controllerWorkerID = controllerWorkerID;
    this.initialIDBInput = initialIDBInput;
    this.iterationInput = iterationInput;
    this.eosControllerInput = eosControllerInput;
    this.selfIDBIdx = selfIDBIdx;
  }

  private boolean compareTuple(final int index, final List<Object> cntTuple) {
    for (int i = 0; i < cntTuple.size(); ++i) {
      if (!(uniqueTuples.get(i, index).equals(cntTuple.get(i)))) {
        return false;
      }
    }
    return true;
  }

  protected TupleBatch doDupElim(final TupleBatch tb) {
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
        if (compareTuple(oldTupleIndex, cntTuple)) {
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
  public TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb;
    if (!child1Ended) {
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
  protected TupleBatch fetchNext() throws DbException, InterruptedException {
    TupleBatch tb;
    while ((tb = initialIDBInput.next()) != null) {
      tb = doDupElim(tb);
      if (tb.numTuples() > 0) {
        emptyDelta = false;
        return tb;
      }
    }
    if (!child1Ended) {
      return null;
    }

    while ((tb = iterationInput.next()) != null) {
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
    if (!child1Ended) {
      if (initialIDBInput.eos()) {
        setEOI(true);
        emptyDelta = true;
        child1Ended = true;
      }
    } else {
      try {
        eosControllerInput.nextReady();
        if (eosControllerInput.eos()) {
          setEOS();
          eoiReportChannel.write(IPCUtils.EOS);// notify the EOSController to end.
        } else if (iterationInput.eoi()) {
          iterationInput.setEOI(false);
          setEOI(true);
          final TupleBatchBuffer buffer = new TupleBatchBuffer(eoiReportSchema);
          // buffer.put(0, eosControllerInput.getOperatorID().getLong());
          buffer.put(0, selfIDBIdx);
          buffer.put(1, emptyDelta);
          eoiReportChannel.write(buffer.popAnyAsTM(0));
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
  public Operator[] getChildren() {
    return new Operator[] { initialIDBInput, iterationInput, eosControllerInput };
  }

  @Override
  public Schema getSchema() {
    return initialIDBInput.getSchema();
  }

  public static final Schema eoiReportSchema;

  static {
    final ImmutableList<Type> types = ImmutableList.of(Type.INT_TYPE, Type.BOOLEAN_TYPE);
    final ImmutableList<String> columnNames = ImmutableList.of("idbID", "isDeltaEmpty");
    final Schema schema = new Schema(types, columnNames);
    eoiReportSchema = schema;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    child1Ended = false;
    emptyDelta = true;
    uniqueTupleIndices = new HashMap<Integer, List<Integer>>();
    uniqueTuples = new TupleBatchBuffer(getSchema());
    connectionPool = (IPCConnectionPool) execEnvVars.get("ipcConnectionPool");
    eoiReportChannel = connectionPool.reserveLongTermConnection(controllerWorkerID);
    eoiReportChannel.write(IPCUtils.bosTM(controllerOpID));
  }

  @Override
  public void setChildren(final Operator[] children) {
    initialIDBInput = children[0];
    iterationInput = children[1];
    eosControllerInput = (Consumer) children[2];
  }

  @Override
  protected void cleanup() throws DbException {
    uniqueTupleIndices = null;
    uniqueTuples = null;
    if (eoiReportChannel != null) {
      connectionPool.releaseLongTermConnection(eoiReportChannel);
    }
    eoiReportChannel = null;
    connectionPool = null;
  }

  public ExchangePairID getControllerOperatorID() {
    return controllerOpID;
  }

  public int getControllerWorkerID() {
    return controllerWorkerID;
  }

}
