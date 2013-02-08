package edu.washington.escience.myriad.operator;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.IPCConnectionPool;
import edu.washington.escience.myriad.parallel.Producer;

public class IDBInput extends Producer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private Operator child1, child2, child3;
  private final Schema outputSchema;

  private boolean child1Ended = false;
  private int tuplesSentSinceLastEOI = 0;

  private final int controllerWorkerID;
  private final int selfWorkerID;
  private final int selfIDBID;

  private static final Logger LOGGER = LoggerFactory.getLogger("edu.washington.escience.myriad");

  public IDBInput(final Schema outputSchema, final int selfWorkerID, final int selfIDBID,
      final ExchangePairID operatorID, final int controllerWorkerID, final Operator child1, final Operator child2,
      final Operator child3) {
    super(operatorID);
    this.outputSchema = outputSchema;
    this.controllerWorkerID = controllerWorkerID;
    this.selfWorkerID = selfWorkerID;
    this.selfIDBID = selfIDBID;
    this.child1 = child1;
    this.child2 = child2;
    this.child3 = child3;
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
    TupleBatch tb;
    if ((tb = child1.next()) != null) {
      tuplesSentSinceLastEOI += tb.numTuples();
      return tb;
    }
    if (!child1Ended) {
      return null;
    }
    if ((tb = child2.next()) != null) {
      tuplesSentSinceLastEOI += tb.numTuples();
      return tb;
    }
    return null;
  }

  @Override
  public final void checkEOSAndEOI() {
    if (!child1Ended && child1.eos()) {
      setEOI(true);
      tuplesSentSinceLastEOI = 0;
      child1Ended = true;
    } else {
      try {
        if (child3.nextReady()) {
          // should be EOS
          child3.next();
          setEOS(true);
        } else if (child2.eoi()) {
          child2.setEOI(false);
          setEOI(true);
          final IPCConnectionPool connectionPool = getConnectionPool();
          final Channel channel = connectionPool.reserveLongTermConnection(controllerWorkerID);
          // what for?
          final TupleBatchBuffer buffer = new TupleBatchBuffer(getEOIReportSchema());
          buffer.put(0, selfIDBID);
          buffer.put(1, selfWorkerID);
          buffer.put(2, tuplesSentSinceLastEOI);
          final ExchangePairID operatorID = operatorIDs[0];
          channel.write(buffer.popAnyAsTM(operatorID));
          tuplesSentSinceLastEOI = 0;
        }
      } catch (DbException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child1, child2, child3 };
  }

  @Override
  public Schema getSchema() {
    return outputSchema;
  }

  public Schema getEOIReportSchema() {
    final ImmutableList<Type> types = ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE);
    final ImmutableList<String> columnNames = ImmutableList.of("idbID", "workerID", "numNewTuples");
    final Schema schema = new Schema(types, columnNames);
    return schema;
  }

  @Override
  public void init() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child1 = children[0];
    child2 = children[1];
    child3 = children[2];
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    return null;
  }

}
