package edu.washington.escience.myriad.parallel;

import java.util.Objects;

import org.apache.mina.core.session.IoSession;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage.DataMessageType;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * This operator simply drops all child output on the floor until the child reaches EOS. Then the EosProducer sends the
 * Eos message to the corresponding EosConsumer.
 */
public final class EosProducer extends Producer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The child. */
  private Operator child;
  /** The paired EosConsumer address. */
  private final int workerId;

  /**
   * Constructs an EosProducer. This operator simply drops all child output on the floor until the child reaches EOS.
   * Then the EosProducer sends the Eos to the EosConsumer.
   * 
   * @param child the child operator this EosProducer is waiting for.
   * @param exchangeId the identifier of this EosProducer->EosConsumer pipe.
   * @param workerId the identifier of this worker.
   */
  public EosProducer(final Operator child, final ExchangePairID exchangeId, final int workerId) {
    super(exchangeId);
    Objects.requireNonNull(exchangeId);
    this.child = child;
    this.workerId = workerId;
  }

  @Override
  public void cleanup() {
  }

  @Override
  protected _TupleBatch fetchNext() throws DbException {
    final IoSession session = getConnectionPool().get(workerId, null, 3, null);
    final DataMessage eos =
        DataMessage.newBuilder().setType(DataMessageType.EOS).setOperatorID(operatorID.getLong()).build();
    final TransportMessage eosMessage =
        TransportMessage.newBuilder().setType(TransportMessageType.DATA).setData(eos).build();

    /* Actually, it's fine if child is null. Just send Eos right away. */
    if (child != null) {
      /* Loop until child is eos() */
      while (!child.eos()) {
        child.next();
      }
    }

    /* Send the Eos message. */
    session.write(eosMessage);
    setEOS();

    return null;
  }

  @Override
  public _TupleBatch fetchNextReady() throws DbException {
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public Schema getSchema() {
    return child.getSchema();
  }

  @Override
  public void init() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }
}
