package edu.washington.escience.myriad.parallel;

import java.util.Arrays;
import java.util.List;

import org.apache.mina.core.session.IoSession;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage.DataMessageType;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * The producer part of the Collect Exchange operator.
 * 
 * The producer actively pushes the tuples generated by the child operator to the paired CollectConsumer.
 * 
 */
public final class CollectProducer extends Producer {

  /**
   * The working thread, which executes the child operator and send the tuples to the paired CollectConsumer operator.
   */
  class WorkingThread extends Thread {
    @Override
    public void run() {

      // IoSession session =
      // ParallelUtility.createDataConnection(ParallelUtility.createSession(CollectProducer.this.collectConsumerAddr,
      // CollectProducer.this.getThisWorker().minaHandler, -1), CollectProducer.this.operatorID,
      // CollectProducer.this.getThisWorker().workerID);
      final IoSession session =
          CollectProducer.this.getThisWorker().connectionPool.get(CollectProducer.this.collectConsumerWorkerID, null,
              3, null);

      final TransportMessage.Builder messageBuilder = TransportMessage.newBuilder();

      // session.write(messageBuilder.setType(TransportMessageType.CONTROL).setControl(
      // ControlMessage.newBuilder().setType(ControlMessageType.CONNECT).build()).build());

      try {

        while (CollectProducer.this.child.hasNext()) {
          final _TupleBatch tup = CollectProducer.this.child.next();
          final List<Column> columns = tup.outputRawData();
          // ExchangeTupleBatch toSend =
          // new ExchangeTupleBatch(CollectProducer.this.operatorID, CollectProducer.this.getThisWorker().workerID,
          // tup.outputRawData(), CollectProducer.this.getSchema(), tup.numOutputTuples());

          final ColumnMessage[] columnProtos = new ColumnMessage[columns.size()];
          {
            int i = 0;
            for (final Column c : columns) {
              columnProtos[i] = c.serializeToProto();
              i++;
            }
          }

          session.write(messageBuilder.setType(TransportMessageType.DATA).setData(
              DataMessage.newBuilder().setType(DataMessageType.NORMAL).addAllColumns(Arrays.asList(columnProtos))
                  .setOperatorID(CollectProducer.this.operatorID.getLong()).build()).build());
        }

        final DataMessage eos =
            DataMessage.newBuilder().setType(DataMessageType.EOS).setOperatorID(
                CollectProducer.this.operatorID.getLong()).build();
        session.write(messageBuilder.setType(TransportMessageType.DATA).setData(eos).build());
        //
        // new ExchangeTupleBatch(CollectProducer.this.operatorID, CollectProducer.this.getThisWorker().workerID))
        // .addListener(new IoFutureListener<WriteFuture>() {
        //
        // @Override
        // public void operationComplete(WriteFuture future) {
        // ParallelUtility.closeSession(future.getSession());
        // }
        // });// .awaitUninterruptibly(); //wait until all the data have successfully transfered

      } catch (final DbException e) {
        e.printStackTrace();
      }
      // }
    }
  }

  private static final long serialVersionUID = 1L;

  private transient WorkingThread runningThread;
  public static final int MAX_SIZE = 100;
  public static final int MIN_SIZE = 100;

  public static final int MAX_MS = 1000;
  /**
   * The paired collect consumer address
   */
  private final int collectConsumerWorkerID;

  private Operator child;

  public CollectProducer(final Operator child, final ExchangePairID operatorID, final int collectConsumerWorkerID) {
    super(operatorID);
    this.child = child;
    this.collectConsumerWorkerID = collectConsumerWorkerID;
  }

  // public InetSocketAddress getCollectServerAddr() {
  // return this.collectConsumerAddr;
  // }

  @Override
  public void close() {
    super.close();
    child.close();
  }

  @Override
  protected _TupleBatch fetchNext() throws DbException {
    try {
      // wait until the working thread terminate and return an empty tuple set
      runningThread.join();
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { this.child };
  }

  // @Override
  // public void rewind() throws DbException {
  // throw new UnsupportedOperationException();
  // }

  @Override
  public String getName() {
    return "collect_p";
  }

  @Override
  public Schema getSchema() {
    return this.child.getSchema();
  }

  @Override
  public void open() throws DbException {
    this.child.open();
    this.runningThread = new WorkingThread();
    this.runningThread.start();
    super.open();
  }

  @Override
  public void setChildren(final Operator[] children) {
    this.child = children[0];
  }

}
