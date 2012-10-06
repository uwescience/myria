package edu.washington.escience.myriad.parallel;

import java.util.Arrays;
import java.util.List;

import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.session.IoSession;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage.DataMessageType;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * The producer part of the Shuffle Exchange operator.
 * 
 * ShuffleProducer distributes tuples to the workers according to some partition function (provided as a
 * PartitionFunction object during the ShuffleProducer's instantiation).
 * 
 */
public class ShuffleProducer extends Producer {

  class WorkingThread extends Thread {
    @Override
    public void run() {

      final TransportMessage.Builder messageBuilder = TransportMessage.newBuilder();
      final int numWorker = ShuffleProducer.this.workerIDs.length;
      final IoSession[] shuffleSessions = new IoSession[numWorker];
      int index = 0;
      for (final int workerID : ShuffleProducer.this.workerIDs) {
        shuffleSessions[index] = ShuffleProducer.this.getThisWorker().connectionPool.get(workerID, null, 3, null);
        // ParallelUtility.createDataConnection(ParallelUtility.createSession(worker.getAddress(),
        // ShuffleProducer.this.getThisWorker().minaHandler, -1), ShuffleProducer.this.operatorID,
        // ShuffleProducer.this.getThisWorker().workerID);
        // shuffleSessions[index].write(messageBuilder.setType(TransportMessageType.DATA).setData(
        // DataMessage.newBuilder().setType(DataMessageType.EOS).setOperatorID(ShuffleProducer.this.operatorID.getLong()).build()));
        index++;
      }
      final Schema thisSchema = ShuffleProducer.this.getSchema();
      // String thisWorkerID = ShuffleProducer.this.getThisWorker().workerID;
      //
      // Exchange.ExchangePairID thisOID = ShuffleProducer.this.operatorID;

      try {
        while (ShuffleProducer.this.child.hasNext()) {
          final _TupleBatch tup = ShuffleProducer.this.child.next();
          TupleBatchBuffer[] buffers = new TupleBatchBuffer[numWorker];
          for (int i = 0; i < numWorker; i++) {
            buffers[i] = new TupleBatchBuffer(thisSchema);
          }
          buffers = tup.partition(partitionFunction, buffers);
          for (int p = 0; p < numWorker; p++) {
            final TupleBatchBuffer etb = buffers[p];
            if (etb.numTuples() > 0) {
              for (final TupleBatch tb : etb.getOutput()) {

                final List<Column> columns = tb.outputRawData();
                // ExchangeTupleBatch toSend =
                // new ExchangeTupleBatch(CollectProducer.this.operatorID,
                // CollectProducer.this.getThisWorker().workerID,
                // tup.outputRawData(), CollectProducer.this.getSchema(), tup.numOutputTuples());

                final ColumnMessage[] columnProtos = new ColumnMessage[columns.size()];
                int i = 0;
                for (final Column c : columns) {
                  columnProtos[i] = c.serializeToProto();
                  i++;
                }
                shuffleSessions[p].write(messageBuilder.setType(TransportMessageType.DATA).setData(
                    DataMessage.newBuilder().setType(DataMessageType.NORMAL).addAllColumns(Arrays.asList(columnProtos))
                        .setOperatorID(ShuffleProducer.this.operatorID.getLong()).build()).build());

                // ExchangeTupleBatch toSend =
                // new ExchangeTupleBatch(thisOID, thisWorkerID, tb.outputRawData(), ShuffleProducer.this.getSchema(),
                // tb.numOutputTuples());
                // // ShuffleProducer.this.getSession
                // shuffleSessions[p].write(toSend);
              }
            }
          }
        }

      } catch (final DbException e) {
        e.printStackTrace();
      }

      final DataMessage eos =
          DataMessage.newBuilder().setType(DataMessageType.EOS)
              .setOperatorID(ShuffleProducer.this.operatorID.getLong()).build();
      // try {
      for (int i = 0; i < numWorker; i++) {
        // if (buffers[i].numOutputTuples() > 0) {
        // new ExchangeTupleBatch(ShuffleProducer.this.operatorID, ShuffleProducer.this.getThisWorker().workerID,
        // buffers[i].outputRawData(), ShuffleProducer.this.getSchema(), buffers[i].numOutputTuples());
        // }

        shuffleSessions[i].write(messageBuilder.setType(TransportMessageType.DATA).setData(eos).build());
        // .addListener(
        // new IoFutureListener<WriteFuture>() {
        //
        // @Override
        // public void operationComplete(WriteFuture future) {
        // ParallelUtility.closeSession(future.getSession());
        // }
        // });
      }
      // } catch (DbException e) {
      // e.printStackTrace();
      // }
    }
  }

  private static final long serialVersionUID = 1L;

  private transient WorkingThread runningThread;
  private final int[] workerIDs;

  private PartitionFunction<?, ?> partitionFunction;

  private Operator child;

  public ShuffleProducer(final Operator child, final ExchangePairID operatorID, final int[] workerIDs,
      final PartitionFunction<?, ?> pf) {
    super(operatorID);
    this.child = child;
    this.workerIDs = workerIDs;
    this.partitionFunction = pf;
  }

  @Override
  public final void close() {
    super.close();
    child.close();
  }

  @Override
  protected final _TupleBatch fetchNext() throws DbException {
    try {
      this.runningThread.join();
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public final Operator[] getChildren() {
    return new Operator[] { this.child };
  }

  @Override
  public final String getName() {
    return "shuffle_p";
  }

  public final PartitionFunction<?, ?> getPartitionFunction() {
    return this.partitionFunction;
  }

  @Override
  public final Schema getSchema() {
    return this.child.getSchema();
  }

  // @Override
  // public void rewind() throws DbException, TransactionAbortedException {
  // throw new UnsupportedOperationException();
  // }

  final IoSession getSession(final IoSession[] oldS, final IoConnector[] oldC, final int current) {
    if (oldS[current] == null || !oldS[current].isConnected()) {
      return oldS[current] = oldC[current].connect().awaitUninterruptibly().getSession();
    }
    return oldS[current];
  }

  @Override
  public final void open() throws DbException {
    this.child.open();
    this.runningThread = new WorkingThread();
    this.runningThread.start();
    super.open();
  }

  @Override
  public final void setChildren(final Operator[] children) {
    this.child = children[0];
  }

  public final void setPartitionFunction(final PartitionFunction<?, ?> pf) {
    this.partitionFunction = pf;
  }

}
