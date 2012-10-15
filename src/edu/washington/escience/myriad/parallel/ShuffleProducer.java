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
      final int numWorker = workerIDs.length;
      final IoSession[] shuffleSessions = new IoSession[numWorker];
      int index = 0;
      for (final int workerID : workerIDs) {
        shuffleSessions[index] = getThisWorker().connectionPool.get(workerID, null, 3, null);
        index++;
      }
      final Schema thisSchema = getSchema();

      try {
        TupleBatchBuffer[] buffers = new TupleBatchBuffer[numWorker];
        for (int i = 0; i < numWorker; i++) {
          buffers[i] = new TupleBatchBuffer(thisSchema);
        }
        while (child.hasNext()) {
          final _TupleBatch tup = child.next();
          buffers = tup.partition(partitionFunction, buffers);
          for (int p = 0; p < numWorker; p++) {
            final TupleBatchBuffer etb = buffers[p];
            TupleBatch tb = null;
            while ((tb = etb.pop()) != null) {
              final List<Column> columns = tb.outputRawData();

              final ColumnMessage[] columnProtos = new ColumnMessage[columns.size()];
              int i = 0;
              for (final Column c : columns) {
                columnProtos[i] = c.serializeToProto();
                i++;
              }
              shuffleSessions[p].write(messageBuilder.setType(TransportMessageType.DATA).setData(
                  DataMessage.newBuilder().setType(DataMessageType.NORMAL).addAllColumns(Arrays.asList(columnProtos))
                      .setOperatorID(ShuffleProducer.this.operatorID.getLong()).build()).build());
            }
          }
        }

        for (int i = 0; i < numWorker; i++) {
          TupleBatchBuffer tbb = buffers[i];
          if (tbb.numTuples() > 0) {
            List<TupleBatch> remain = tbb.getOutput();
            for (TupleBatch tb : remain) {
              final List<Column> columns = tb.outputRawData();
              final ColumnMessage[] columnProtos = new ColumnMessage[columns.size()];
              int j = 0;
              for (final Column c : columns) {
                columnProtos[j] = c.serializeToProto();
                j++;
              }
              shuffleSessions[i].write(messageBuilder.setType(TransportMessageType.DATA).setData(
                  DataMessage.newBuilder().setType(DataMessageType.NORMAL).addAllColumns(Arrays.asList(columnProtos))
                      .setOperatorID(ShuffleProducer.this.operatorID.getLong()).build()).build());
            }
          }
        }

      } catch (final DbException e) {
        e.printStackTrace();
      }

      final DataMessage eos =
          DataMessage.newBuilder().setType(DataMessageType.EOS)
              .setOperatorID(ShuffleProducer.this.operatorID.getLong()).build();
      for (int i = 0; i < numWorker; i++) {

        shuffleSessions[i].write(messageBuilder.setType(TransportMessageType.DATA).setData(eos).build());
      }
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
    partitionFunction = pf;
  }

  @Override
  public final void close() {
    super.close();
    child.close();
  }

  @Override
  protected final _TupleBatch fetchNext() throws DbException {
    try {
      runningThread.join();
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public final Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public final String getName() {
    return "shuffle_p";
  }

  public final PartitionFunction<?, ?> getPartitionFunction() {
    return partitionFunction;
  }

  @Override
  public final Schema getSchema() {
    return child.getSchema();
  }

  final IoSession getSession(final IoSession[] oldS, final IoConnector[] oldC, final int current) {
    if (oldS[current] == null || !oldS[current].isConnected()) {
      return oldS[current] = oldC[current].connect().awaitUninterruptibly().getSession();
    }
    return oldS[current];
  }

  @Override
  public final void open() throws DbException {
    child.open();
    runningThread = new WorkingThread();
    runningThread.start();
    super.open();
  }

  @Override
  public final void setChildren(final Operator[] children) {
    child = children[0];
  }

  public final void setPartitionFunction(final PartitionFunction<?, ?> pf) {
    partitionFunction = pf;
  }

}
