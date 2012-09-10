package edu.washington.escience.myriad.parallel;

// import java.util.ArrayList;

import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.session.IoSession;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * The producer part of the Shuffle Exchange operator.
 * 
 * ShuffleProducer distributes tuples to the workers according to some partition function (provided as a
 * PartitionFunction object during the ShuffleProducer's instantiation).
 * 
 * */
public class ShuffleProducer extends Producer {

  private static final long serialVersionUID = 1L;

  transient private WorkingThread runningThread;

  private final SocketInfo[] workers;
  private PartitionFunction<?, ?> partitionFunction;

  private Operator child;

  public String getName() {
    return "shuffle_p";
  }

  public ShuffleProducer(Operator child, ExchangePairID operatorID, SocketInfo[] workers, PartitionFunction<?, ?> pf) {
    super(operatorID);
    this.child = child;
    this.workers = workers;
    this.partitionFunction = pf;
  }

  public void setPartitionFunction(PartitionFunction<?, ?> pf) {
    this.partitionFunction = pf;
  }

  public SocketInfo[] getWorkers() {
    return this.workers;
  }

  public PartitionFunction<?, ?> getPartitionFunction() {
    return this.partitionFunction;
  }

  IoSession getSession(IoSession[] oldS, IoConnector[] oldC, int current) {
    if (oldS[current] == null || !oldS[current].isConnected())
      return oldS[current] = oldC[current].connect().awaitUninterruptibly().getSession();
    return oldS[current];
  }

  class WorkingThread extends Thread {
    public void run() {

      final int numWorker = ShuffleProducer.this.workers.length;
      IoSession[] shuffleSessions = new IoSession[numWorker];
      int index = 0;
      for (SocketInfo worker : ShuffleProducer.this.workers) {
        shuffleSessions[index] =
            ParallelUtility.createSession(worker.getAddress(), ShuffleProducer.this.getThisWorker().minaHandler, -1);
        index++;

      }
      Schema thisTD = ShuffleProducer.this.getSchema();
      String thisWorkerID = ShuffleProducer.this.getThisWorker().workerID;

      Exchange.ExchangePairID thisOID = ShuffleProducer.this.operatorID;

      try {
        while (ShuffleProducer.this.child.hasNext()) {
          _TupleBatch tup = ShuffleProducer.this.child.next();
          _TupleBatch[] buffers = new ConcurrentInMemoryTupleBatch[numWorker];
          for (int i = 0; i < numWorker; i++)
            buffers[i] = new ConcurrentInMemoryTupleBatch(thisTD);
          buffers = tup.partition(partitionFunction, buffers);
          for (int p = 0; p < numWorker; p++) {
            _TupleBatch etb = buffers[p];
            if (etb.numOutputTuples() > 0) {
              ExchangeTupleBatch toSend =
                  new ExchangeTupleBatch(thisOID, thisWorkerID, tup.outputRawData(), ShuffleProducer.this.getSchema(),
                      tup.numOutputTuples());
              // ShuffleProducer.this.getSession
              shuffleSessions[p].write(toSend);
            }
          }
        }

      } catch (DbException e) {
        e.printStackTrace();
      }

      // try {
      for (int i = 0; i < numWorker; i++) {
        // if (buffers[i].numOutputTuples() > 0) {
        // new ExchangeTupleBatch(ShuffleProducer.this.operatorID, ShuffleProducer.this.getThisWorker().workerID,
        // buffers[i].outputRawData(), ShuffleProducer.this.getSchema(), buffers[i].numOutputTuples());
        // }
        shuffleSessions[i].write(new ExchangeTupleBatch(thisOID, thisWorkerID)).addListener(
            new IoFutureListener<WriteFuture>() {

              @Override
              public void operationComplete(WriteFuture future) {
                ParallelUtility.closeSession(future.getSession());
              }
            });
      }
      // } catch (DbException e) {
      // e.printStackTrace();
      // }
    }
  }

  @Override
  public void open() throws DbException {
    this.child.open();
    this.runningThread = new WorkingThread();
    this.runningThread.start();
    super.open();
  }

  public void close() {
    super.close();
    child.close();
  }

  // @Override
  // public void rewind() throws DbException, TransactionAbortedException {
  // throw new UnsupportedOperationException();
  // }

  @Override
  protected _TupleBatch fetchNext() throws DbException {
    try {
      this.runningThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { this.child };
  }

  @Override
  public void setChildren(Operator[] children) {
    this.child = children[0];
  }

  @Override
  public Schema getSchema() {
    return this.child.getSchema();
  }

}
