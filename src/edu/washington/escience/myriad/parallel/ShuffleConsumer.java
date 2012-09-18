package edu.washington.escience.myriad.parallel;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * The consumer part of the Shuffle Exchange operator.
 * 
 * A ShuffleProducer operator sends tuples to all the workers according to some PartitionFunction, while the
 * ShuffleConsumer (this class) encapsulates the methods to collect the tuples received at the worker from multiple
 * source workers' ShuffleProducer.
 * 
 * */
public class ShuffleConsumer extends Consumer {

  private static final long serialVersionUID = 1L;

//  private transient Iterator<_TupleBatch> tuples;
//  private transient int innerBufferIndex;
  private boolean finish;

  // Used to remember which of the source workers have sent an end of stream
  // message.
  private final BitSet workerEOS;

  private final SocketInfo[] sourceWorkers;
  private final HashMap<String, Integer> workerIdToIndex;
  private ShuffleProducer child;
  private Schema schema;

  public String getName() {
    return "shuffle_c";
  }

  public ShuffleConsumer(ShuffleProducer child, ExchangePairID operatorID, SocketInfo[] workers,
      _TupleBatch outputBuffer) {
    super(operatorID);
    this.child = child;
    this.sourceWorkers = workers;
    this.workerIdToIndex = new HashMap<String, Integer>();
    int i = 0;
    for (SocketInfo w : this.sourceWorkers)
      this.workerIdToIndex.put(w.getId(), i++);
    this.workerEOS = new BitSet(workers.length);
    this.outputBuffer = outputBuffer;
    this.finish = false;
  }

  @Override
  public void open() throws DbException {
//    this.tuples = null;
    // this.innerBuffer = new ArrayList<_TupleBatch>();
//    this.innerBufferIndex = 0;
    if (this.child != null)
      this.child.open();
    super.open();
  }

  // @Override
  // public void rewind() throws DbException{
  // // Note that we store the tuples we read in the first run in memory and
  // // reuse them when fetchNext() is called after a rewind.
  // this.tuples = null;
  // this.innerBufferIndex = 0;
  // }

  @Override
  public void close() {
    super.close();
//    tuples = null;
    this.workerEOS.clear();
  }

  /**
   * 
   * Retrieve a batch of tuples from the buffer of ExchangeMessages. Wait if the buffer is empty.
   * 
   * @return Iterator over the new tuples received from the source workers. Return <code>null</code> if all source
   *         workers have sent an end of file message.
   */
  _TupleBatch getTuples() throws InterruptedException {
    ExchangeTupleBatch tb = null;

    while (this.workerEOS.nextClearBit(0) < this.sourceWorkers.length) {
      tb = (ExchangeTupleBatch) this.take(-1);
      if (tb.isEos()) {
        this.workerEOS.set(this.workerIdToIndex.get(tb.getWorkerID()));
      } else {
        outputBuffer.append(tb);
        // this.innerBufferIndex++;
        // return tb.iterator();
      }
    }
    // have received all the eos message from all the workers
    finish = true;

    return outputBuffer;
  }

  @Override
  protected _TupleBatch fetchNext() throws DbException {
    if (!finish) {
      try {
        return getTuples();
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new DbException(e.getLocalizedMessage());
      }
      // if (tuples == null) // finish
      // return null;
    }
    // return tuples.next();
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { this.child };
  }

  @Override
  public void setChildren(Operator[] children) {
    this.child = (ShuffleProducer) children[0];
  }

  @Override
  public Schema getSchema() {
    if (this.child != null)
      return this.child.getSchema();
    else
      return this.schema;
  }

}
