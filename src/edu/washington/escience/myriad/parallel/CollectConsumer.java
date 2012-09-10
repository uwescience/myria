package edu.washington.escience.myriad.parallel;

// import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
// import java.util.Iterator;

// import edu.washington.escience.ConcurrentInMemoryTupleBatch;
import edu.washington.escience.myriad.Schema;
// import edu.washington.escience.table.DbIterateReader;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * The consumer part of the Collect Exchange operator.
 * 
 * A Collect operator collects tuples from all the workers. There is a collect producer on each worker, and a collect
 * consumer on the server and a master worker if a master worker is needed.
 * 
 * The consumer passively collects Tuples from all the paired CollectProducers
 * 
 * */
public class CollectConsumer extends Consumer {

  private static final long serialVersionUID = 1L;

  // private transient Iterator<TupleBatch> tuples;

  /**
   * innerBufferIndex and innerBuffer are used to buffer all the TupleBags this operator has received. We need this
   * because we need to support rewind.
   * */
  private final _TupleBatch outputBuffer;

  private Schema schema;
  private final BitSet workerEOS;
  private final SocketInfo[] sourceWorkers;
  private final HashMap<String, Integer> workerIdToIndex;
  private boolean finish = false;
  /**
   * The child of a CollectConsumer must be a paired CollectProducer.
   * */
  private CollectProducer child;

  public String getName() {
    return "collect_c";
  }

  /**
   * If there's no child operator, a TupleDesc is needed
   * */
  public CollectConsumer(Schema schema, ExchangePairID operatorID, SocketInfo[] workers, _TupleBatch outputBuffer) {
    super(operatorID);
    this.schema = schema;
    this.sourceWorkers = workers;
    this.workerIdToIndex = new HashMap<String, Integer>();
    int idx = 0;
    for (SocketInfo w : workers) {
      this.workerIdToIndex.put(w.getId(), idx++);
    }
    this.workerEOS = new BitSet(workers.length);
    this.outputBuffer = outputBuffer;
  }

  /**
   * If a child is provided, the TupleDesc is the child's TD
   * */
  public CollectConsumer(CollectProducer child, ExchangePairID operatorID, SocketInfo[] workers,
      _TupleBatch outputBuffer) {
    super(operatorID);
    this.child = child;
    this.schema = child.getSchema();
    this.sourceWorkers = workers;
    this.workerIdToIndex = new HashMap<String, Integer>();
    int idx = 0;
    for (SocketInfo w : workers) {
      this.workerIdToIndex.put(w.getId(), idx++);
    }
    this.workerEOS = new BitSet(workers.length);
    this.outputBuffer = outputBuffer;
  }

  @Override
  public void open() throws DbException {
    // this.tuples = null;
    // this.innerBuffer = new ConcurrentInMemoryTupleBatch(this.child.getSchema());
    // this.innerBufferIndex = 0;
    if (this.child != null)
      this.child.open();
    super.open();
  }

  // @Override
  // public void rewind() throws DbException {
  // // this.tuples = null;
  // // this.innerBufferIndex = 0;
  // this.finish = false;
  // }

  public void close() {
    super.close();
    this.setBuffer(null);
    // this.tuples = null;
    // this.innerBufferIndex = -1;
    // this.innerBuffer = null;
    // this.innerBuffer.
    this.workerEOS.clear();
  }

  _TupleBatch getTuples() throws InterruptedException {
    ExchangeTupleBatch tb = null;
    // if (this.innerBufferIndex < this.innerBuffer.size())
    // return this.innerBuffer.get(this.innerBufferIndex++).iterator();

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
    this.child = (CollectProducer) children[0];
    if (this.child != null)
      this.schema = this.child.getSchema();
  }

  @Override
  public Schema getSchema() {
    if (this.child != null)
      return this.child.getSchema();
    else
      return this.schema;
  }
}
