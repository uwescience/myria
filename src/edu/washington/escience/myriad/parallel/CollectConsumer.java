package edu.washington.escience.myriad.parallel;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import edu.washington.escience.myriad.Schema;
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

  // /**
  // * innerBufferIndex and innerBuffer are used to buffer all the TupleBags this operator has received. We need this
  // * because we need to support rewind.
  // * */
  // private final _TupleBatch outputBuffer;

  private Schema schema;
  private final BitSet workerEOS;
  private final int[] sourceWorkers;
  private boolean finish = false;
  private final Map<Integer, Integer> workerIdToIndex;

  /**
   * The child of a CollectConsumer must be a paired CollectProducer.
   * */
  private CollectProducer child;

  @Override
  public String getName() {
    return "collect_c";
  }

  /**
   * If there's no child operator, a TupleDesc is needed
   * */
  public CollectConsumer(Schema schema, ExchangePairID operatorID, int[] workerIDs) {
    super(operatorID);
    this.schema = schema;
    this.sourceWorkers = workerIDs;
    this.workerIdToIndex = new HashMap<Integer, Integer>();
    int idx = 0;
    for (int w : workerIDs) {
      this.workerIdToIndex.put(w, idx++);
    }
    this.workerEOS = new BitSet(workerIDs.length);
  }

  /**
   * If a child is provided, the TupleDesc is the child's TD
   * */
  public CollectConsumer(CollectProducer child, ExchangePairID operatorID, int[] workerIDs) {
    super(operatorID);
    this.child = child;
    this.schema = child.getSchema();
    this.sourceWorkers = workerIDs;
    this.workerIdToIndex = new HashMap<Integer, Integer>();
    int idx = 0;
    for (int w : workerIDs) {
      this.workerIdToIndex.put(w, idx++);
    }
    this.workerEOS = new BitSet(workerIDs.length);
  }

  @Override
  public void open() throws DbException {
    if (this.child != null)
      this.child.open();
    super.open();
  }

  @Override
  public void close() {
    super.close();
    this.setInputBuffer(null);
    this.workerEOS.clear();
  }

  _TupleBatch getTuples() throws InterruptedException {
    ExchangeTupleBatch tb = null;

    while (this.workerEOS.nextClearBit(0) < this.sourceWorkers.length) {
      tb = this.take(-1);
      if (tb.isEos()) {
        this.workerEOS.set(this.workerIdToIndex.get(tb.getWorkerID()));
      } else {
        return tb.getRealData();
      }
    }
    // have received all the eos message from all the workers
    this.finish = true;
    return null;
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
    }
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
