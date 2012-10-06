package edu.washington.escience.myriad.parallel;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * The consumer part of the Collect Exchange operator.
 * 
 * A Collect operator collects tuples from all the workers. There is a collect producer on each worker, and a collect
 * consumer on the server and a master worker if a master worker is needed.
 * 
 * The consumer passively collects Tuples from all the paired CollectProducers
 * 
 */
public final class CollectConsumer extends Consumer {

  private static final long serialVersionUID = 1L;

  // private transient Iterator<TupleBatch> tuples;

  // /**
  // * innerBufferIndex and innerBuffer are used to buffer all the TupleBags this operator has received. We need this
  // * because we need to support rewind.
  // */
  // private final _TupleBatch outputBuffer;

  private Schema schema;
  private final BitSet workerEOS;
  private final int[] sourceWorkers;
  private boolean finish = false;
  private final Map<Integer, Integer> workerIdToIndex;

  /**
   * The child of a CollectConsumer must be a paired CollectProducer.
   */
  private CollectProducer child;

  /**
   * If a child is provided, the TupleDesc is the child's TD
   */
  public CollectConsumer(final CollectProducer child, final ExchangePairID operatorID, final int[] workerIDs) {
    super(operatorID);
    this.child = child;
    this.schema = child.getSchema();
    this.sourceWorkers = workerIDs;
    this.workerIdToIndex = new HashMap<Integer, Integer>();
    int idx = 0;
    for (final int w : workerIDs) {
      this.workerIdToIndex.put(w, idx++);
    }
    this.workerEOS = new BitSet(workerIDs.length);
  }

  /**
   * If there's no child operator, a TupleDesc is needed
   */
  public CollectConsumer(final Schema schema, final ExchangePairID operatorID, final int[] workerIDs) {
    super(operatorID);
    this.schema = schema;
    this.sourceWorkers = workerIDs;
    this.workerIdToIndex = new HashMap<Integer, Integer>();
    int idx = 0;
    for (final int w : workerIDs) {
      this.workerIdToIndex.put(w, idx++);
    }
    this.workerEOS = new BitSet(workerIDs.length);
  }

  @Override
  public void close() {
    super.close();
    this.setInputBuffer(null);
    this.workerEOS.clear();
  }

  @Override
  protected _TupleBatch fetchNext() throws DbException {
    if (!finish) {
      try {
        return getTuples();
      } catch (final InterruptedException e) {
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
  public String getName() {
    return "collect_c";
  }

  @Override
  public Schema getSchema() {
    if (this.child != null) {
      return this.child.getSchema();
    } else {
      return this.schema;
    }
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
  public void open() throws DbException {
    if (this.child != null) {
      this.child.open();
    }
    super.open();
  }

  @Override
  public void setChildren(final Operator[] children) {
    this.child = (CollectProducer) children[0];
    if (this.child != null) {
      this.schema = this.child.getSchema();
    }
  }
}
