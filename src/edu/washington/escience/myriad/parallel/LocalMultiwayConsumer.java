package edu.washington.escience.myriad.parallel;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.operator.Operator;

/**
 * The consumer part of the Collect Exchange operator.
 * 
 * A Collect operator collects tuples from all the workers. There is a collect producer on each worker, and a collect
 * consumer on the server and a master worker if a master worker is needed.
 * 
 * The consumer passively collects Tuples from all the paired LocalMultiwayProducers
 * 
 */
public final class LocalMultiwayConsumer extends Consumer {

  /** The logger for this class. Defaults to myriad level, but could be set to a finer granularity if needed. */
  private static final Logger LOGGER = LoggerFactory.getLogger("edu.washington.escience.myriad");

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private final Schema schema;
  private final BitSet workerEOS;
  private final int[] sourceWorkers;
  // private final boolean finish = false;
  private final Map<Integer, Integer> workerIdToIndex;

  /**
   * The child of a LocalMultiwayConsumer must be a paired LocalMultiwayProducer.
   */
  private LocalMultiwayProducer child;

  /**
   * When a child is provided, the Schema is the child's Schema.
   * 
   * @throws DbException
   */
  public LocalMultiwayConsumer(final LocalMultiwayProducer child, final ExchangePairID operatorID, final int[] workerIDs)
      throws DbException {
    super(operatorID);
    this.child = child;
    schema = child.getSchema();
    sourceWorkers = workerIDs;
    workerIdToIndex = new HashMap<Integer, Integer>();
    int idx = 0;
    for (final int w : workerIDs) {
      workerIdToIndex.put(w, idx++);
    }
    workerEOS = new BitSet(workerIDs.length);
  }

  /**
   * If there's no child operator, a Schema is needed.
   */
  public LocalMultiwayConsumer(final Schema schema, final ExchangePairID operatorID, final int[] workerIDs) {
    super(operatorID);
    this.schema = schema;
    sourceWorkers = workerIDs;
    workerIdToIndex = new HashMap<Integer, Integer>();
    int idx = 0;
    for (final int w : workerIDs) {
      workerIdToIndex.put(w, idx++);
    }
    workerEOS = new BitSet(workerIDs.length);
  }

  @Override
  public void cleanup() {
    setInputBuffer(null);
    workerEOS.clear();
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
    try {
      return getTuples(true);
    } catch (final InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
      throw new DbException(e);
    }
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public Schema getSchema() {
    if (child != null) {
      return child.getSchema();
    } else {
      return schema;
    }
  }

  private TupleBatch getTuples(final boolean blocking) throws InterruptedException {

    int timeToWait = -1;
    if (!blocking) {
      timeToWait = 0;
    }

    ExchangeData tb = null;
    TupleBatch result = null;
    while (workerEOS.nextClearBit(0) < sourceWorkers.length) {
      tb = take(timeToWait);
      if (tb != null) {
        if (tb.isEos()) {
          workerEOS.set(workerIdToIndex.get(tb.getWorkerID()));
          LOGGER.debug("EOS received in LocalMultiwayConsumer. From WorkerID:" + tb.getWorkerID());
        } else {
          result = tb.getRealData();
          break;
        }
      }
    }
    // have received all the eos message from all the workers
    // finish = true;
    if (result == null) {
      setEOS();
    }
    return result;
  }

  @Override
  public void init() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = (LocalMultiwayProducer) children[0];
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    if (!eos()) {
      try {
        return getTuples(false);
      } catch (final InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
        throw new DbException(e.getLocalizedMessage());
      }
    }
    return null;

  }
}
