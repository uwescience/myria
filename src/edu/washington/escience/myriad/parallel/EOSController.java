package edu.washington.escience.myriad.parallel;

import java.util.ArrayList;

import org.jboss.netty.channel.Channel;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;

/**
 * The producer part of the Shuffle Exchange operator.
 * 
 * EOSController distributes tuples to the workers according to some partition function (provided as a PartitionFunction
 * object during the EOSController's instantiation).
 * 
 */
public class EOSController extends Producer {

  final class WorkingThread extends Thread {
    @Override
    public void run() {

      final int numWorker = workerIDs.length;
      final int numIDB = idbOpIDs.length;
      final Channel[] channels = new Channel[numWorker];
      final IPCConnectionPool connectionPool = getConnectionPool();
      int index = 0;
      for (final int workerID : workerIDs) {
        channels[index] = connectionPool.reserveLongTermConnection(workerID);
        index++;
      }

      System.out.println("EOSController running " + numIDB + " " + numWorker + " " + idbOpIDs[0]);
      final ArrayList<Integer> zeroCol = new ArrayList<Integer>();
      final int[][] numEOI = new int[numIDB][numWorker];
      try {
        TupleBatch tb = null;
        while ((tb = child.next()) != null) {
          for (int i = 0; i < tb.numTuples(); ++i) {
            int idbID = tb.getInt(0, i);
            int workerID = tb.getInt(1, i) - 1;
            int numNewTuples = tb.getInt(2, i);
            if (numEOI[idbID][workerID] >= zeroCol.size()) {
              zeroCol.add(0);
            }
            int tmp = zeroCol.get(numEOI[idbID][workerID]);
            if (numNewTuples != 0) {
              zeroCol.set(numEOI[idbID][workerID], -1);
            } else if (tmp >= 0) {
              zeroCol.set(numEOI[idbID][workerID], tmp + 1);
              if (tmp + 1 == numIDB * numWorker) {
                TupleBatchBuffer tbb = new TupleBatchBuffer(getEOSReportSchema());
                for (int k = 0; k < numIDB; k++) {
                  tbb.put(0, true);
                  TransportMessage tm = tbb.popAnyAsTM(idbOpIDs[k]);
                  for (int j = 0; j < numWorker; j++) {
                    channels[j].write(tm);
                  }
                }
                return;
              }
            }
            numEOI[idbID][workerID]++;
          }
        }
      } catch (final DbException e) {
        e.printStackTrace();
      } finally {
        System.out.println("releaing!");
        for (final Channel ch : channels) {
          connectionPool.releaseLongTermConnection(ch);
        }
        System.out.println("released!");
      }

    }
  }

  public Schema getEOSReportSchema() {
    final ImmutableList<Type> types = ImmutableList.of(Type.BOOLEAN_TYPE);
    final ImmutableList<String> columnNames = ImmutableList.of("EOS");
    final Schema schema = new Schema(types, columnNames);
    return schema;
  }

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private transient WorkingThread runningThread;
  private final int[] workerIDs;
  private Operator child;
  private final ExchangePairID[] idbOpIDs;

  public EOSController(final Operator child, final ExchangePairID operatorID, final ExchangePairID[] idbOpIDs,
      final int[] workerIDs) {
    super(operatorID);
    this.child = child;
    this.workerIDs = workerIDs;
    this.idbOpIDs = idbOpIDs;
  }

  @Override
  public final void cleanup() {
  }

  @Override
  protected final TupleBatch fetchNext() throws DbException {
    try {
      runningThread.join();
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    return fetchNext();
  }

  @Override
  public final Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public final Schema getSchema() {
    return child.getSchema();
  }

  @Override
  public final void init() throws DbException {
    runningThread = new WorkingThread();
    runningThread.start();
  }

  @Override
  public final void setChildren(final Operator[] children) {
    child = children[0];
  }

}
