package edu.washington.escience.myriad.parallel;

import java.util.ArrayList;

import org.jboss.netty.channel.Channel;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.ExchangeTupleBatch;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.operator.IDBInput;
import edu.washington.escience.myriad.operator.Merge;
import edu.washington.escience.myriad.util.IPCUtils;
import gnu.trove.impl.unmodifiable.TUnmodifiableIntIntMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

/**
 * The producer part of the Shuffle Exchange operator.
 * 
 * EOSController distributes tuples to the workers according to some partition function (provided as a PartitionFunction
 * object during the EOSController's instantiation).
 * 
 */
public class EOSController extends Producer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Recording the number of EOI received from each controlled {@link IDBInput}.
   * */
  private final int[][] numEOI;
  /**
   * 
   * */
  private final ArrayList<Integer> zeroCol;

  /**
   * If the number of empty reports at a time stamp is the same as this value, the iteration is done.
   * */
  private final int eosZeroColValue;
  /**
   * Mapping from workerID to index.
   * */
  private final TIntIntMap workerIdToIndex;

  // /**
  // * Mapping from EOSReceiver ID to index.
  // * */
  // private final TLongIntMap idbEOSReceiverIDToIndex;

  /**
   * Each worker in workerIDs has the whole array of idbOpIDS. So the total number of IDBInput operators are
   * idbOpIDs.length*workerIDs.length.
   * 
   * @param children The children are responsible for receiving EOI report from all controlled IDBInputs.
   * @param workerIDs the workers where the IDBInput operators resides
   * @param idbOpIDs the IDB operatorIDs in each Worker
   * */
  public EOSController(final Consumer[] children, final ExchangePairID[] idbOpIDs, final int[] workerIDs) {
    super(new Merge(children), idbOpIDs, workerIDs, false);
    numEOI = new int[idbOpIDs.length][workerIDs.length];
    zeroCol = new ArrayList<Integer>();
    eosZeroColValue = idbOpIDs.length * workerIDs.length;

    TIntIntMap tmp = new TIntIntHashMap();
    int idx = 0;
    for (int workerID : workerIDs) {
      tmp.put(workerID, idx++);
    }
    workerIdToIndex = new TUnmodifiableIntIntMap(tmp);

    // TLongIntMap tmp2 = new TLongIntHashMap();
    // idx = 0;
    // for (ExchangePairID idbID : idbOpIDs) {
    // tmp2.put(idbID.getLong(), idx++);
    // }
    // idbEOSReceiverIDToIndex = new TUnmodifiableLongIntMap(tmp2);
  }

  @Override
  protected final void consumeTuples(final TupleBatch otb) throws DbException {
    if (isEOSSent) {
      // after eos, do nothing.
      return;
    }
    ExchangeTupleBatch etb = (ExchangeTupleBatch) otb;
    for (int i = 0; i < etb.numTuples(); ++i) {
      // int idbIdx = idbIDToIndex.get(etb.getLong(0, i));
      int idbIdx = etb.getInt(0, i);
      boolean isEmpty = etb.getBoolean(1, i);
      int workerIdx = workerIdToIndex.get(etb.getSourceWorkerID());
      while (numEOI[idbIdx][workerIdx] >= zeroCol.size()) {
        zeroCol.add(0);
      }
      int tmp = zeroCol.get(numEOI[idbIdx][workerIdx]);
      if (!isEmpty) {
        zeroCol.set(numEOI[idbIdx][workerIdx], -1);
      } else if (tmp >= 0) {
        zeroCol.set(numEOI[idbIdx][workerIdx], tmp + 1);
        if (tmp + 1 == eosZeroColValue) {
          final Channel[] channels = super.getChannels();
          for (Channel ch : channels) {
            ch.write(IPCUtils.EOS); // directly emit an EOS makes more sense.
          }
          isEOSSent = true;
          return;
        }
      }
      numEOI[idbIdx][workerIdx]++;
    }
  }

  /**
   * EOS report schema.
   * */
  public static final Schema EOS_REPORT_SCHEMA = Schema.EMPTY_SCHEMA;

  // TODO add Root operator init and cleanup.
  /**
   * If the EOS messages are sent.
   * */
  private volatile boolean isEOSSent = false;

  @Override
  protected void childEOS() throws DbException {

  }

  @Override
  protected void childEOI() throws DbException {

  }

}
