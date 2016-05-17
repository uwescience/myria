package edu.washington.escience.myria.operator.network;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants.FTMode;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.IDBController;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnionAll;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.ExchangeTupleBatch;
import edu.washington.escience.myria.storage.TupleBatch;

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

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(EOSController.class);
  /**
   * Recording the number of EOI received from each controlled {@link IDBController}.
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
  private final ImmutableMap<Integer, Integer> workerIdToIndex;

  // /**
  // * Mapping from EOSReceiver ID to index.
  // * */
  // private final TLongIntMap idbEOSReceiverIDToIndex;

  /**
   * Each worker in workerIDs has the whole array of idbOpIDS. So the total number of IDBController operators are
   * idbOpIDs.length*workerIDs.length.
   *
   * @param child The child are responsible for receiving EOI report from all controlled IDBControllers.
   * @param workerIDs the workers where the IDBController operators resides
   * @param idbOpIDs the IDB operatorIDs in each Worker
   * */
  public EOSController(
      final UnionAll child, final ExchangePairID[] idbOpIDs, final int[] workerIDs) {
    super(null, idbOpIDs, workerIDs, false);
    if (child != null) {
      setChildren(new Operator[] {child});
    }
    numEOI = new int[idbOpIDs.length][workerIDs.length];
    zeroCol = new ArrayList<Integer>();
    eosZeroColValue = idbOpIDs.length * workerIDs.length;

    int idx = 0;
    HashMap<Integer, Integer> tmp = new HashMap<>();
    for (int workerId : workerIDs) {
      tmp.put(workerId, idx++);
    }
    workerIdToIndex = ImmutableMap.copyOf(tmp);
  }

  @Override
  protected final void consumeTuples(final TupleBatch otb) throws DbException {
    if (isEOSSent) {
      // after eos, do nothing.
      return;
    }

    int numExpecting = eosZeroColValue;
    if (getTaskResourceManager()
        .getFragment()
        .getLocalSubQuery()
        .getFTMode()
        .equals(FTMode.ABANDON)) {
      Set<Integer> missingWorkers =
          getTaskResourceManager().getFragment().getLocalSubQuery().getMissingWorkers();
      for (Integer id : missingWorkers) {
        if (workerIdToIndex.containsKey(id)) {
          int workerIdx = workerIdToIndex.get(id);
          for (int[] numWorkerReportedEOI : numEOI) {
            /* for this IDB, the array of the number of EOIs reported by each worker. */
            if (numWorkerReportedEOI[workerIdx] != -1
                && numWorkerReportedEOI[workerIdx] < zeroCol.size()) {
              int tmp = zeroCol.get(numWorkerReportedEOI[workerIdx]);
              if (tmp <= 0) {
                /* no effect */
                continue;
              }
              /* remove the report of this missing worker from the total count */
              zeroCol.set(numWorkerReportedEOI[workerIdx], tmp - 1);
              /* mark it as removed. */
              numWorkerReportedEOI[workerIdx] = -1;
            }
          }
        }
      }
      Set<Integer> expectingWorkers = new HashSet<Integer>();
      expectingWorkers.addAll(workerIdToIndex.keySet().asList());
      expectingWorkers.removeAll(missingWorkers);
      /* only count EOI reports from alive workers. */
      numExpecting = expectingWorkers.size() * numEOI.length;
    }
    ExchangeTupleBatch etb = (ExchangeTupleBatch) otb;
    for (int i = 0; i < etb.numTuples(); ++i) {
      int idbIdx = etb.getInt(0, i);
      int workerIdx = workerIdToIndex.get(etb.getSourceWorkerID());
      if (numEOI[idbIdx][workerIdx] == -1) {
        /* from a removed worker */
        continue;
      }
      boolean isEmpty = etb.getBoolean(1, i);
      while (numEOI[idbIdx][workerIdx] >= zeroCol.size()) {
        zeroCol.add(0);
      }
      int tmp = zeroCol.get(numEOI[idbIdx][workerIdx]);
      if (!isEmpty) {
        zeroCol.set(numEOI[idbIdx][workerIdx], -1);
      } else if (tmp >= 0) {
        tmp++;
        zeroCol.set(numEOI[idbIdx][workerIdx], tmp);
        if (tmp == numExpecting) {
          for (int j = 0; j < super.numChannels(); j++) {
            super.channelEnds(j); // directly emit an EOS makes more sense.
          }
          if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("EOSC has sent EOS!");
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
  protected void childEOS() throws DbException {}

  @Override
  protected void childEOI() throws DbException {}
}
