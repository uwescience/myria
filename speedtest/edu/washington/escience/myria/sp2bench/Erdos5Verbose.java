package edu.washington.escience.myria.sp2bench;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.StreamingStateWrapper;
import edu.washington.escience.myria.operator.network.Producer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

public class Erdos5Verbose implements QueryPlanGenerator {

  final static ImmutableList<Type> outputTypes = ImmutableList.of(Type.STRING_TYPE);
  final static ImmutableList<String> outputColumnNames = ImmutableList.of("names");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final ExchangePairID sendToMasterID = ExchangePairID.newID();

  /**
   * select distinct names.val <br>
   * from <br>
   * Triples pubs <br>
   * join Dictionary pe on pubs.object=pe.id <br>
   * join Dictionary creator on pubs.predicate=creator.id, <br>
   * Triples authors <br>
   * join Dictionary creator2 on authors.predicate=creator2.id <br>
   * join Dictionary names on authors.object=names.id <br>
   * where <br>
   * creator.val='dc:creator' <br>
   * and pe.val='<http://localhost/persons/Paul_Erdoes>' <br>
   * and creator2.val='dc:creator' <br>
   * and pubs.subject=authors.subject;<br>
   *
   * */
  @Override
  public Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers) throws Exception {
    ArrayList<Producer> producers = new ArrayList<Producer>();
    StreamingStateWrapper e5 = ErdosVerbose.erdosN(5, allWorkers, producers);
    return ErdosVerbose.getWorkerPlan(allWorkers, e5, producers);
  }

  @Override
  public RootOperator getMasterPlan(
      int[] allWorkers, final LinkedBlockingQueue<TupleBatch> receivedTupleBatches)
      throws Exception {
    return ErdosVerbose.getMasterPlan(allWorkers, receivedTupleBatches);
  }
}
