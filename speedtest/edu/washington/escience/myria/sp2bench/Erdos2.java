package edu.washington.escience.myria.sp2bench;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.StreamingStateWrapper;
import edu.washington.escience.myria.operator.network.Producer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

public class Erdos2 implements QueryPlanGenerator {

  final static ImmutableList<Type> outputTypes = ImmutableList.of(Type.STRING_TYPE);
  final static ImmutableList<String> outputColumnNames = ImmutableList.of("names");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final ExchangePairID sendToMasterID = ExchangePairID.newID();

  /**
   * select distinct names.val from <br>
   * Triples erdospubs <br>
   * join Dictionary erdos on erdospubs.object=erdos.id <br>
   * join Dictionary creator1 on erdospubs.predicate=creator1.id, <br>
   * Triples coauthors <br>
   * join Dictionary creator2 on coauthors.predicate=creator2.id, <br>
   * Triples coauthorpubs <br>
   * join Dictionary creator3 on coauthorpubs.predicate=creator3.id, <br>
   * Triples cocoauthors <br>
   * join Dictionary creator4 on cocoauthors.predicate=creator4.id <br>
   * join Dictionary names on cocoauthors.object=names.id <br>
   * where <br>
   * erdos.val='<http://localhost/persons/Paul_Erdoes>' <br>
   * and creator1.val='dc:creator' <br>
   * and creator2.val='dc:creator' <br>
   * and creator3.val='dc:creator' <br>
   * and creator4.val='dc:creator' <br>
   * and erdospubs.subject=coauthors.subject <br>
   * and coauthors.object=coauthorpubs.object <br>
   * and cocoauthors.subject=coauthorpubs.subject<br>
   *
   * */
  @Override
  public Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers) throws Exception {
    ArrayList<Producer> producers = new ArrayList<Producer>();
    StreamingStateWrapper e2 = Erdos.erdosN(2, allWorkers, producers);
    return Erdos.getWorkerPlan(allWorkers, Erdos.extractName(e2), producers);
  }

  @Override
  public RootOperator getMasterPlan(
      int[] allWorkers, final LinkedBlockingQueue<TupleBatch> receivedTupleBatches)
      throws Exception {
    return Erdos.getMasterPlan(allWorkers, null);
  }

  @Test
  public void test() throws Exception {
    System.out.println(new Erdos2().getWorkerPlan(new int[] {0, 1, 2, 3, 4}).get(0)[0]);
  }
}
