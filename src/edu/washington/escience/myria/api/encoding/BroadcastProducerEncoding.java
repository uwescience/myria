package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.FixValuePartitionFunction;
import edu.washington.escience.myria.parallel.GenericShuffleProducer;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * 
 * JSON wrapper for BroadcastProducer
 * 
 * @author Shumo Chu <chushumo@cs.washington.edu>
 * 
 */
public class BroadcastProducerEncoding extends AbstractProducerEncoding<GenericShuffleProducer> {

  public String argChild;

  private static final List<String> requiredArguments = ImmutableList.of("argChild");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public GenericShuffleProducer construct(Server server) {
    int[][] cellPartition = new int[1][];
    int[] allCells = new int[getRealWorkerIds().size()];
    for (int i = 0; i < getRealWorkerIds().size(); i++) {
      allCells[i] = i;
    }
    cellPartition[0] = allCells;
    return new GenericShuffleProducer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), cellPartition,
        MyriaUtils.integerCollectionToIntArray(getRealWorkerIds()), new FixValuePartitionFunction(0));
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

}
