package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.GenericShuffleProducer;
import edu.washington.escience.myria.parallel.MFMDHashPartitionFunction;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.util.MyriaArrayUtils;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * Producer part of JSON Encoding for HyperCube Join.
 * 
 * @author Shumo Chu <chushumo@cs.washington.edu>
 * 
 */
public class HyperShuffleProducerEncoding extends AbstractProducerEncoding<GenericShuffleProducer> {

  public String argChild;
  public String argOperatorId;
  public SingleFieldPartitionFunctionEncoding[] argPfs;
  public int[] hyperCubeDimensions;
  public int[][] cellPartition;

  private static final List<String> requiredArguments = ImmutableList.of("argChild", "argOperatorId", "argPfs",
      "hyperCubeDimensions", "cellPartition");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public GenericShuffleProducer construct(Server server) throws MyriaApiException {

    /*
     * Validate whether number of workers matches cube dimensions.
     * 
     * has to validate here because until now the workers has been set.
     */
    int numCells = 1;
    for (int d : hyperCubeDimensions) {
      numCells = numCells * d;
    }
    if (getRealWorkerIds().size() != numCells) {
      throw new MyriaApiException(Status.BAD_REQUEST, "number of workers (" + getRealWorkerIds().size()
          + ") is not equal to the product of hyper join dimensions (" + numCells + ")");
    }

    /* constructing a MFMDHashPartitionFunction. */
    SingleFieldHashPartitionFunction[] pfs = new SingleFieldHashPartitionFunction[argPfs.length];
    for (int i = 0; i < pfs.length; i++) {
      pfs[i] = argPfs[i].construct(hyperCubeDimensions[argPfs[i].index]);
    }
    MFMDHashPartitionFunction pf = new MFMDHashPartitionFunction(cellPartition.length);
    pf.setAttribute("partition_functions", pfs);

    return new GenericShuffleProducer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), cellPartition,
        MyriaUtils.integerCollectionToIntArray(getRealWorkerIds()), pf);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

  @Override
  protected void validateExtra() {
    int[] arr = MyriaArrayUtils.arrayFlattenThenSort(cellPartition);
    for (int i = 0; i < arr.length; i++) {
      if (arr[i] != i) {
        throw new MyriaApiException(Status.BAD_REQUEST, "invalid cell partition");
      }
    }
  }

}
