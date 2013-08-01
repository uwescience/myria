package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.GenericShuffleProducer;
import edu.washington.escience.myriad.parallel.MFMDHashPartitionFunction;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myriad.util.MyriaUtils;

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
  public GenericShuffleProducer construct(Server server) {

    /** constructing a MFMDHashPartitionFunction. */
    SingleFieldHashPartitionFunction[] pfs = new SingleFieldHashPartitionFunction[argPfs.length];
    for (int i = 0; i < cellPartition.length; i++) {
      pfs[i] = argPfs[i].construct(hyperCubeDimensions[argPfs[i].index]);
    }
    MFMDHashPartitionFunction pf = new MFMDHashPartitionFunction(cellPartition.length);
    pf.setAttribute("partition_functions", pfs);

    return new GenericShuffleProducer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), cellPartition, pf);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

  @Override
  protected void validateExtra() {
    /** Validate whether number of workers matches cube dimensions. */
    int numCells = 1;
    for (int d : hyperCubeDimensions) {
      numCells = numCells * d;
    }
    if (getRealWorkerIds().size() != numCells) {
      throw new MyriaApiException(Status.BAD_REQUEST,
          "number of workers must equal to the product of hyper join dimensions");
    }
  }

  @Override
  protected List<String> getOperatorIds() {
    return ImmutableList.of(argOperatorId);
  }

}
