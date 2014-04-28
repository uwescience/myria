package edu.washington.escience.myria.api.encoding;

import javax.ws.rs.core.Response.Status;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.MFMDHashPartitionFunction;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaArrayUtils;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * Producer part of JSON Encoding for HyperCube Join.
 * 
 * @author Shumo Chu <chushumo@cs.washington.edu>
 * 
 */
public class HyperShuffleProducerEncoding extends AbstractProducerEncoding<GenericShuffleProducer> {

  @Required
  public int[] indexes;
  @Required
  public int[] hyperCubeDimensions;
  @Required
  public int[][] cellPartition;

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
    MFMDHashPartitionFunction pf = new MFMDHashPartitionFunction(cellPartition.length, hyperCubeDimensions, indexes);

    return new GenericShuffleProducer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), cellPartition,
        MyriaUtils.integerCollectionToIntArray(getRealWorkerIds()), pf);
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
