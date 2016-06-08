package edu.washington.escience.myria.api.encoding;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.MFMDHashPartitionFunction;
import edu.washington.escience.myria.util.MyriaArrayUtils;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * Producer part of JSON Encoding for HyperCube Join.
 *
 */
public class HyperShuffleProducerEncoding extends AbstractProducerEncoding<GenericShuffleProducer> {

  @Required public int[] hashedColumns;
  @Required public int[] mappedHCDimensions;
  @Required public int[] hyperCubeDimensions;
  @Required public int[][] cellPartition;

  @Override
  public GenericShuffleProducer construct(ConstructArgs args) throws MyriaApiException {

    /*
     * Validate whether number of workers matches cube dimensions.
     *
     * has to validate here because until now the workers has been set.
     */
    int numCells = 1;
    for (int d : hyperCubeDimensions) {
      numCells = numCells * d;
    }
    for (int[] partition : cellPartition) {
      for (int cellId : partition) {
        Preconditions.checkElementIndex(cellId, getRealWorkerIds().size());
      }
    }

    /* constructing a MFMDHashPartitionFunction. */
    MFMDHashPartitionFunction pf =
        new MFMDHashPartitionFunction(
            cellPartition.length, hyperCubeDimensions, hashedColumns, mappedHCDimensions);

    return new GenericShuffleProducer(
        null,
        MyriaUtils.getSingleElement(getRealOperatorIds()),
        cellPartition,
        MyriaUtils.integerSetToIntArray(args.getServer().getRandomWorkers(numCells)),
        pf);
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
