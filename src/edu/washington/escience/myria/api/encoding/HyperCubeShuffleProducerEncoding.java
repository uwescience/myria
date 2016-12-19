package edu.washington.escience.myria.api.encoding;

import java.util.Collections;
import java.util.List;

import javax.ws.rs.core.Response.Status;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.distribute.HyperCubeDistributeFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * JSON Encoding for HyperCubeShuffle.
 */
public class HyperCubeShuffleProducerEncoding
    extends AbstractProducerEncoding<GenericShuffleProducer> {

  /** distribute function. */
  @Required public HyperCubeDistributeFunction distributeFunction;

  @Override
  public GenericShuffleProducer construct(ConstructArgs args) throws MyriaApiException {
    return new GenericShuffleProducer(
        null,
        getRealOperatorIds().toArray(new ExchangePairID[] {}),
        MyriaUtils.integerSetToIntArray(
            args.getServer().getRandomWorkers(distributeFunction.getAllDestinations().size())),
        distributeFunction);
  }

  @Override
  protected void validateExtra() {
    List<Integer> values = distributeFunction.getAllDestinations();
    Collections.sort(values);
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i) != i) {
        throw new MyriaApiException(Status.BAD_REQUEST, "invalid cell partition");
      }
    }
  }
}
