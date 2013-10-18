package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.EOSController;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaUtils;

public class EOSControllerEncoding extends AbstractProducerEncoding<EOSController> {
  public String argChild;
  private static final List<String> requiredArguments = ImmutableList.of("argChild");

  @Override
  public EOSController construct(Server server) {
    List<ExchangePairID> ids = getRealOperatorIds();
    return new EOSController(null, ids.toArray(new ExchangePairID[ids.size()]), MyriaUtils
        .integerCollectionToIntArray(getRealWorkerIds()));
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

}