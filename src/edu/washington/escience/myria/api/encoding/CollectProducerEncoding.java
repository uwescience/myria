package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaUtils;

public class CollectProducerEncoding extends AbstractProducerEncoding<CollectProducer> {
  public String argChild;
  public String argOperatorId;
  private static final List<String> requiredArguments = ImmutableList.of("argChild", "argOperatorId");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public CollectProducer construct(Server server) {
    return new CollectProducer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
        .getSingleElement(getRealWorkerIds()));
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

  @Override
  protected List<String> getOperatorIds() {
    return ImmutableList.of(argOperatorId);
  }
}