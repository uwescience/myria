package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.ExchangePairID;

public class CollectProducerEncoding extends OperatorEncoding<CollectProducer> {
  public String argChild;
  public Integer argOperatorId;
  public Integer argWorkerId;
  private static final List<String> requiredArguments = ImmutableList.of("argChild", "argWorkerId", "argOperatorId");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public CollectProducer construct() {
    return new CollectProducer(null, ExchangePairID.fromExisting(argOperatorId), argWorkerId);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}