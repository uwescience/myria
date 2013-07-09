package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.BroadcastProducer;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.util.MyriaUtils;

/**
 * 
 * JSON wrapper for BroadcastProducer
 * 
 * @author Shumo Chu <chushumo@cs.washington.edu>
 * 
 */
public class BroadcastProducerEncoding extends AbstractProducerEncoding<BroadcastProducer> {

  public String argChild;
  public String argOperatorId;

  private static final List<String> requiredArguments = ImmutableList.of("argChild", "argOperatorId");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public BroadcastProducer construct(Server server) {
    return new BroadcastProducer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
        .integerCollectionToIntArray(getRealWorkerIds()));
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
