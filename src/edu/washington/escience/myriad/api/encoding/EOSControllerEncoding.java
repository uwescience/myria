package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.operator.Merge;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.EOSController;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.util.MyriaUtils;

public class EOSControllerEncoding extends AbstractProducerEncoding<EOSController> {
  public List<String> argIdbOperatorIds;
  public String[] argChildren;
  private static final List<String> requiredArguments = ImmutableList.of("argIdbOperatorIds", "argChildren");

  @Override
  public EOSController construct() {
    List<ExchangePairID> ids = getRealOperatorIds();
    return new EOSController(null, ids.toArray(new ExchangePairID[ids.size()]), MyriaUtils
        .integerCollectionToIntArray(getRealWorkerIds()));
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    Operator[] tmp = new Operator[argChildren.length];
    for (int i = 0; i < tmp.length; ++i) {
      tmp[i] = operators.get(argChildren[i]);
    }
    current.setChildren(new Operator[] { new Merge(tmp) });
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

  @Override
  protected List<String> getOperatorIds() {
    return argIdbOperatorIds;
  }
}