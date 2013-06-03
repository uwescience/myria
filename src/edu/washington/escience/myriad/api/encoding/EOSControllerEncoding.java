package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.operator.Merge;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.EOSController;
import edu.washington.escience.myriad.parallel.ExchangePairID;

public class EOSControllerEncoding extends OperatorEncoding<EOSController> {
  public int[] argWorkerIds;
  public int[] argIdbOperatorIds;
  public String[] argChildren;
  private static final List<String> requiredArguments = ImmutableList.of("argWorkerIds", "argIdbOperatorIds",
      "argChildren");

  @Override
  public EOSController construct() {
    ExchangePairID tmp[] = new ExchangePairID[argIdbOperatorIds.length];
    for (int i = 0; i < tmp.length; ++i) {
      tmp[i] = ExchangePairID.fromExisting(argIdbOperatorIds[i]);
    }
    return new EOSController(null, tmp, argWorkerIds);
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
}