package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.operator.IDBInput;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.ExchangePairID;

public class IDBInputEncoding extends OperatorEncoding<IDBInput> {
  public Integer argSelfIdbId;
  public String argControllerOperatorId;
  public String argInitialInput;
  public String argIterationInput;
  public String argEosControllerInput;
  public ExchangePairID realControllerOperatorId;
  public Integer realControllerWorkerId;
  private static final List<String> requiredArguments = ImmutableList.of("argSelfIdbId", "argControllerOperatorId",
      "argInitialInput", "argIterationInput", "argEosControllerInput");

  @Override
  public IDBInput construct() {
    return new IDBInput(argSelfIdbId, realControllerOperatorId, realControllerWorkerId, null, null, null);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] {
        operators.get(argInitialInput), operators.get(argIterationInput), operators.get(argEosControllerInput) });
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}