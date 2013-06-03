package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.operator.IDBInput;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.ExchangePairID;

public class IDBInputEncoding extends OperatorEncoding<IDBInput> {
  public int argControllerOperatorId;
  public int argControllerWorkerId;
  public int argSelfIdbId;
  public String argInitialInput;
  public String argIterationInput;
  public String argEosControllerInput;
  private static final List<String> requiredArguments = ImmutableList.of("argSelfIdbId", "argControllerOperatorId",
      "argControllerWorkerId", "argInitialInput", "argIterationInput", "argEosControllerInput");

  @Override
  public IDBInput construct() {
    return new IDBInput(argSelfIdbId, ExchangePairID.fromExisting(argControllerOperatorId), argControllerWorkerId,
        null, null, null);
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