package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.IDBController;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.Server;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IDBControllerEncoding extends OperatorEncoding<IDBController> {
  @JsonProperty
  public Integer argSelfIdbId;
  @JsonProperty
  public String argInitialInput;
  @JsonProperty
  public String argIterationInput;
  @JsonProperty
  public String argEosControllerInput;

  private ExchangePairID realControllerOperatorId;
  public Integer realControllerWorkerId;

  public StreamingStateEncoding<?> argState;
  private static final List<String> requiredArguments = ImmutableList.of("argSelfIdbId", "argInitialInput",
      "argIterationInput", "argEosControllerInput", "argState");

  @Override
  public IDBController construct(Server server) {
    return new IDBController(argSelfIdbId, realControllerOperatorId, realControllerWorkerId, null, null, null, argState
        .construct());
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

  protected void setRealControllerOperatorID(final ExchangePairID realControllerOperatorId) {
    this.realControllerOperatorId = realControllerOperatorId;
  }

  protected ExchangePairID getRealControllerOperatorID() {
    return realControllerOperatorId;
  }
}