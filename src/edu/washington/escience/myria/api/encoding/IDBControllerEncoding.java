package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.operator.IDBController;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.Server;

public class IDBControllerEncoding extends OperatorEncoding<IDBController> {
  @JsonProperty
  @Required
  public Integer argSelfIdbId;
  @JsonProperty
  @Required
  public String argInitialInput;
  @JsonProperty
  @Required
  public String argIterationInput;
  @JsonProperty
  @Required
  public String argEosControllerInput;

  private ExchangePairID realControllerOperatorId;
  public Integer realControllerWorkerId;

  @Required
  public StreamingStateEncoding<?> argState;

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

  protected void setRealControllerOperatorID(final ExchangePairID realControllerOperatorId) {
    this.realControllerOperatorId = realControllerOperatorId;
  }

  protected ExchangePairID getRealControllerOperatorID() {
    return realControllerOperatorId;
  }
}