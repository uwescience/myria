package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.IDBController;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.ExchangePairID;

public class IDBControllerEncoding extends OperatorEncoding<IDBController> {
  @JsonProperty
  @Required
  public Integer argSelfIdbId;
  @JsonProperty
  @Required
  public Integer argInitialInput;
  @JsonProperty
  @Required
  public Integer argIterationInput;
  @JsonProperty
  @Required
  public Integer argEosControllerInput;

  private ExchangePairID realControllerOperatorId;
  public Integer realControllerWorkerId;

  @Required
  public StreamingStateEncoding<?> argState;

  @Override
  public IDBController construct(ConstructArgs args) {
    return new IDBController(argSelfIdbId, realControllerOperatorId, realControllerWorkerId, null, null, null, argState
        .construct());
  }

  @Override
  public void connect(Operator current, Map<Integer, Operator> operators) {
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