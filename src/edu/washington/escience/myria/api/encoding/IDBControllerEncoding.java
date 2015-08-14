package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

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

  public Boolean sync;

  private ExchangePairID realEosControllerOperatorId;
  public Integer realEosControllerWorkerId;

  @Required
  public StreamingStateEncoding<?> argState;

  @Override
  public IDBController construct(@Nonnull final ConstructArgs args) {
    return new IDBController(argSelfIdbId, realEosControllerOperatorId, realEosControllerWorkerId, null, null, null,
        argState.construct(), MoreObjects.firstNonNull(sync, Boolean.FALSE));
  }

  @Override
  public void connect(final Operator current, final Map<Integer, Operator> operators) {
    current.setChildren(new Operator[] {
        operators.get(argInitialInput), operators.get(argIterationInput), operators.get(argEosControllerInput) });
  }

  protected void setRealEosControllerOperatorID(final ExchangePairID realEosControllerOperatorId) {
    this.realEosControllerOperatorId = realEosControllerOperatorId;
  }

  protected ExchangePairID getRealEosControllerOperatorID() {
    return realEosControllerOperatorId;
  }
}