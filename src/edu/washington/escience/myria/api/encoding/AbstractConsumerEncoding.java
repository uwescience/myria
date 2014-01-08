package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.parallel.Consumer;

public abstract class AbstractConsumerEncoding<C extends Consumer> extends ExchangeEncoding<C> {
  public String argOperatorId;

  String getOperatorId() {
    return argOperatorId;
  }

}