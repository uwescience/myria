package edu.washington.escience.myria.api.encoding;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import edu.washington.escience.myria.parallel.Consumer;

@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class AbstractConsumerEncoding<C extends Consumer> extends ExchangeEncoding<C> {
  public String argOperatorId;

  String getOperatorId() {
    return argOperatorId;
  }

}