package edu.washington.escience.myria.api.encoding;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import edu.washington.escience.myria.parallel.Producer;

@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class AbstractProducerEncoding<P extends Producer> extends ExchangeEncoding<P> {
}