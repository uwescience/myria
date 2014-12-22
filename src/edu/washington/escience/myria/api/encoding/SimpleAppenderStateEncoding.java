package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.SimpleAppender;

public class SimpleAppenderStateEncoding extends StreamingStateEncoding<SimpleAppender> {

  @Override
  public SimpleAppender construct() {
    return new SimpleAppender();
  }
}
