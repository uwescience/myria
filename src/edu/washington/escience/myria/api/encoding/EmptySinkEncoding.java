package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.EmptySink;

public class EmptySinkEncoding extends UnaryOperatorEncoding<EmptySink> {

  public Integer argLimit;

  @Override
  public EmptySink construct(final ConstructArgs args) {
    if (argLimit != null) {
      return new EmptySink(null, argLimit);
    } else {
      return new EmptySink(null);
    }
  }
}
