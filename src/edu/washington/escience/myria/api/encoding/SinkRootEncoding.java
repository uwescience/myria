package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.SinkRoot;

public class SinkRootEncoding extends UnaryOperatorEncoding<SinkRoot> {

  public Integer argLimit;

  @Override
  public SinkRoot construct(ConstructArgs args) {
    if (argLimit != null) {
      return new SinkRoot(null, argLimit);
    } else {
      return new SinkRoot(null);
    }
  }
}
