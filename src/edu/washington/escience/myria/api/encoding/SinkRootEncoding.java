package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.parallel.Server;

public class SinkRootEncoding extends UnaryOperatorEncoding<SinkRoot> {

  public Integer argLimit;

  @Override
  public SinkRoot construct(Server server) {
    if (argLimit != null) {
      return new SinkRoot(null, argLimit);
    } else {
      return new SinkRoot(null);
    }
  }
}