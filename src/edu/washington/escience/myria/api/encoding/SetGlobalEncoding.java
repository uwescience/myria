package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.SetGlobal;
import edu.washington.escience.myria.parallel.Server;

public class SetGlobalEncoding extends UnaryOperatorEncoding<SetGlobal> {

  @Required
  public String key;

  @Override
  public SetGlobal construct(final Server server) {
    return new SetGlobal(null, key, server);
  }
}
