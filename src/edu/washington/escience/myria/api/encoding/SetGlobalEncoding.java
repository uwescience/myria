package edu.washington.escience.myria.api.encoding;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.SetGlobal;

public class SetGlobalEncoding extends UnaryOperatorEncoding<SetGlobal> {

  @Required
  public String key;

  @Override
  public SetGlobal construct(@Nonnull ConstructArgs args) {
    return new SetGlobal(null, key, args.getServer());
  }
}
