package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.UnionAll;
import edu.washington.escience.myria.parallel.Server;

public class UnionAllEncoding extends NaryOperatorEncoding<UnionAll> {

  @Override
  public UnionAll construct(final Server server) {
    return new UnionAll(null);
  }

}