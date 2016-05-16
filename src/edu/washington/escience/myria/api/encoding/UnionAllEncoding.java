package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.UnionAll;

public class UnionAllEncoding extends NaryOperatorEncoding<UnionAll> {

  @Override
  public UnionAll construct(ConstructArgs args) {
    return new UnionAll(null);
  }
}
