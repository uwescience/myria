package edu.washington.escience.myria.api.encoding;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.UnionAll;

public class UnionAllEncoding extends NaryOperatorEncoding<UnionAll> {

  @Override
  public UnionAll construct(@Nonnull ConstructArgs args) {
    return new UnionAll(null);
  }

}