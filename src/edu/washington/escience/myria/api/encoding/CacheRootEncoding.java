package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.CacheRoot;

public class CacheRootEncoding extends UnaryOperatorEncoding<CacheRoot> {

  @Override
  public CacheRoot construct(final ConstructArgs args) {
    return new CacheRoot(null);
  }
}