package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.CacheLeaf;

public class CacheLeafEncoding extends LeafOperatorEncoding<CacheLeaf> {

  @Override
  public CacheLeaf construct(final ConstructArgs args) {
    return new CacheLeaf();
  }
}
