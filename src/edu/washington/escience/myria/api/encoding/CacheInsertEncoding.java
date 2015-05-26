package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.CacheInsert;

public class CacheInsertEncoding extends UnaryOperatorEncoding<CacheInsert> {

  @Override
  public CacheInsert construct(final ConstructArgs args) {
    return new CacheInsert(null);
  }
}