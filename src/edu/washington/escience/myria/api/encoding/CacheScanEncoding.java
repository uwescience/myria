package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.CacheScan;

public class CacheScanEncoding extends LeafOperatorEncoding<CacheScan> {
  @Required
  public Schema schema;

  @Override
  public CacheScan construct(final ConstructArgs args) {
    return new CacheScan(schema);
  }
}
