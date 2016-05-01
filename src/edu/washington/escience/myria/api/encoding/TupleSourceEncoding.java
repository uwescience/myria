package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.TupleReader;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.TupleSource;

public class TupleSourceEncoding extends LeafOperatorEncoding<TupleSource> {
  @Required
  public TupleReader reader;
  @Required
  public DataSource source;

  @Override
  public TupleSource construct(final ConstructArgs args) {
    return new TupleSource(reader, source);
  }
}