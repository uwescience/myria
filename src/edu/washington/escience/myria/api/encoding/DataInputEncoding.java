package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.TupleReader;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.DataInput;

public class DataInputEncoding extends LeafOperatorEncoding<DataInput> {
  @Required
  public TupleReader reader;
  @Required
  public DataSource source;

  @Override
  public DataInput construct(final ConstructArgs args) {
    return new DataInput(reader, source);
  }
}