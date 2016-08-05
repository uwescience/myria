package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.TupleWriter;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.io.DataSink;
import edu.washington.escience.myria.operator.DataOutput;

public class DataOutputEncoding extends UnaryOperatorEncoding<DataOutput> {
  @Required public TupleWriter tupleWriter;
  @Required public DataSink dataSink;

  @Override
  public DataOutput construct(ConstructArgs args) {
    return new DataOutput(null, tupleWriter, dataSink);
  }
}
