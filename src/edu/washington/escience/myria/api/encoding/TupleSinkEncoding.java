package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.TupleSink;
import edu.washington.escience.myria.TupleWriter;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.io.DataSink;

public class TupleSinkEncoding extends UnaryOperatorEncoding<TupleSink> {
  @Required public TupleWriter tupleWriter;
  @Required public DataSink dataSink;

  @Override
  public TupleSink construct(ConstructArgs args) {
    return new TupleSink(null, tupleWriter, dataSink);
  }
}
