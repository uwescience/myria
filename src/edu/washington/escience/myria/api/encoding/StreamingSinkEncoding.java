package edu.washington.escience.myria.api.encoding;

import java.io.IOException;

import javax.ws.rs.core.Response.Status;

import edu.washington.escience.myria.CsvTupleWriter;
import edu.washington.escience.myria.TupleWriter;
import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.io.PipeSink;
import edu.washington.escience.myria.operator.TupleSink;

public class StreamingSinkEncoding extends UnaryOperatorEncoding<TupleSink> {

  @Override
  public TupleSink construct(final ConstructArgs args) throws MyriaApiException {
    // TODO: dynamically select TupleWriter impl from API format parameter
    final TupleWriter tupleWriter = new CsvTupleWriter();
    final PipeSink dataSink;
    try {
      dataSink = new PipeSink();
    } catch (IOException e) {
      throw new MyriaApiException(Status.INTERNAL_SERVER_ERROR, e);
    }
    args.getServer().registerQueryOutput(args.getQueryId(), dataSink.getInputStream());
    return new TupleSink(null, tupleWriter, dataSink);
  }
}
