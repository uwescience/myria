/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.PostgresBinaryTupleWriter;
import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.io.DataSink;
import edu.washington.escience.myria.operator.DataOutput;

/**
 * 
 */
public class PersistEncoding extends UnaryOperatorEncoding<DataOutput> {
  @Required
  public DataSink sink;

  @Override
  public DataOutput construct(final ConstructArgs args) throws MyriaApiException {
    return new DataOutput(null, new PostgresBinaryTupleWriter(), sink);
  }
}