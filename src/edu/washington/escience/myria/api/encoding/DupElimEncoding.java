package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.operator.DupElim;
import edu.washington.escience.myria.operator.StreamingStateWrapper;
import edu.washington.escience.myria.parallel.Server;

public class DupElimEncoding extends UnaryOperatorEncoding<StreamingStateWrapper> {

  @Override
  public StreamingStateWrapper construct(Server server) throws MyriaApiException {
    return new StreamingStateWrapper(null, new DupElim());
  }
}
