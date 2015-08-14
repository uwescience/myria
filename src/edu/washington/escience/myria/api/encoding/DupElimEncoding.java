package edu.washington.escience.myria.api.encoding;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.DupElim;
import edu.washington.escience.myria.operator.StreamingStateWrapper;

public class DupElimEncoding extends UnaryOperatorEncoding<StreamingStateWrapper> {

  @Override
  public StreamingStateWrapper construct(@Nonnull ConstructArgs args) throws MyriaApiException {
    return new StreamingStateWrapper(null, new DupElim());
  }
}
