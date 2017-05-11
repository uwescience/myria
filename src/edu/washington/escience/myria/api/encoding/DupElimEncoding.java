package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.DupElim;

public class DupElimEncoding extends UnaryOperatorEncoding<DupElim> {

  @Override
  public DupElim construct(ConstructArgs args) throws MyriaApiException {
    return new DupElim(null);
  }
}
