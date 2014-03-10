package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.DupElim;

public class DupElimStateEncoding extends StreamingStateEncoding<DupElim> {

  @Override
  public DupElim construct() {
    return new DupElim();
  }
}
