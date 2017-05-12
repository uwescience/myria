package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.StatefulDupElim;

public class DupElimStateEncoding extends StreamingStateEncoding<StatefulDupElim> {

  @Override
  public StatefulDupElim construct() {
    return new StatefulDupElim();
  }
}
