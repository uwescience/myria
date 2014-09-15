package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.CrossWithSingleton;

public class CrossWithSingletonEncoding extends BinaryOperatorEncoding<CrossWithSingleton> {

  @Override
  public CrossWithSingleton construct(ConstructArgs args) throws MyriaApiException {
    return new CrossWithSingleton(null, null);
  }
}
