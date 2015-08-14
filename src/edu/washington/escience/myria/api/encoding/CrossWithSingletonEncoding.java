package edu.washington.escience.myria.api.encoding;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.CrossWithSingleton;

public class CrossWithSingletonEncoding extends BinaryOperatorEncoding<CrossWithSingleton> {

  @Override
  public CrossWithSingleton construct(@Nonnull ConstructArgs args) throws MyriaApiException {
    return new CrossWithSingleton(null, null);
  }
}
