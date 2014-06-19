package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.Difference;

public class DifferenceEncoding extends BinaryOperatorEncoding<Difference> {

  @Override
  public Difference construct(ConstructArgs args) throws MyriaApiException {
    return new Difference(null, null);
  }
}
