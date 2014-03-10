package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.operator.Difference;
import edu.washington.escience.myria.parallel.Server;

public class DifferenceEncoding extends BinaryOperatorEncoding<Difference> {

  @Override
  public Difference construct(Server server) throws MyriaApiException {
    return new Difference(null, null);
  }
}
