package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.Split;

public class SplitEncoding extends UnaryOperatorEncoding<Split> {

  @Required public int splitColumnIndex;

  @Required public String regex;

  @Override
  public Split construct(final ConstructArgs args) {
    return new Split(splitColumnIndex, regex);
  }
}
