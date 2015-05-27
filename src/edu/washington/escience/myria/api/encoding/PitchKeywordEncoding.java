package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.PitchKeyword;

public class PitchKeywordEncoding extends UnaryOperatorEncoding<PitchKeyword> {

  @Override
  public PitchKeyword construct(ConstructArgs args) {
    return new PitchKeyword(null);
  }
}
