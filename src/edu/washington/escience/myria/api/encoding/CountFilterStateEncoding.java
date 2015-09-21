package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.CountFilter;

public class CountFilterStateEncoding extends StreamingStateEncoding<CountFilter> {

  public int threshold;

  @Override
  public CountFilter construct() {
    return new CountFilter(threshold);
  }
}