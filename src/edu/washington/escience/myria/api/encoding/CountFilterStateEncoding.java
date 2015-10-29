package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.CountFilter;

public class CountFilterStateEncoding extends StreamingStateEncoding<CountFilter> {

  @Required
  public int threshold;
  @Required
  public int[] keyColIndices;

  @Override
  public CountFilter construct() {
    return new CountFilter(threshold, keyColIndices);
  }
}