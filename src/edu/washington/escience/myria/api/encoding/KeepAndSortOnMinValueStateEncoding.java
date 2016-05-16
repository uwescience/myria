package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.KeepAndSortOnMinValue;

public class KeepAndSortOnMinValueStateEncoding
    extends StreamingStateEncoding<KeepAndSortOnMinValue> {

  public int[] keyColIndices;
  public int[] valueColIndices;

  @Override
  public KeepAndSortOnMinValue construct() {
    return new KeepAndSortOnMinValue(keyColIndices, valueColIndices);
  }
}
