package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.KeepMinValue;

public class KeepMinValueStateEncoding extends StreamingStateEncoding<KeepMinValue> {

  public int[] keyColIndices;
  public int valueColIndex;

  @Override
  public KeepMinValue construct() {
    return new KeepMinValue(keyColIndices, valueColIndex);
  }
}