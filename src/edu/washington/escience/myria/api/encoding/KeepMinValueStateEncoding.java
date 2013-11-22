package edu.washington.escience.myria.api.encoding;

import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.KeepMinValue;

public class KeepMinValueStateEncoding extends StreamingStateEncoding<KeepMinValue> {
  private static final List<String> requiredFields = ImmutableList.of();

  public int[] keyColIndices;
  public int valueColIndex;

  @Override
  public KeepMinValue construct() {
    return new KeepMinValue(keyColIndices, valueColIndex);
  }

  @Override
  protected List<String> getRequiredFields() {
    return requiredFields;
  }
}