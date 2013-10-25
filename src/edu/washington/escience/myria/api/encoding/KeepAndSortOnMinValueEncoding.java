package edu.washington.escience.myria.api.encoding;

import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.KeepAndSortOnMinValue;

public class KeepAndSortOnMinValueEncoding extends StreamingStateUpdaterEncoding<KeepAndSortOnMinValue> {
  private static final List<String> requiredFields = ImmutableList.of();

  public int[] keyColIndices;
  public int valueColIndex;

  @Override
  public KeepAndSortOnMinValue construct() {
    return new KeepAndSortOnMinValue(keyColIndices, valueColIndex);
  }

  @Override
  protected List<String> getRequiredFields() {
    return requiredFields;
  }
}