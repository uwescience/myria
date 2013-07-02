package edu.washington.escience.myriad.api.encoding;

import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.WithinSumRangePredicate;

public class WithinSumRangePredicateEncoding extends PredicateEncoding<WithinSumRangePredicate> {

  public Integer argCompareIndex;
  public List<Integer> argOperandIndices;
  private static final List<String> requiredFields = ImmutableList.of("argCompareIndex", "argOperandIndices");

  @Override
  public WithinSumRangePredicate construct() {
    return new WithinSumRangePredicate(argCompareIndex, argOperandIndices);
  }

  @Override
  protected List<String> getRequiredFields() {
    return requiredFields;
  }
}
