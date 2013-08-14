package edu.washington.escience.myria.api.encoding;

import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.EqualsPredicate;

public class EqualsPredicateEncoding extends PredicateEncoding<EqualsPredicate> {

  public Integer argCompareIndex;
  public String argCompareValue;
  private static List<String> requiredFields = ImmutableList.of("argCompareIndex", "argCompareValue");

  @Override
  public EqualsPredicate construct() {
    return new EqualsPredicate(argCompareIndex, argCompareValue);
  }

  @Override
  protected List<String> getRequiredFields() {
    return requiredFields;
  }

}
