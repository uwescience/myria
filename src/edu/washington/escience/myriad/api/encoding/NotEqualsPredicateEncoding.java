package edu.washington.escience.myriad.api.encoding;

import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.NotEqualsPredicate;

public class NotEqualsPredicateEncoding extends PredicateEncoding<NotEqualsPredicate> {

  public Integer argCompareIndex;
  public String argCompareValue;
  private static final List<String> requiredArguments = ImmutableList.of("argCompareIndex", "argCompareValue");

  @Override
  public NotEqualsPredicate construct() {
    return new NotEqualsPredicate(argCompareIndex, argCompareValue);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

}
