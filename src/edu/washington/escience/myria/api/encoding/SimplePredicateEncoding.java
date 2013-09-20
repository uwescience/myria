package edu.washington.escience.myria.api.encoding;

import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.SimplePredicate;

public class SimplePredicateEncoding extends PredicateEncoding<SimplePredicate> {

  public Integer argCompareIndex;
  public String argCompareValue;
  public SimplePredicate.Op argOp;

  private static List<String> requiredFields = ImmutableList.of("argCompareIndex", "argCompareValue", "argOp");

  @Override
  public SimplePredicate construct() {
    return new SimplePredicate(argCompareIndex, argOp, argCompareValue);
  }

  @Override
  protected List<String> getRequiredFields() {
    return requiredFields;
  }

}
