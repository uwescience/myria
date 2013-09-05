package edu.washington.escience.myria.api.encoding;

import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.DupElim;

public class DupElimEncoding extends StreamingStateUpdaterEncoding<DupElim> {
  private static final List<String> requiredFields = ImmutableList.of();

  @Override
  public DupElim construct() {
    return new DupElim();
  }

  @Override
  protected List<String> getRequiredFields() {
    return requiredFields;
  }
}