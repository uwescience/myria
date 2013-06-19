package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.operator.Merge;
import edu.washington.escience.myriad.operator.Operator;

public class MergeEncoding extends OperatorEncoding<Merge> {
  public String[] argChildren;
  private static final List<String> requiredArguments = ImmutableList.of("argChildren");

  @Override
  public Merge construct() {
    return new Merge(null);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    Operator[] tmp = new Operator[argChildren.length];
    for (int i = 0; i < tmp.length; ++i) {
      tmp[i] = operators.get(argChildren[i]);
    }
    current.setChildren(tmp);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}