package edu.washington.escience.myriad.datalog.syntax;

import java.util.ArrayList;
import java.util.List;

/*
 * the purpose of this class is to implement more advanced operations apart from projections including aggregates:
 * addition, min, max
 */

public class DatalogOperation extends DatalogParamValue {

  private final String op;
  private final List<DatalogParamValue> params = new ArrayList<DatalogParamValue>();

  public DatalogOperation(final String str, final List<DatalogParamValue> args) {
    op = str;
    params.addAll(args);
  }

  public String getOperation() {
    return op;
  }

  public List<DatalogParamValue> getParams() {
    return params;
  }

  @Override
  public boolean isConstant() {
    return false;
  }

  @Override
  public boolean isVariable() {
    return false;
  }

  @Override
  public String toString() {
    final String res = "Op(" + op + ")";
    return res;
  }
}