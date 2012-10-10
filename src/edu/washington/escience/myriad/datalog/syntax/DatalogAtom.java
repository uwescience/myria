package edu.washington.escience.myriad.datalog.syntax;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class DatalogAtom {
  String predicate = null;
  List<DatalogParamValue> params = new ArrayList<DatalogParamValue>();

  public DatalogAtom(final String p, final List<DatalogParamValue> args) {
    predicate = p;
    params.addAll(args);
  }

  public String getName() {
    return predicate;
  }

  public List<DatalogParamValue> getParams() {
    return params;
  }

  public void setName(final String s) {
    predicate = s;
  }

  public List<DatalogParamValue> getDefinedVariables() {
    final Set<DatalogParamValue> vars = new LinkedHashSet<DatalogParamValue>();
    for (final Iterator<DatalogParamValue> it = params.iterator(); it.hasNext();) {
      final DatalogParamValue dpv = it.next();
      if (dpv instanceof DatalogParamVariable) {
        vars.add(dpv);
      }
    }
    return new ArrayList<DatalogParamValue>(vars);
  }

  @Override
  public String toString() {
    String res = "";
    if (predicate != null) {
      res = res + predicate.toString() + "(";
    }
    if (params != null) {
      for (int i = 0; i < params.size(); i = i + 1) {
        // String a = (String)params.get(i);
        final DatalogParamValue a = params.get(i);
        res = res + a.toString();
        if (i < (params.size() - 1)) {
          res = res + ", ";
        }
      }
    }
    res = res + ")";
    return res;
  }
}