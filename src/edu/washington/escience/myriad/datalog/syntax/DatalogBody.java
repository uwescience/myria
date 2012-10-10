package edu.washington.escience.myriad.datalog.syntax;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class DatalogBody {
  private final List<DatalogAtom> ruleBody = new ArrayList<DatalogAtom>();

  public DatalogBody() {
  }

  public DatalogBody(final List<DatalogAtom> args) {
    ruleBody.addAll(args);
  }

  public void addAtom(final DatalogAtom a) {
    ruleBody.add(a);
  }

  public List<DatalogParamValue> getDefinedVariables() {
    final Set<DatalogParamValue> res = new LinkedHashSet<DatalogParamValue>();
    for (final Iterator<DatalogAtom> it = ruleBody.iterator(); it.hasNext();) {
      final DatalogAtom a = it.next();
      res.addAll(a.getDefinedVariables());
    }
    return new ArrayList<DatalogParamValue>(res);
  }

  public List<DatalogAtom> getRuleAtoms() {
    return ruleBody;
  }

  public Set<String> getBodyAtomNames() {
    final Set<String> names = new LinkedHashSet<String>();
    for (int i = 0; i < ruleBody.size(); i = i + 1) {
      final DatalogAtom a = ruleBody.get(i);
      final String nm = a.getName();
      names.add(nm);
    }
    return names;
  }

  @Override
  public String toString() {
    String res = "";
    for (int i = 0; i < ruleBody.size(); i = i + 1) {
      final DatalogAtom a = ruleBody.get(i);
      res = res + a.toString();
      if (i < (ruleBody.size() - 1)) {
        res = res + ", ";
      }
    }
    return res;
  }
}