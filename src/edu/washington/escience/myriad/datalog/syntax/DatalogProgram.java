package edu.washington.escience.myriad.datalog.syntax;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public final class DatalogProgram {
  private final List<DatalogBackend> backends = new ArrayList<DatalogBackend>();
  private DatalogRuleSet rules = null;
  private DatalogAnswer answer = null;
  private DependencyGraph deps = null;
  private HashMap<String, String> inputMap = null;

  public DatalogProgram() {
  }

  public void processProgram() {

    // check semantics first
    if (!(this.areRulesSafe() && this.areRelationSizesRespected())) {
      System.err.println("Program is not semantically correct!");
      System.exit(-1);
    }
    // create the dependency graph
    deps = new DependencyGraph(this);
    // create the input map
    this.createInputMap();
  }

  private boolean areRelationSizesRespected() {

    boolean areRespected = true;
    final HashMap<String, Integer> relsToSize = new HashMap<String, Integer>();

    // First, add the number of parameters for each backend
    for (final Iterator<DatalogBackend> it = backends.iterator(); it.hasNext();) {
      final DatalogBackend bnd = it.next();
      final int numBndElems = bnd.getModelArgs().size();
      relsToSize.put(bnd.getModelName(), numBndElems);
    }

    final List<DatalogPredicate> indRules = rules.getPredicates();

    // For each rule, as it is encountered add the expected number of
    // elements to the hashmap. If we ever encounter a problem, warn.
    for (final Iterator<DatalogPredicate> it = indRules.iterator(); it.hasNext();) {
      final DatalogPredicate r = it.next();
      for (final Iterator<DatalogRule> rIt = r.getDefiningRules().iterator(); rIt.hasNext();) {
        final DatalogRule indr = rIt.next();
        final DatalogAtom hd = indr.getHead();

        // First check the head
        if (doesAtomRespectsRelationSizes(relsToSize, hd)) {
          relsToSize.put(hd.getName(), hd.getParams().size());
        } else {
          areRespected = false;
          System.err.println("ERR: Relation/rule size mismatch on definition and use of individual rule: "
              + indr.toString());
        }

        // Then check the atoms in the body
        final DatalogBody body = indr.getBody();
        for (final Iterator<DatalogAtom> bIt = body.getRuleAtoms().iterator(); bIt.hasNext();) {
          final DatalogAtom a = bIt.next();
          if (doesAtomRespectsRelationSizes(relsToSize, a)) {
            relsToSize.put(a.getName(), a.getParams().size());
          } else {
            areRespected = false;
            System.err.println("ERR: Relation/rule size mismatch on definition of individual rule: " + indr.toString());
            final DatalogBackend mismatchedBnd = this.getBackend(a.getName());
            if (mismatchedBnd != null) {
              System.err.println("ERR: Backend is specified as:\n" + mismatchedBnd.toString());
            }
            final DatalogPredicate mismatchedRule = this.getRuleSet().getDefinedRule(a.getName());
            if (mismatchedRule != null) {
              System.err.println("ERR: Rule is specified as:\n" + mismatchedRule.toString());
            }
          }
        }
      }
    }
    return areRespected;
  }

  private boolean doesAtomRespectsRelationSizes(final HashMap<String, Integer> relsToSize, final DatalogAtom a) {
    boolean areRespected = true;
    final String nm = a.getName();
    final int numAtomParams = a.getParams().size();
    if (relsToSize.containsKey(nm)) {
      final int existingSize = relsToSize.get(nm);
      if (existingSize != numAtomParams) {
        System.err.println("Multiple sizes used for relation " + nm + ": " + existingSize + " & " + numAtomParams);
        areRespected = false;
      }
    }
    return areRespected;
  }

  /**
   * Checks the safety of the rules. This means that all variables used in the head are defined in the body.
   * 
   * @return true if the rules are safe.
   */
  private boolean areRulesSafe() {

    boolean areSafe = true;
    final List<DatalogPredicate> indRules = rules.getPredicates();

    for (DatalogPredicate r : indRules) {
      for (DatalogRule indr : r.getDefiningRules()) {
        // Check that all variables in the head are defined in the body
        final DatalogAtom hd = indr.getHead();
        final List<DatalogParamValue> hdVars = hd.getDefinedVariables();
        final DatalogBody body = indr.getBody();
        final List<DatalogParamValue> bodyVars = body.getDefinedVariables();

        for (final Iterator<DatalogParamValue> vit = hdVars.iterator(); vit.hasNext();) {
          final DatalogParamVariable var = (DatalogParamVariable) vit.next();
          if (!bodyVars.contains(var)) {
            areSafe = false;
            System.err.println("UNSAFE RULE: " + var.toString() + " not defined in rule body:\n\t" + indr.toString());
          }
        }
      }
    }
    return areSafe;
  }

  /**
   * @return a List<String> containing all the names of the Backend predicates.
   */
  public List<String> getProgramBackendNames() {
    final List<String> res = new ArrayList<String>();
    for (DatalogBackend bnd : backends) {
      res.add(bnd.getModelName());
    }
    return res;
  }

  /**
   * Add a new Backend data source (EDBs) to this Datalog program.
   * 
   * @param b the Backend object used as an EDB source for this program.
   */
  public void addBackend(final DatalogBackend b) {
    backends.add(b);
  }

  public DatalogBackend getBackend(final String nm) {
    for (final Iterator<DatalogBackend> it = backends.iterator(); it.hasNext();) {
      final DatalogBackend bnd = it.next();
      if (bnd.getModelName().equals(nm)) {
        return bnd;
      }
    }
    return null;
  }

  public List<DatalogBackend> getBackends() {
    return backends;
  }

  private void createInputMap() {

    inputMap = new HashMap<String, String>();
    for (final Iterator<DatalogBackend> it = backends.iterator(); it.hasNext();) {
      final DatalogBackend bnd = it.next();
      final String nm = bnd.getModelName();
      final String fileName = bnd.getFilename();
      inputMap.put(nm, fileName);
    }
  }

  public HashMap<String, String> getInputMap() {
    return inputMap;
  }

  public void setRuleSet(final DatalogRuleSet rs) {
    if (rules != null) {
      System.err.println("Warning: Reseting ruleset from " + rules.toString() + " to " + rs.toString());
    }
    rules = rs;
  }

  public DatalogRuleSet getRuleSet() {
    return rules;
  }

  public void setAnswer(final DatalogAnswer ans) {
    if (answer != null) {
      System.err.println("Warning: Reseting answer from " + answer.toString() + " to " + ans.toString());
    }
    answer = ans;
  }

  public DatalogAtom getAnswer() {
    return answer.getAnswer();
  }

  public DependencyGraph getDependencyGraph() {
    return deps;
  }

  @Override
  public String toString() {
    String res = "DatalogProgram:\n";
    res = res + "backends:\n";
    for (int i = 0; i < backends.size(); i = i + 1) {
      final DatalogBackend db = backends.get(i);
      res = res + "\t" + db.toString() + "\n";
    }
    res = res + rules.toString() + "\n";
    res = res + answer.toString() + "\n";
    return res;
  }
}
