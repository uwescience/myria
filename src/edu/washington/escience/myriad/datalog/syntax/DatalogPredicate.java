package edu.washington.escience.myriad.datalog.syntax;

import java.util.ArrayList;
import java.util.List;

/**
 * DatalogPredicate contains a Datalog predicate, defined by a list of <code>DatalogRule</code>s.
 * 
 * @author mar, dhalperi
 * 
 */
public final class DatalogPredicate {
  /** The name of this (head) predicate. */
  private final String predicateName;
  /** The list of rules that define this head predicate. */
  private final List<DatalogRule> definingRules = new ArrayList<DatalogRule>();

  /**
   * Constructs a new predicate corresponding to this name, with an empty set of rules.
   * 
   * @param name the name of this predicate.
   */
  public DatalogPredicate(final String name) {
    predicateName = name;
  }

  /**
   * Add a rule to the definition of this predicate.
   * 
   * @param rule the rule to be added.
   */
  public void addRule(final DatalogRule rule) {
    definingRules.add(rule);
  }

  /**
   * @return the list of rules that define this predicate.
   */
  public List<DatalogRule> getDefiningRules() {
    return definingRules;
  }

  /**
   * @return the name of this predicate.
   */
  public String getPredicateName() {
    return predicateName;
  }

  @Override
  public String toString() {
    String res = "*** predicateName: " + predicateName + " ***\n";
    for (final DatalogRule rule : definingRules) {
      res = res + rule.toString() + "\n";
    }
    return res;
  }
}