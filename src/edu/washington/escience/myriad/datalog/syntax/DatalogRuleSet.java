package edu.washington.escience.myriad.datalog.syntax;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * This class holds a set of Datalog rules, organized by the head predicate.
 * 
 * @author mar, dhalperi
 */
public final class DatalogRuleSet {
  /** The definitions of each of the head predicates in this set of Datalog rules. */
  private final List<DatalogPredicate> predicates = new ArrayList<DatalogPredicate>();
  /** A mapping from the name of a head predicate to its definition. */
  private final HashMap<String, DatalogPredicate> ruleSetMap = new HashMap<String, DatalogPredicate>();

  /** Primitive constructor sets up no state. */
  public DatalogRuleSet() {
  }

  /**
   * Adds a rule to this DatalogRuleSet. Creates a new Predicate if the head predicate has not been seen before.
   * Otherwise, appends this new rule to the Predicate's definition.
   * 
   * @param rule the rule to be added to this DatalogRuleSet.
   */
  public void addRule(final DatalogRule rule) {
    final String headPredicateName = rule.getHeadName();

    /* Get or create the DatalogPredicate corresponding to the rule's head predicate */
    DatalogPredicate r;
    if (ruleSetMap.containsKey(headPredicateName)) {
      /* Existing head predicate */
      r = ruleSetMap.get(headPredicateName);
    } else {
      /* New head predicate */
      r = new DatalogPredicate(headPredicateName);
      ruleSetMap.put(headPredicateName, r);
      predicates.add(r);
    }
    /* Add the rule */
    r.addRule(rule);
  }

  /**
   * Get the definition of a specific predicate of the given name.
   * 
   * @param name the name of the desired predicate.
   * @return the definition of the specified predicate, or null if no such predicate exists.
   */
  public DatalogPredicate getDefinedRule(final String name) {
    if (ruleSetMap.containsKey(name)) {
      return ruleSetMap.get(name);
    }
    return null;
  }

  /**
   * @return a list of Datalog predicates defined by this rule set. Each predicate contains one or more defining rules.
   */
  public List<DatalogPredicate> getPredicates() {
    return predicates;
  }

  @Override
  public String toString() {
    String res = "";

    for (int i = 0; i < predicates.size(); i = i + 1) {
      final DatalogPredicate r = predicates.get(i);
      res = res + r.toString() + "\n";
    }
    return res;
  }
}