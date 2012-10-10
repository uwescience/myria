package edu.washington.escience.myriad.datalog.syntax;

import java.util.Objects;
import java.util.Set;

/**
 * DatalogRule wraps a Head DatalogAtom and Body DatalogBody object into a single Datalog rule.
 * 
 * @author mar, dhalperi
 * 
 */
public final class DatalogRule {
  /** The head of this rule. */
  private final DatalogAtom head;
  /** The body of this rule. */
  private final DatalogBody body;

  /**
   * Builds a DatalogRule from a given head and body.
   * 
   * @param head the head atom of this rule.
   * @param body the body of this rule.
   */
  public DatalogRule(final DatalogAtom head, final DatalogBody body) {
    Objects.requireNonNull(head);
    Objects.requireNonNull(body);
    this.head = head;
    this.body = body;
  }

  /** @return the body of this rule. */
  public DatalogBody getBody() {
    return body;
  }

  /** @return a set of the names of predicates in the body of this rule. */
  public Set<String> getBodyAtomNames() {
    final Set<String> blst = body.getBodyAtomNames();
    return blst;
  }

  /** @return the head atom of this rule. */
  public DatalogAtom getHead() {
    return head;
  }

  /** @return the name of the head predicate of this rule. */
  public String getHeadName() {
    return head.getName();
  }

  @Override
  public String toString() {
    String res = "";
    res = res + "hd: ";
    res = res + head.toString() + " ";
    res = res + "body: ";
    res = res + body.toString() + " ";
    return res;
  }
}