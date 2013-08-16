package edu.washington.escience.myria.api.encoding;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.washington.escience.myria.Predicate;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @Type(value = WithinSumRangePredicateEncoding.class, name = "WithinSumRangePredicate"),
    @Type(value = EqualsPredicateEncoding.class, name = "EqualsPredicate"),
    @Type(value = NotEqualsPredicateEncoding.class, name = "NotEqualsPredicate") })
public abstract class PredicateEncoding<T extends Predicate> extends MyriaApiEncoding {
  /**
   * @return the instantiated Predicate.
   */
  public abstract T construct();
}
