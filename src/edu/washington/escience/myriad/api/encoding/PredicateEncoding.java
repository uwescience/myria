package edu.washington.escience.myriad.api.encoding;

import java.util.List;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonSubTypes.Type;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Predicate;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @Type(value = WithinSumRangePredicateEncoding.class, name = "WithinSumRangePredicate"),
    @Type(value = EqualsPredicateEncoding.class, name = "EqualsPredicate"),
    @Type(value = NotEqualsPredicateEncoding.class, name = "NotEqualsPredicate") })
public abstract class PredicateEncoding<T extends Predicate> extends MyriaApiEncoding {
  public String type;

  /**
   * @return the instantiated Predicate.
   */
  public abstract T construct();

  /**
   * @return the list of required arguments.
   */
  protected abstract List<String> getRequiredArguments();

  @Override
  protected List<String> getRequiredFields() {
    return new ImmutableList.Builder<String>().add("type").addAll(getRequiredArguments()).build();
  }
}
