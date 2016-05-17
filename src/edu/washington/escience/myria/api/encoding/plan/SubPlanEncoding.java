package edu.washington.escience.myria.api.encoding.plan;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.washington.escience.myria.api.encoding.MyriaApiEncoding;
import edu.washington.escience.myria.parallel.QueryPlan;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
  @Type(name = "DoWhile", value = DoWhileEncoding.class),
  @Type(name = "Sequence", value = SequenceEncoding.class),
  @Type(name = "SubQuery", value = SubQueryEncoding.class),
})
public abstract class SubPlanEncoding extends MyriaApiEncoding {
  /**
   * Generate a {@link QueryPlan} from this encoding.
   *
   * @return the {@link QueryPlan} corresponding to this encoding.
   */
  public abstract QueryPlan getPlan();

  /**
   * Return the set of workers requested by the client for this subplan.
   *
   * @return the set of workers requested by the client for this subplan.
   */
  public abstract Set<Integer> getWorkers();
}
