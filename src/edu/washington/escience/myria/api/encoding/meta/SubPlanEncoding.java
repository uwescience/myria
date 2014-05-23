package edu.washington.escience.myria.api.encoding.meta;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.washington.escience.myria.api.encoding.MyriaApiEncoding;
import edu.washington.escience.myria.parallel.MetaTask;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @Type(name = "SubQuery", value = SubQueryEncoding.class), @Type(name = "Sequence", value = SequenceEncoding.class), })
public abstract class SubPlanEncoding extends MyriaApiEncoding {
  /**
   * Generate a {@link MetaTask} from this encoding.
   * 
   * @return the {@link MetaTask} corresponding to this encoding.
   */
  public abstract MetaTask getTask();

  /**
   * Return the set of workers requested by the client for this subplan.
   * 
   * @return the set of workers requested by the client for this subplan.
   */
  public abstract Set<Integer> getWorkers();
}
