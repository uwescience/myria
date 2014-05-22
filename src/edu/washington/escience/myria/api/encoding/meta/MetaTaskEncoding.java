package edu.washington.escience.myria.api.encoding.meta;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.washington.escience.myria.api.encoding.MyriaApiEncoding;
import edu.washington.escience.myria.parallel.meta.MetaTask;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @Type(name = "Fragment", value = FragmentEncoding.class), @Type(name = "Sequence", value = SequenceEncoding.class), })
public abstract class MetaTaskEncoding extends MyriaApiEncoding {
  /**
   * Turn this MetaTaskEncoding into a {@link MetaTask}.
   * 
   * @return the {@link MetaTask} corresponding to this MetaTaskEncoding.
   */
  public abstract MetaTask getTask();

  /**
   * Return the set of workers requested by the client for this query.
   * 
   * @return the set of workers requested by the client for this query.
   */
  public abstract Set<Integer> getWorkers();
}
