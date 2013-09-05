package edu.washington.escience.myria.api.encoding;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.washington.escience.myria.operator.StreamingStateUpdater;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @Type(value = DupElimEncoding.class, name = "DupElim"),
    @Type(value = KeepMinValueEncoding.class, name = "KeepMinValue") })
public abstract class StreamingStateUpdaterEncoding<T extends StreamingStateUpdater> extends MyriaApiEncoding {
  /**
   * @return the instantiated StreamingStateUpdater.
   */
  public abstract T construct();

}