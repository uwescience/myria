package edu.washington.escience.myria.api.encoding;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.washington.escience.myria.operator.StreamingState;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
  @Type(value = DupElimStateEncoding.class, name = "DupElim"),
  @Type(value = KeepMinValueStateEncoding.class, name = "KeepMinValue"),
  @Type(value = KeepAndSortOnMinValueStateEncoding.class, name = "KeepAndSortOnMinValue"),
  @Type(value = SimpleAppenderStateEncoding.class, name = "SimpleAppender")
})
public abstract class StreamingStateEncoding<T extends StreamingState> extends MyriaApiEncoding {
  /**
   * @return the instantiated StreamingState.
   */
  public abstract T construct();
}
