package edu.washington.escience.myria.api.encoding.meta;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.washington.escience.myria.api.encoding.MyriaApiEncoding;
import edu.washington.escience.myria.parallel.meta.MetaTask;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @Type(name = "Fragment", value = FragmentEncoding.class), @Type(name = "Sequence", value = SequenceEncoding.class), })
public abstract class MetaTaskEncoding extends MyriaApiEncoding {
  abstract MetaTask getTask();
}
