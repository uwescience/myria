package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonSubTypes.Type;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.operator.Operator;

/**
 * A JSON-able wrapper for the expected wire message for an operator. To add a new operator, three things need to be
 * done.
 * 
 * 1. Create an Encoding class that extends OperatorEncoding.
 * 
 * 2. Add the operator to the list of JsonSubTypes below.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "op_type")
@JsonSubTypes({
    @Type(name = "CollectConsumer", value = CollectConsumerEncoding.class),
    @Type(name = "CollectProducer", value = CollectProducerEncoding.class),
    @Type(name = "Consumer", value = ConsumerEncoding.class),
    @Type(name = "ShuffleConsumer", value = ShuffleConsumerEncoding.class),
    @Type(name = "LocalMultiwayConsumer", value = LocalMultiwayConsumerEncoding.class),
    @Type(name = "ShuffleProducer", value = ShuffleProducerEncoding.class),
    @Type(name = "LocalMultiwayProducer", value = LocalMultiwayProducerEncoding.class),
    @Type(name = "LocalJoin", value = LocalJoinEncoding.class),
    @Type(name = "LocalCountingJoin", value = LocalCountingJoinEncoding.class),
    @Type(name = "MultiGroupByAggregate", value = MultiGroupByAggregateEncoding.class),
    @Type(name = "SQLiteInsert", value = SQLiteInsertEncoding.class),
    @Type(name = "EOSController", value = EOSControllerEncoding.class),
    @Type(name = "IDBInput", value = IDBInputEncoding.class),
    @Type(name = "Aggregate", value = AggregateEncoding.class),
    @Type(name = "SQLiteScan", value = SQLiteScanEncoding.class),
    @Type(name = "Project", value = ProjectEncoding.class), @Type(name = "Apply", value = ApplyEncoding.class),
    @Type(name = "Filter", value = FilterEncoding.class) })
public abstract class OperatorEncoding<T extends Operator> extends MyriaApiEncoding {

  public String opName;

  /**
   * Connect any operators to this one.
   */
  public abstract void connect(Operator operator, Map<String, Operator> operators);

  /**
   * @return an instantiated operator.
   */
  public abstract T construct();

  /**
   * @return the list of arguments required for this OperatorEncoding.
   */
  @JsonIgnore
  protected abstract List<String> getRequiredArguments();

  @Override
  protected final List<String> getRequiredFields() {
    return new ImmutableList.Builder<String>().add("opName").addAll(getRequiredArguments()).build();
  }
}