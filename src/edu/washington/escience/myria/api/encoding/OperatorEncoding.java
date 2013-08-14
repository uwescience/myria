package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

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
    @Type(name = "SinkRoot", value = SinkRootEncoding.class),
    @Type(name = "LocalMultiwayProducer", value = LocalMultiwayProducerEncoding.class),
    @Type(name = "LocalJoin", value = LocalJoinEncoding.class),
    @Type(name = "LocalCountingJoin", value = LocalCountingJoinEncoding.class),
    @Type(name = "MultiGroupByAggregate", value = MultiGroupByAggregateEncoding.class),
    @Type(name = "SingleGroupByAggregate", value = SingleGroupByAggregateEncoding.class),
    @Type(name = "DbInsert", value = DbInsertEncoding.class), @Type(name = "FileScan", value = FileScanEncoding.class),
    @Type(name = "BinaryFileScan", value = BinaryFileScanEncoding.class),
    @Type(name = "TipsyFileScan", value = TipsyFileScanEncoding.class),
    @Type(name = "EOSController", value = EOSControllerEncoding.class),
    @Type(name = "IDBInput", value = IDBInputEncoding.class),
    @Type(name = "Aggregate", value = AggregateEncoding.class), @Type(name = "Merge", value = MergeEncoding.class),
    @Type(name = "TableScan", value = TableScanEncoding.class), @Type(name = "Project", value = ProjectEncoding.class),
    @Type(name = "Apply", value = ApplyEncoding.class), @Type(name = "DbQueryScan", value = QueryScanEncoding.class),
    @Type(name = "Filter", value = FilterEncoding.class),
    @Type(name = "BroadcastProducer", value = BroadcastProducerEncoding.class),
    @Type(name = "BroadcastConsumer", value = BroadcastConsumerEncoding.class) })
public abstract class OperatorEncoding<T extends Operator> extends MyriaApiEncoding {

  public String opName;

  /**
   * Connect any operators to this one.
   */
  public abstract void connect(Operator operator, Map<String, Operator> operators);

  /**
   * @param server the Myria server for which this operator will be used.
   * @return an instantiated operator.
   */
  public abstract T construct(Server server);

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