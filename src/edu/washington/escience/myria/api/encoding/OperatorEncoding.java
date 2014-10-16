package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.Operator;

/**
 * A JSON-able wrapper for the expected wire message for an operator. To add a new operator, two things need to be done.
 * 
 * 1. Create an Encoding class that extends OperatorEncoding.
 * 
 * 2. Add the operator to the list of (alphabetically sorted) JsonSubTypes below.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "opType")
@JsonSubTypes({
    @Type(name = "Aggregate", value = AggregateEncoding.class), @Type(name = "Apply", value = ApplyEncoding.class),
    @Type(name = "BinaryFileScan", value = BinaryFileScanEncoding.class),
    @Type(name = "BroadcastConsumer", value = BroadcastConsumerEncoding.class),
    @Type(name = "BroadcastProducer", value = BroadcastProducerEncoding.class),
    @Type(name = "CollectConsumer", value = CollectConsumerEncoding.class),
    @Type(name = "CollectProducer", value = CollectProducerEncoding.class),
    @Type(name = "Consumer", value = ConsumerEncoding.class), @Type(name = "Counter", value = CounterEncoding.class),
    @Type(name = "DbInsert", value = DbInsertEncoding.class),
    @Type(name = "DbQueryScan", value = QueryScanEncoding.class),
    @Type(name = "Difference", value = DifferenceEncoding.class),
    @Type(name = "DupElim", value = DupElimEncoding.class), @Type(name = "Empty", value = EmptyRelationEncoding.class),
    @Type(name = "EOSController", value = EOSControllerEncoding.class),
    @Type(name = "FileScan", value = FileScanEncoding.class), @Type(name = "Filter", value = FilterEncoding.class),
    @Type(name = "HyperShuffleProducer", value = HyperShuffleProducerEncoding.class),
    @Type(name = "HyperShuffleConsumer", value = HyperShuffleConsumerEncoding.class),
    @Type(name = "IDBController", value = IDBControllerEncoding.class),
    @Type(name = "InMemoryOrderBy", value = InMemoryOrderByEncoding.class),
    @Type(name = "LeapFrogJoin", value = LeapFrogJoinEncoding.class),
    @Type(name = "LocalMultiwayConsumer", value = LocalMultiwayConsumerEncoding.class),
    @Type(name = "LocalMultiwayProducer", value = LocalMultiwayProducerEncoding.class),
    @Type(name = "Merge", value = MergeEncoding.class), @Type(name = "MergeJoin", value = MergeJoinEncoding.class),
    @Type(name = "MultiGroupByAggregate", value = MultiGroupByAggregateEncoding.class),
    @Type(name = "NChiladaFileScan", value = NChiladaFileScanEncoding.class),
    @Type(name = "RightHashCountingJoin", value = RightHashCountingJoinEncoding.class),
    @Type(name = "RightHashJoin", value = RightHashJoinEncoding.class),
    @Type(name = "SeaFlowScan", value = SeaFlowFileScanEncoding.class),
    @Type(name = "SetGlobal", value = SetGlobalEncoding.class),
    @Type(name = "ShuffleConsumer", value = ShuffleConsumerEncoding.class),
    @Type(name = "ShuffleProducer", value = ShuffleProducerEncoding.class),
    @Type(name = "SingleGroupByAggregate", value = SingleGroupByAggregateEncoding.class),
    @Type(name = "Singleton", value = SingletonEncoding.class),
    @Type(name = "CrossWithSingleton", value = CrossWithSingletonEncoding.class),
    @Type(name = "SinkRoot", value = SinkRootEncoding.class),
    @Type(name = "StatefulApply", value = StatefulApplyEncoding.class),
    @Type(name = "SymmetricHashJoin", value = SymmetricHashJoinEncoding.class),
    @Type(name = "SymmetricHashCountingJoin", value = SymmetricHashCountingJoinEncoding.class),
    @Type(name = "TableScan", value = TableScanEncoding.class),
    @Type(name = "TempInsert", value = TempInsertEncoding.class),
    @Type(name = "TempTableScan", value = TempTableScanEncoding.class),
    @Type(name = "TipsyFileScan", value = TipsyFileScanEncoding.class),
    @Type(name = "UnionAll", value = UnionAllEncoding.class) })
public abstract class OperatorEncoding<T extends Operator> extends MyriaApiEncoding {

  @Required
  public Integer opId;

  public String opName;

  /**
   * Connect any operators to this one.
   */
  public abstract void connect(Operator operator, Map<Integer, Operator> operators);

  /**
   * @param args TODO
   * @return an instantiated operator.
   */
  public abstract T construct(@Nonnull ConstructArgs args) throws MyriaApiException;

}