package edu.washington.escience.myria.operator.agg;

import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * The Aggregation operator that computes an aggregate.
 * 
 * This class does not do group by.
 */
public final class Aggregate extends UnaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** Does the actual aggregation work. */
  private Aggregator<?>[] agg;
  /** Which fields the aggregate is computed over. */
  private final int[] afields;
  /** Aggregate operators. */
  private final int[] aggOps;

  /**
   * buffer for holding results.
   * */
  private transient TupleBatchBuffer aggBuffer;

  /**
   * Constructor. <br>
   * 
   * <p>
   * The {@link Aggregate} is able to compute multiple aggregates over multiple columns simultaneously. The multiple
   * columns are specified in afields. The aggregate operations needed for each column is specified in aggOps. Multiple
   * aggregates for a column is composed by the binary OR operation. For example, <br>
   * 
   * <code>Aggregator.AGG_OP_MIN | Aggregator.AGG_OP_MAX </code> denotes computing a min and a max simultaneously. <br>
   * 
   * <p>
   * 
   * The result column of the {@link Aggregate} operator is ordered in two layers. Firstly, if a column c1 appears
   * before another column c2 in the afields array, all the aggregate results of c1 appears before those of c2.
   * Secondly, if multiple aggregate operations are computed on a column, the result of these aggregate operations are
   * orderd by the digital number representation of them in {@link Aggregator}. For example,
   * {@link Aggregator#AGG_OP_COUNT} is 0x01, {@link Aggregator#AGG_OP_SUM} is 0x08, then count result appears before
   * sum.
   * 
   * <p>
   * 
   * Example.
   * 
   * Suppose we are going to compute a sum and an average on column 0, and a min on column 1, the Aggregate should be
   * constructed as:
   * 
   * <pre>
   * Aggregate agg = new Aggregate(child, new int[]{0,1}, new int[]{Aggregator.AGG_OP_SUM | Aggregator.AGG_OP_AVG, Aggregator.AGG_OP_MIN});
   * </pre>
   * 
   * Let's assume the child schema is <code>(INT_TYPE, INT_TYPE)</code>, then the the output schema of the aggregate is
   * <code>( INT_TYPE (sum), DOUBLE_TYPE (avg), INT_TYPE (min) ) </code>
   * 
   * Implementation hint: depending on the type of afield, you will want to construct an {@link IntAggregator} or
   * {@link StringAggregator} to help you with your implementation of readNext(). <br>
   * 
   * 
   * @param child The Operator that is feeding us tuples.
   * @param afields The columns over which we are computing aggregates. Each column will be computed a set of aggregates
   *          denoted in the aggOps.
   * @param aggOps The aggregation operator to use
   */
  public Aggregate(final Operator child, final int[] afields, final int[] aggOps) {
    super(child);
    Objects.requireNonNull(afields);
    Preconditions.checkArgument(afields.length == aggOps.length, "one aggOp for each agg field");
    if (afields.length == 0) {
      throw new IllegalArgumentException("aggregation fields must not be empty");
    }
    this.afields = afields;
    this.aggOps = aggOps;
    agg = new Aggregator<?>[aggOps.length];
  }

  /**
   * @return the aggregate field
   * */
  public int[] aggregateFields() {
    return afields;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = null;
    final Operator child = getChild();

    if (child.eos()) {
      return aggBuffer.popAny();
    }

    while ((tb = child.nextReady()) != null) {
      for (int i = 0; i < agg.length; ++i) {
        agg[i].add(tb, afields[i]);
      }
    }

    if (child.eos()) {
      int fromIndex = 0;
      for (final Aggregator<?> element : agg) {
        element.getResult(aggBuffer, fromIndex);
        fromIndex += element.getResultSchema().numColumns();
      }
      return aggBuffer.popAny();
    }
    return null;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    aggBuffer = new TupleBatchBuffer(getSchema());
  }

  @Override
  protected Schema generateSchema() {
    if (getChild() == null) {
      return null;
    }
    final Schema childSchema = getChild().getSchema();
    if (childSchema == null) {
      return null;
    }

    final ImmutableList.Builder<Type> gTypes = ImmutableList.builder();
    final ImmutableList.Builder<String> gNames = ImmutableList.builder();

    agg = AggUtils.allocate(childSchema, afields, aggOps);
    for (final Aggregator<?> a : agg) {
      gTypes.addAll(a.getResultSchema().getColumnTypes());
      gNames.addAll(a.getResultSchema().getColumnNames());
    }
    return new Schema(gTypes, gNames);
  }
}
