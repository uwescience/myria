package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.MyriaJsonMapperProvider;
import edu.washington.escience.myria.expression.ConditionalExpression;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.GreaterThanExpression;
import edu.washington.escience.myria.expression.PlusExpression;
import edu.washington.escience.myria.expression.StateExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.agg.AggregatorFactory;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.UserDefinedAggregatorFactory;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.TestEnvVars;

public class UserDefinedAggregatorTest {

  private final ObjectReader reader =
      MyriaJsonMapperProvider.getReader().withType(AggregatorFactory.class);
  private final ObjectWriter writer = MyriaJsonMapperProvider.getWriter();
  private final int NUM_TUPLES = 2 * TupleUtils.getBatchSize(Type.LONG_TYPE);
  private final int NUM_TUPLES_20K = 2 * 10000;

  /**
   * Tests a re-implementation of the Count aggregate using a user-defined aggregate. Also tests serialization and
   * deserialization.
   *
   * @throws Exception if something goes wrong.
   */
  @Test
  public void testCount() throws Exception {
    final Schema schema = new Schema(ImmutableList.of(Type.STRING_TYPE), ImmutableList.of("name"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < NUM_TUPLES; i++) {
      tbb.putString(0, "Foo" + i);
    }

    Expression initializer = new Expression("counter", new ConstantExpression(0L));
    Expression increment =
        new Expression(
            "counter", new PlusExpression(new StateExpression(0), new ConstantExpression(1L)));
    Expression emitter = new Expression("index", new StateExpression(0));

    ImmutableList.Builder<Expression> Initializers = ImmutableList.builder();
    Initializers.add(initializer);

    ImmutableList.Builder<Expression> Updaters = ImmutableList.builder();
    Updaters.add(increment);

    ImmutableList.Builder<Expression> Emitters = ImmutableList.builder();
    Emitters.add(emitter);

    AggregatorFactory factory =
        new UserDefinedAggregatorFactory(Initializers.build(), Updaters.build(), Emitters.build());
    factory = reader.readValue(writer.writeValueAsString(factory));

    Aggregate agg = new Aggregate(new BatchTupleSource(tbb), new int[] {}, factory);
    agg.open(TestEnvVars.get());
    TupleBatch result;
    int resultSize = 0;
    while (!agg.eos()) {
      result = agg.nextReady();
      if (result != null) {
        assertEquals(1, result.numTuples());
        assertEquals(1, result.numColumns());
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(0));
        assertEquals(NUM_TUPLES, result.getLong(0, 0));
        resultSize += result.numTuples();
      }
    }
    assertEquals(1, resultSize);
    agg.close();
  }

  /**
   * Tests a re-implementation of the Count aggregate using a user-defined aggregate that also implements a constant
   * value column. Also tests serialization and deserialization.
   *
   * @throws Exception if something goes wrong.
   */
  @Test
  public void testCountAndConst() throws Exception {
    final Schema schema = new Schema(ImmutableList.of(Type.STRING_TYPE), ImmutableList.of("name"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < NUM_TUPLES; i++) {
      tbb.putString(0, "Foo" + i);
    }

    Expression initializer = new Expression("counter", new ConstantExpression(0L));
    Expression increment =
        new Expression(
            "counter", new PlusExpression(new StateExpression(0), new ConstantExpression(1L)));
    Expression emitter = new Expression("index", new StateExpression(0));
    Expression constEmitter = new Expression("const", new ConstantExpression(5L));

    ImmutableList.Builder<Expression> Initializers = ImmutableList.builder();
    Initializers.add(initializer);

    ImmutableList.Builder<Expression> Updaters = ImmutableList.builder();
    Updaters.add(increment);

    ImmutableList.Builder<Expression> Emitters = ImmutableList.builder();
    Emitters.add(emitter);
    Emitters.add(constEmitter);

    AggregatorFactory factory =
        new UserDefinedAggregatorFactory(Initializers.build(), Updaters.build(), Emitters.build());
    factory = reader.readValue(writer.writeValueAsString(factory));

    Aggregate agg = new Aggregate(new BatchTupleSource(tbb), new int[] {}, factory);
    agg.open(TestEnvVars.get());
    TupleBatch result;
    int resultSize = 0;
    while (!agg.eos()) {
      result = agg.nextReady();
      if (result != null) {
        assertEquals(1, result.numTuples());
        assertEquals(2, result.numColumns());
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(0));
        assertEquals(NUM_TUPLES, result.getLong(0, 0));
        assertEquals(5L, result.getLong(1, 0));
        resultSize += result.numTuples();
      }
    }
    assertEquals(1, resultSize);
    agg.close();
  }

  /**
   * Tests an arg-max-like aggregate function. Also tests serialization and deserialization.
   *
   * @throws Exception if something goes wrong.
   */
  @Test
  public void testRowOfMax() throws Exception {
    final Schema schema = new Schema(ImmutableList.of(Type.STRING_TYPE), ImmutableList.of("name"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < NUM_TUPLES_20K; i++) {
      tbb.putString(0, "Foo" + i);
    }

    ImmutableList.Builder<Expression> Initializers = ImmutableList.builder();
    Initializers.add(new Expression("counter", new ConstantExpression(0L)));
    Initializers.add(new Expression("maxrow", new ConstantExpression(-1L)));
    Initializers.add(new Expression("maxval", new ConstantExpression("")));

    ImmutableList.Builder<Expression> Updaters = ImmutableList.builder();
    // State.$0 counts the index of the current row.
    Updaters.add(
        new Expression(
            "counter", new PlusExpression(new StateExpression(0), new ConstantExpression(1L))));
    ExpressionOperator newRowIsBigger =
        new GreaterThanExpression(new VariableExpression(0), new StateExpression(2));
    // State.$1 tracks the index of the biggest row.
    Updaters.add(
        new Expression(
            "maxrow",
            new ConditionalExpression(
                newRowIsBigger, new StateExpression(0), new StateExpression(1))));
    // State.$2 tracks the value of the biggest row.
    Updaters.add(
        new Expression(
            "maxval",
            new ConditionalExpression(
                newRowIsBigger, new VariableExpression(0), new StateExpression(2))));

    ImmutableList.Builder<Expression> Emitters = ImmutableList.builder();
    Emitters.add(new Expression("indexOfMax", new StateExpression(1)));
    Emitters.add(new Expression("max", new StateExpression(2)));

    AggregatorFactory factory =
        new UserDefinedAggregatorFactory(Initializers.build(), Updaters.build(), Emitters.build());
    factory = reader.readValue(writer.writeValueAsString(factory));

    Aggregate agg = new Aggregate(new BatchTupleSource(tbb), new int[] {}, factory);
    agg.open(TestEnvVars.get());
    TupleBatch result;
    int resultSize = 0;
    while (!agg.eos()) {
      result = agg.nextReady();
      if (result != null) {
        assertEquals(1, result.numTuples());
        assertEquals(2, result.numColumns());
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(0));
        assertEquals(Type.STRING_TYPE, result.getSchema().getColumnType(1));
        assertEquals(9999, result.getLong(0, 0));
        assertEquals("Foo9999", result.getString(1, 0));
        resultSize += result.numTuples();
      }
    }
    assertEquals(1, resultSize);
    agg.close();
  }
}
