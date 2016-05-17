package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.encoding.DatasetStatus;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding.Status;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.LessThanExpression;
import edu.washington.escience.myria.expression.PlusExpression;
import edu.washington.escience.myria.expression.TimesExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbInsertTemp;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;
import edu.washington.escience.myria.operator.agg.SingleColumnAggregatorFactory;
import edu.washington.escience.myria.parallel.DoWhile;
import edu.washington.escience.myria.parallel.Query;
import edu.washington.escience.myria.parallel.QueryPlan;
import edu.washington.escience.myria.parallel.Sequence;
import edu.washington.escience.myria.parallel.SubQuery;
import edu.washington.escience.myria.parallel.SubQueryPlan;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.JsonAPIUtils;

public class DoWhileTest extends SystemTestBase {

  /**
   * Here's the equivalent MyriaL query:
   *
   * <pre>
   * x = [0 as val, 1 as exp];
   * do
   *     x = [from x emit val+1 as val, 2*exp as exp];
   * while [from x emit max(val) < 5];
   * store(x, OUTPUT);
   * </pre>
   *
   * This computes {@code exp=pow(2, val)} up to {@code val=5}.
   */
  @Test
  public void testDoWhile() throws Exception {

    /* x */
    RelationKey x = RelationKey.of("test", "dowhile", "x");
    /* condition */
    String condition = "condition";

    /* x = [0 as val, 1 as exp]; */
    Schema schema = Schema.ofFields("val", Type.INT_TYPE, "exp", Type.INT_TYPE);
    TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    tbb.putInt(0, 0);
    tbb.putInt(1, 1);
    TupleSource source = new TupleSource(tbb);
    DbInsert insert = new DbInsert(source, x, true);
    // All the work done before the main body.
    SubQuery pre = wrapForWorker1(insert);

    /* Body expression: x = [from x emit val+1 as val, 2*exp as exp]. */
    DbQueryScan scan = new DbQueryScan(x, schema);
    List<Expression> expressions =
        ImmutableList.of(
            new Expression(
                "val", new PlusExpression(new VariableExpression(0), new ConstantExpression(1))),
            new Expression(
                "exp", new TimesExpression(new VariableExpression(1), new ConstantExpression(2))));
    Apply apply = new Apply(scan, expressions);
    DbInsert writeBack = new DbInsert(apply, x, true);
    // The loop body
    SubQuery body = wrapForWorker1(writeBack);

    /* Condition: condition = [from x emit max(val) < 5]. */
    DbQueryScan scanXForCondition = new DbQueryScan(x, schema);
    Aggregate maxX =
        new Aggregate(scanXForCondition, new SingleColumnAggregatorFactory(0, AggregationOp.MAX));
    Expression filterExpression =
        new Expression(
            "f", new LessThanExpression(new VariableExpression(0), new ConstantExpression(5)));
    Apply maxXapply = new Apply(maxX, ImmutableList.of(filterExpression));
    DbInsertTemp writeCondition =
        new DbInsertTemp(maxXapply, RelationKey.ofTemp(1, condition), null, true, null);
    // The condition
    SubQuery updateCondition = wrapForWorker1(writeCondition);

    /* The DoWhile itself */
    DoWhile doWhile = new DoWhile(ImmutableList.of(body, updateCondition), condition);

    /* The finishing work. */
    DbQueryScan finalScan = new DbQueryScan(x, schema);
    DbInsert finalWrite = new DbInsert(finalScan, x, true);
    SubQuery post = wrapForWorker1(finalWrite);

    /* Execute the actual query */
    QueryPlan actualQuery = new Sequence(ImmutableList.of(pre, doWhile, post));
    QueryEncoding queryEncoding = new QueryEncoding();
    queryEncoding.rawQuery = "testDoWhile";
    queryEncoding.logicalRa = "testDoWhile";
    Query query = server.getQueryManager().submitQuery(queryEncoding, actualQuery).get();

    /* Tests. */
    assertEquals(query.getStatus(), Status.SUCCESS);
    DatasetStatus status = server.getDatasetStatus(x);
    assertEquals(1, status.getNumTuples());
    assertEquals(Long.valueOf(query.getQueryId()), status.getQueryId());
    String finalX =
        JsonAPIUtils.download(
            "localhost",
            masterDaemonPort,
            x.getUserName(),
            x.getProgramName(),
            x.getRelationName(),
            "csv");
    assertEquals("val,exp\r\n5,32\r\n", finalX);
  }

  private static SubQueryPlan emptyMasterPlan() {
    return new SubQueryPlan(new SinkRoot(new EOSSource()));
  }

  private SubQuery wrapForWorker1(final RootOperator op) {
    SubQueryPlan masterPlan = emptyMasterPlan();
    Map<Integer, SubQueryPlan> workerPlans = new HashMap<>();
    workerPlans.put(workerIDs[0], new SubQueryPlan(op));
    return new SubQuery(masterPlan, workerPlans);
  }
}
