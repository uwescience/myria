package edu.washington.escience.myria.systemtest;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.accessmethod.JdbcInfo;
import edu.washington.escience.myria.operator.BlockingJDBCDataReceiver;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.JdbcSQLProcessor;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.util.TestUtils;

public class ParallelJDBCTest extends SystemTestBase {

  @Test
  public void parallelTestJDBC() throws Exception {
    final String host = "54.245.108.198";
    final int port = 3306;
    final String username = "myriad";
    final String password = "nays26[shark";
    final String dbms = "mysql";
    final String databaseName = "myriad_test";

    final String driverClass = "com.mysql.jdbc.Driver";
    final JdbcInfo jdbcInfo = JdbcInfo.of(driverClass, dbms, host, port, databaseName, username, password);

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID worker2ReceiveID = ExchangePairID.newID();

    final ImmutableList<Type> types = ImmutableList.of(Type.INT_TYPE, Type.STRING_TYPE);
    final ImmutableList<String> columnNames = ImmutableList.of("id", "name");
    final Schema schema = new Schema(types, columnNames);

    final DbQueryScan scan1 = new DbQueryScan(jdbcInfo, "select distinct * from testtable3", schema);

    final CollectProducer cp1 = new CollectProducer(scan1, worker2ReceiveID, WORKER_ID[1]);

    final DbQueryScan scan2 = new DbQueryScan(jdbcInfo, "select distinct * from testtable4", schema);

    final CollectProducer cp2 = new CollectProducer(scan2, worker2ReceiveID, WORKER_ID[1]);
    final CollectConsumer cc2 =
        new CollectConsumer(cp2.getSchema(), worker2ReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final BlockingJDBCDataReceiver block2 = new BlockingJDBCDataReceiver("temptable34", jdbcInfo, cc2);

    final JdbcSQLProcessor scan22 =
        new JdbcSQLProcessor(jdbcInfo, "select distinct * from temptable34", schema, new Operator[] { block2 });
    final CollectProducer cp22 = new CollectProducer(scan22, serverReceiveID, MASTER_ID);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(WORKER_ID[0], new RootOperator[] { cp1 });
    workerPlans.put(WORKER_ID[1], new RootOperator[] { cp2, cp22 });

    final CollectConsumer serverCollect = new CollectConsumer(schema, serverReceiveID, new int[] { WORKER_ID[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).sync();
    TupleBatchBuffer actualResult = new TupleBatchBuffer(queueStore.getSchema());
    TupleBatch tb = null;
    while (!receivedTupleBatches.isEmpty()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        tb.compactInto(actualResult);
      }
    }
    final HashMap<Tuple, Integer> resultBag = TestUtils.tupleBatchToTupleBag(actualResult);
    // TODO test the result.

  }
}
