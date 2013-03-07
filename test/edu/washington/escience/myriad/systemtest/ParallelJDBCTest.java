package edu.washington.escience.myriad.systemtest;

import java.io.IOException;
import java.util.HashMap;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.accessmethod.JdbcInfo;
import edu.washington.escience.myriad.operator.BlockingJDBCDataReceiver;
import edu.washington.escience.myriad.operator.JdbcQueryScan;
import edu.washington.escience.myriad.operator.JdbcSQLProcessor;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;

public class ParallelJDBCTest extends SystemTestBase {

  @Test
  public void parallelTestJDBC() throws DbException, IOException {
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

    final JdbcQueryScan scan1 = new JdbcQueryScan(jdbcInfo, "select distinct * from testtable3", schema);

    final CollectProducer cp1 = new CollectProducer(scan1, worker2ReceiveID, WORKER_ID[1]);

    final JdbcQueryScan scan2 = new JdbcQueryScan(jdbcInfo, "select distinct * from testtable4", schema);

    final CollectProducer cp2 = new CollectProducer(scan2, worker2ReceiveID, WORKER_ID[1]);
    final CollectConsumer cc2 =
        new CollectConsumer(cp2.getSchema(), worker2ReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final BlockingJDBCDataReceiver block2 = new BlockingJDBCDataReceiver("temptable34", jdbcInfo, cc2);

    final JdbcSQLProcessor scan22 =
        new JdbcSQLProcessor(jdbcInfo, "select distinct * from temptable34", schema, block2);
    final CollectProducer cp22 = new CollectProducer(scan22, serverReceiveID, MASTER_ID);
    final HashMap<Integer, Operator[]> workerPlans = new HashMap<Integer, Operator[]>();
    workerPlans.put(WORKER_ID[0], new Operator[] { cp1 });
    workerPlans.put(WORKER_ID[1], new Operator[] { cp2, cp22 });

    final CollectConsumer serverPlan = new CollectConsumer(schema, serverReceiveID, new int[] { WORKER_ID[1] });
    server.dispatchWorkerQueryPlans(0L, workerPlans);
    LOGGER.debug("Query dispatched to the workers");
    server.startServerQuery(0L, serverPlan);
  }
}
