package edu.washington.escience.myriad.systemtest;

import java.io.IOException;
import java.util.HashMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.BlockingDataReceiver;
import edu.washington.escience.myriad.operator.JdbcQueryScan;
import edu.washington.escience.myriad.operator.JdbcSQLProcessor;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.JdbcTupleBatch;
import edu.washington.escience.myriad.parallel.Server;

public class ParallelJDBCTest extends SystemTestBase {

  public static void parallelTestJDBC(final String[] args) throws DbException, IOException {
    // create table testtable1 (id int, name varchar(20));
    // insert into testtable1 (id,name) values (1,'name1'), (2, 'name2');
    //
    // create table testtable2 (id int, name varchar(20));
    // insert into testtable2 (id,name) values (1,'name1'), (2, 'name2');
    //
    // create table temptable1 (id int, name varchar(20));

    // Process worker1P = new
    // ProcessBuilder("/usr/bin/java","-Dfile.encoding=UTF-8 -classpath /home/slxu/workspace/JdbcAccessMethod/bin:/home/slxu/workspace/JdbcAccessMethod/lib/mysql-connector-java-5.1.21-bin.jar:/home/slxu/workspace/JdbcAccessMethod/lib/sqlite4java-282/sqlite4java.jar:/home/slxu/workspace/JdbcAccessMethod/lib/guava-12.0.1.jar:/home/slxu/workspace/JdbcAccessMethod/lib/mina-core-2.0.4.jar:/home/slxu/workspace/JdbcAccessMethod/lib/mina-filter-compression-2.0.4.jar:/home/slxu/workspace/JdbcAccessMethod/lib/slf4j-api-1.6.1.jar:/home/slxu/workspace/JdbcAccessMethod/lib/slf4j-log4j12-1.6.1.jar:/home/slxu/workspace/JdbcAccessMethod/lib/log4j-1.2.17.jar:/home/slxu/workspace/JdbcAccessMethod/lib/jline-0.9.94.jar:/home/slxu/workspace/JdbcAccessMethod/lib/commons-lang3-3.1.jar edu.washington.escience.parallel.Worker localhost:9001 localhost:8001").start();
    // Process worker2P = new ProcessBuilder("java",
    // "-Dfile.encoding=UTF-8 -classpath /home/slxu/workspace/JdbcAccessMethod/bin:/home/slxu/workspace/JdbcAccessMethod/lib/mysql-connector-java-5.1.21-bin.jar:/home/slxu/workspace/JdbcAccessMethod/lib/sqlite4java-282/sqlite4java.jar:/home/slxu/workspace/JdbcAccessMethod/lib/guava-12.0.1.jar:/home/slxu/workspace/JdbcAccessMethod/lib/mina-core-2.0.4.jar:/home/slxu/workspace/JdbcAccessMethod/lib/mina-filter-compression-2.0.4.jar:/home/slxu/workspace/JdbcAccessMethod/lib/slf4j-api-1.6.1.jar:/home/slxu/workspace/JdbcAccessMethod/lib/slf4j-log4j12-1.6.1.jar:/home/slxu/workspace/JdbcAccessMethod/lib/log4j-1.2.17.jar:/home/slxu/workspace/JdbcAccessMethod/lib/jline-0.9.94.jar:/home/slxu/workspace/JdbcAccessMethod/lib/commons-lang3-3.1.jar edu.washington.escience.parallel.Worker localhost:9002 localhost:8001").start();

    final String username = "root";
    final String password = "1234";

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID worker2ReceiveID = ExchangePairID.newID();

    final Type[] types = new Type[] { Type.INT_TYPE, Type.STRING_TYPE };
    final String[] columnNames = new String[] { "id", "name" };
    final Schema outputSchema = new Schema(types, columnNames);

    final JdbcQueryScan scan1 =
        new JdbcQueryScan("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test",
            "select distinct * from testtable1", outputSchema, username, password);
    final CollectProducer cp1 = new CollectProducer(scan1, worker2ReceiveID, WORKER_ID[1]);

    final JdbcQueryScan scan2 =
        new JdbcQueryScan("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test",
            "select distinct * from testtable2", outputSchema, username, password);
    final CollectProducer cp2 = new CollectProducer(scan2, worker2ReceiveID, WORKER_ID[1]);
    // CollectProducer child, ParallelOperatorID operatorID, SocketInfo[] workers
    final JdbcTupleBatch bufferWorker2 =
        new JdbcTupleBatch(outputSchema, "temptable1", "jdbc:mysql://localhost:3306/test", "com.mysql.jdbc.Driver",
            username, password);
    final CollectConsumer cc2 = new CollectConsumer(cp2, worker2ReceiveID, new int[] { 1, 2 });
    final BlockingDataReceiver block2 = new BlockingDataReceiver(bufferWorker2, cc2);
    final JdbcSQLProcessor scan22 =
        new JdbcSQLProcessor("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test",
            "select distinct * from temptable1", outputSchema, block2, username, password);
    final CollectProducer cp22 = new CollectProducer(scan22, serverReceiveID, MASTER_ID);
    final HashMap<Integer, Operator> workerPlans = new HashMap<Integer, Operator>();
    workerPlans.put(WORKER_ID[0], cp1);
    workerPlans.put(WORKER_ID[1], cp22);

    while (Server.runningInstance == null) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
      }
    }
    Server.runningInstance.dispatchWorkerQueryPlans(workerPlans);
    System.out.println("Query dispatched to the workers");
    Server.runningInstance.startServerQuery(0, new CollectConsumer(outputSchema, serverReceiveID,
        new int[] { WORKER_ID[1] }));
  }
}
