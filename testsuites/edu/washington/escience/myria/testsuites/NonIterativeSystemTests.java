package edu.washington.escience.myria.testsuites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import edu.washington.escience.myria.systemtest.BroadcastTest;
import edu.washington.escience.myria.systemtest.CollectTest;
import edu.washington.escience.myria.systemtest.FlowControlTest;
import edu.washington.escience.myria.systemtest.IterativeSelfJoinTest;
import edu.washington.escience.myria.systemtest.LocalMultiwayProducerTest;
import edu.washington.escience.myria.systemtest.MultithreadScanTest;
import edu.washington.escience.myria.systemtest.OperatorTestUsingSQLiteStorage;
import edu.washington.escience.myria.systemtest.QueryFailureTest;
import edu.washington.escience.myria.systemtest.QueryKillTest;
import edu.washington.escience.myria.systemtest.SplitDataTest;

@RunWith(Suite.class)
@SuiteClasses({ CollectTest.class,//
    FlowControlTest.class, //
    IterativeSelfJoinTest.class, //
    LocalMultiwayProducerTest.class, //
    MultithreadScanTest.class, //
    OperatorTestUsingSQLiteStorage.class, //
    SplitDataTest.class, //
    QueryKillTest.class, //
    QueryFailureTest.class, //
    BroadcastTest.class, //
})
public class NonIterativeSystemTests {

}
