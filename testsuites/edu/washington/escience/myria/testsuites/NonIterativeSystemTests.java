package edu.washington.escience.myria.testsuites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import edu.washington.escience.myria.operator.apply.ConstantMultiplicationIFunctionTest;
import edu.washington.escience.myria.operator.apply.PowIFunctionTest;
import edu.washington.escience.myria.operator.apply.SqrtIFunctionTest;
import edu.washington.escience.myria.systemtest.BroadcastTest;
import edu.washington.escience.myria.systemtest.CollectTest;
import edu.washington.escience.myria.systemtest.FlowControlTest;
import edu.washington.escience.myria.systemtest.IterativeSelfJoinTest;
import edu.washington.escience.myria.systemtest.LocalMultiwayProducerTest;
import edu.washington.escience.myria.systemtest.MultithreadScanTest;
import edu.washington.escience.myria.systemtest.OperatorTestUsingSQLiteStorage;
import edu.washington.escience.myria.systemtest.ParallelDistinctUsingSQLiteTest;
import edu.washington.escience.myria.systemtest.QueryFailureTest;
import edu.washington.escience.myria.systemtest.QueryKillTest;
import edu.washington.escience.myria.systemtest.ShuffleSQLiteTest;
import edu.washington.escience.myria.systemtest.SplitDataTest;
import edu.washington.escience.myria.systemtest.UnionAllTest;

@RunWith(Suite.class)
@SuiteClasses({ ConstantMultiplicationIFunctionTest.class,//
    PowIFunctionTest.class,//
    SqrtIFunctionTest.class,//
    CollectTest.class,//
    FlowControlTest.class, //
    IterativeSelfJoinTest.class, //
    LocalMultiwayProducerTest.class, //
    UnionAllTest.class, //
    MultithreadScanTest.class, //
    OperatorTestUsingSQLiteStorage.class, //
    ParallelDistinctUsingSQLiteTest.class, //
    ShuffleSQLiteTest.class, //
    SplitDataTest.class, //
    QueryKillTest.class, //
    QueryFailureTest.class, //
    BroadcastTest.class, //
})
public class NonIterativeSystemTests {

}
