package edu.washington.escience.myriad.testsuites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import edu.washington.escience.myriad.systemtest.CollectTest;
import edu.washington.escience.myriad.systemtest.FlowControlTest;
import edu.washington.escience.myriad.systemtest.IterativeSelfJoinTest;
import edu.washington.escience.myriad.systemtest.LocalMultiwayProducerTest;
import edu.washington.escience.myriad.systemtest.MergeTest;
import edu.washington.escience.myriad.systemtest.MultithreadScanTest;
import edu.washington.escience.myriad.systemtest.OperatorTestUsingSQLiteStorage;
import edu.washington.escience.myriad.systemtest.ParallelDistinctUsingSQLiteTest;
import edu.washington.escience.myriad.systemtest.ParallelJDBCTest;
import edu.washington.escience.myriad.systemtest.ShuffleSQLiteTest;
import edu.washington.escience.myriad.systemtest.SplitDataTest;
import edu.washington.escience.myriad.operator.ApplyTest;
import edu.washington.escience.myriad.operator.apply.ConstantMultiplicationIFunctionTest;
import edu.washington.escience.myriad.operator.apply.PowIFunctionTest ;
import edu.washington.escience.myriad.operator.apply.SqrtIFunctionTest ;


@RunWith(Suite.class)
@SuiteClasses({ ConstantMultiplicationIFunctionTest.class,//
    PowIFunctionTest.class,//
    SqrtIFunctionTest.class,//
    ApplyTest.class,//
    CollectTest.class,//
    FlowControlTest.class, //
    IterativeSelfJoinTest.class, //
    LocalMultiwayProducerTest.class, //
    MergeTest.class, //
    MultithreadScanTest.class, //
    OperatorTestUsingSQLiteStorage.class, //
    ParallelDistinctUsingSQLiteTest.class, //
    ParallelJDBCTest.class, //
    ShuffleSQLiteTest.class, //
    SplitDataTest.class, })
// TransitiveClosureWithEOITest.class })
public class NonIterativeSystemTests {

}
