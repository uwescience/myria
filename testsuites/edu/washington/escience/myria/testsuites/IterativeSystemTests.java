package edu.washington.escience.myria.testsuites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import edu.washington.escience.myria.systemtest.IterativeFailureTest;
import edu.washington.escience.myria.systemtest.IterativeFlowControlTest;
import edu.washington.escience.myria.systemtest.MultipleIDBTest;
import edu.washington.escience.myria.systemtest.TransitiveClosureWithEOITest;

@RunWith(Suite.class)
@SuiteClasses({ MultipleIDBTest.class,//
    TransitiveClosureWithEOITest.class,//
    IterativeFlowControlTest.class,//
    IterativeFailureTest.class})
public class IterativeSystemTests {

}
