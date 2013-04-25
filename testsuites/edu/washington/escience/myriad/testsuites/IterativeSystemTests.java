package edu.washington.escience.myriad.testsuites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import edu.washington.escience.myriad.systemtest.IterativeFlowControlTest;
import edu.washington.escience.myriad.systemtest.MultipleIDBTest;
import edu.washington.escience.myriad.systemtest.TransitiveClosureWithEOITest;
import edu.washington.escience.myriad.systemtest.IterativeFailureTest;

@RunWith(Suite.class)
@SuiteClasses({ MultipleIDBTest.class,//
    TransitiveClosureWithEOITest.class,//
    IterativeFlowControlTest.class,//
    IterativeFailureTest.class})
public class IterativeSystemTests {

}
