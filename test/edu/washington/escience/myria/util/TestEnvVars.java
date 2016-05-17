package edu.washington.escience.myria.util;

import java.util.HashMap;
import java.util.Map;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.parallel.QueryExecutionMode;

public final class TestEnvVars {
  /** Utility classes cannot be constructed. */
  private TestEnvVars() {}

  /**
   * Construct a test execution environment variables dictionary.
   *
   * By default, the <code>node id</code> is -1.
   *
   * @return the specified execution environment variables.
   */
  public static Map<String, Object> get() {
    return get(-1);
  }

  /**
   * Construct a test execution environment variables dictionary.
   *
   * @param nodeId the node id (master or worker) that the operator is operating on.
   * @return the specified execution environment variables.
   */
  public static Map<String, Object> get(final int nodeId) {
    Map<String, Object> vars = new HashMap<>();

    vars.put(MyriaConstants.EXEC_ENV_VAR_NODE_ID, nodeId);
    vars.put(MyriaConstants.EXEC_ENV_VAR_EXECUTION_MODE, QueryExecutionMode.NON_BLOCKING);

    return vars;
  }
}
