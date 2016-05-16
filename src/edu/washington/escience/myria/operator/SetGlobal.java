package edu.washington.escience.myria.operator;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * A utility class used to update the global variables of a query at the master.
 *
 */
public class SetGlobal extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The {@link Server} on which the queries are run. */
  private final Server server;

  /** The name of the global variable to be updated. */
  private final String key;

  /** The query that is running. */
  private Long queryId;

  /** Whether we have written the global yet. */
  private boolean hasWritten;

  /**
   * This operator will update the server's catalog with the tuple counts supplied by the child. The child schema is
   * expected to be (userName:string, programName:string, relationName:string, count:long).
   *
   * @param child the source of tuples.
   * @param key the variable whose value will be set.
   * @param server the server whose catalog will be updated.
   */
  public SetGlobal(final Operator child, @Nonnull final String key, @Nonnull final Server server) {
    super(child);
    this.server = Objects.requireNonNull(server, "server");
    this.key = Objects.requireNonNull(key, "key");
    queryId = null;
    hasWritten = false;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws Exception {
    int nodeId =
        (Integer)
            Preconditions.checkNotNull(
                execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_NODE_ID), "node ID in execEnvVars");
    Preconditions.checkArgument(
        nodeId == MyriaConstants.MASTER_ID, "%s can only be run on the master", SetGlobal.class);
    Schema schema = Preconditions.checkNotNull(getSchema(), "schema cannot be null");
    Preconditions.checkArgument(
        schema.numColumns() == 1,
        "the child of %s must be a singleton and have only 1 column, not %s",
        SetGlobal.class,
        schema.numColumns());
    queryId =
        (Long)
            Preconditions.checkNotNull(
                execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_QUERY_ID), "query ID in execEnvVars");
  }

  @SuppressWarnings("deprecation")
  @Override
  protected void consumeTuples(final TupleBatch tuples) throws DbException {
    for (int i = 0; i < tuples.numTuples(); ++i) {
      Preconditions.checkState(
          !hasWritten,
          "In query %s: have already written to the global variable %s. Further writes violate the invariant",
          queryId,
          key);
      server.setQueryGlobal(queryId, key, tuples.getObject(0, i));
      hasWritten = true;
    }
  }

  @Override
  protected void childEOI() throws DbException {
    /* Do nothing. */
  }

  @Override
  protected void childEOS() throws DbException {
    /* Do nothing. */
  }
}
