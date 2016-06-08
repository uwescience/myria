package edu.washington.escience.myria.operator;

import java.util.Objects;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.coordinator.MasterCatalog;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * A utility class used to update the {@link MasterCatalog} when a query finishes.
 *
 */
public class UpdateCatalog extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The {@link Server} on which the relations are created. */
  private final Server server;

  /**
   * This operator will update the server's catalog with the tuple counts supplied by the child. The child schema is
   * expected to be (userName:string, programName:string, relationName:string, count:long).
   *
   * @param child the source of tuples.
   * @param server the server whose catalog will be updated.
   */
  public UpdateCatalog(@Nonnull final Operator child, @Nonnull final Server server) {
    super(child);
    this.server = Objects.requireNonNull(server, "server");
  }

  @Override
  protected void consumeTuples(final TupleBatch tuples) throws DbException {
    for (int i = 0; i < tuples.numTuples(); ++i) {
      RelationKey relation =
          RelationKey.of(tuples.getString(0, i), tuples.getString(1, i), tuples.getString(2, i));
      long count = tuples.getLong(3, i);
      server.updateRelationTupleCount(relation, count);
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
