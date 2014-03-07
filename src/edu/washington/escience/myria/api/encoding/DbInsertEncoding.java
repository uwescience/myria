package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.parallel.Server;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class DbInsertEncoding extends UnaryOperatorEncoding<DbInsert> {
  /** The name under which the dataset will be stored. */
  @Required
  public RelationKey relationKey;
  /** The source of tuples to be inserted. */
  @Required
  public String argChild;
  /** Whether to overwrite an existing dataset. */
  public Boolean argOverwriteTable;
  /** Indexes created. */
  public List<List<IndexRef>> indexes;
  /**
   * The ConnectionInfo struct determines what database the data will be written to. If null, the worker's default
   * database will be used.
   */
  public ConnectionInfo connectionInfo;

  @Override
  public DbInsert construct(Server server) {
    if (argOverwriteTable == null) {
      argOverwriteTable = Boolean.FALSE;
    }
    return new DbInsert(null, relationKey, connectionInfo, argOverwriteTable, indexes);
  }

}