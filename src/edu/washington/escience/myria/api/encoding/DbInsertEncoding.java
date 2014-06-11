package edu.washington.escience.myria.api.encoding;

import java.util.List;

import com.google.common.base.Objects;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.operator.AbstractDbInsert;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbInsertTemp;
import edu.washington.escience.myria.parallel.Server;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class DbInsertEncoding extends UnaryOperatorEncoding<AbstractDbInsert> {
  /** The name under which the dataset will be stored. */
  @Required
  public RelationKey relationKey;
  /** Whether to overwrite an existing dataset. */
  public Boolean argOverwriteTable;
  /** Indexes created. */
  public List<List<IndexRef>> indexes;
  /** Whether this relation is temporary or will be added to the catalog. */
  public Boolean argTemporary;
  /**
   * The ConnectionInfo struct determines what database the data will be written to. If null, the worker's default
   * database will be used.
   */
  public ConnectionInfo connectionInfo;

  @Override
  public AbstractDbInsert construct(Server server) {
    /* default overwrite to {@code false}, so we append. */
    argOverwriteTable = Objects.firstNonNull(argOverwriteTable, Boolean.FALSE);
    /* default temporary to {@code false}, so we persist the inserted relation. */
    argTemporary = Objects.firstNonNull(argTemporary, Boolean.FALSE);
    if (argTemporary) {
      return new DbInsertTemp(null, relationKey, connectionInfo, argOverwriteTable, indexes);
    }
    return new DbInsert(null, relationKey, connectionInfo, argOverwriteTable, indexes);
  }

}