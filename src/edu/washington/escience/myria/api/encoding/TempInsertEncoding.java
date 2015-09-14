package edu.washington.escience.myria.api.encoding;

import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.base.MoreObjects;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.DbInsertTemp;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class TempInsertEncoding extends UnaryOperatorEncoding<DbInsertTemp> {
  /** The name under which the dataset will be stored. */
  @Required
  public String table;
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
  public DbInsertTemp construct(@Nonnull ConstructArgs args) {
    /* default overwrite to {@code false}, so we append. */
    argOverwriteTable = MoreObjects.firstNonNull(argOverwriteTable, Boolean.FALSE);
    return new DbInsertTemp(null, RelationKey.ofTemp(args.getQueryId(), table), connectionInfo, argOverwriteTable,
        indexes);
  }
}