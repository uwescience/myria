package edu.washington.escience.myria.operator;

import java.util.List;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;

/**
 * Insert operator that will insert the tuples into a table and create a view on top of that table. The view will be as
 * follows
 * 
 * CREATE VIEW [viewRelationKey] AS SELECT [columns from viewSchema] FROM [relationKey] WHERE [condition];
 */
public class DbInsertWithView extends DbInsert {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The key of the view to be created. */
  private final RelationKey viewRelationKey;
  /** The schema of the view. */
  private final Schema viewSchema;
  /** The condition for the view. */
  private final String condition;

  /**
   * Constructs an insertion operator to store the tuples from the specified child into the specified database. If the
   * table does not exist, it will be created; if it does exist then old data will persist and new data will be
   * inserted. A view will be created on top of the table specified by <code>viewRelationKey</code>.
   * 
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param connectionInfo the parameters of the database connection.
   * @param viewRelationKey the relation key for the view.
   * @param viewSchema the schema of the view
   */
  public DbInsertWithView(final Operator child, final RelationKey relationKey, final ConnectionInfo connectionInfo,
      final RelationKey viewRelationKey, final Schema viewSchema) {
    this(child, relationKey, connectionInfo, false, viewRelationKey, viewSchema, null);
  }

  /**
   * Constructs an insertion operator to store the tuples from the specified child into the specified database. If the
   * table does not exist, it will be created; if it does exist then old data will persist and new data will be
   * inserted. A view will be created on top of the table specified by <code>viewRelationKey</code>.
   * 
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param connectionInfo the parameters of the database connection.
   * @param viewRelationKey the relation key for the view.
   * @param viewSchema the schema of the view
   * @param condition the WHERE clause for the view. If none is needed, null should be passed.
   */
  public DbInsertWithView(final Operator child, final RelationKey relationKey, final ConnectionInfo connectionInfo,
      final RelationKey viewRelationKey, final Schema viewSchema, final String condition) {
    this(child, relationKey, connectionInfo, false, viewRelationKey, viewSchema, condition);
  }

  /**
   * Constructs an insertion operator to store the tuples from the specified child into the worker's default database.
   * If the table does not exist, it will be created. If <code>overwriteTable</code> is <code>true</code>, any existing
   * data will be dropped. A view will be created on top of the table specified by <code>viewRelationKey</code>.
   * 
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param overwriteTable whether to overwrite a table that already exists.
   * @param viewRelationKey the relation key for the view.
   * @param viewSchema the schema of the view
   * @param condition the WHERE clause for the view. If none is needed, null should be passed.
   */
  public DbInsertWithView(final Operator child, final RelationKey relationKey, final boolean overwriteTable,
      final RelationKey viewRelationKey, final Schema viewSchema, final String condition) {
    this(child, relationKey, null, overwriteTable, viewRelationKey, viewSchema, condition);
  }

  /**
   * Constructs an insertion operator to store the tuples from the specified child into the specified database. If the
   * table does not exist, it will be created. If <code>overwriteTable</code> is <code>true</code>, any existing data
   * will be dropped.
   * 
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param overwriteTable whether to overwrite a table that already exists.
   * @param indexes indexes created.
   * @param viewRelationKey the relation key for the view.
   * @param viewSchema the schema of the view
   * @param condition the WHERE clause for the view. If none is needed, null should be passed.
   */
  public DbInsertWithView(final Operator child, final RelationKey relationKey, final boolean overwriteTable,
      final List<List<IndexRef>> indexes, final RelationKey viewRelationKey, final Schema viewSchema,
      final String condition) {
    this(child, relationKey, null, overwriteTable, indexes, viewRelationKey, viewSchema, condition);
  }

  /**
   * Constructs an insertion operator to store the tuples from the specified child into the specified database. If the
   * table does not exist, it will be created. If <code>overwriteTable</code> is <code>true</code>, any existing data
   * will be dropped. A view will be created on top of the table specified by <code>viewRelationKey</code>.
   * 
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param connectionInfo the parameters of the database connection.
   * @param overwriteTable whether to overwrite a table that already exists.
   * @param viewRelationKey the relation key for the view.
   * @param viewSchema the schema of the view
   * @param condition the WHERE clause for the view. If none is needed, null should be passed.
   */
  public DbInsertWithView(final Operator child, final RelationKey relationKey, final ConnectionInfo connectionInfo,
      final boolean overwriteTable, final RelationKey viewRelationKey, final Schema viewSchema, final String condition) {
    this(child, relationKey, connectionInfo, overwriteTable, null, viewRelationKey, viewSchema, condition);
  }

  /**
   * Constructs an insertion operator to store the tuples from the specified child into the specified database. If the
   * table does not exist, it will be created. If <code>overwriteTable</code> is <code>true</code>, any existing data
   * will be dropped. A view will be created on top of the table specified by <code>viewRelationKey</code>.
   * 
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param connectionInfo the parameters of the database connection.
   * @param overwriteTable whether to overwrite a table that already exists.
   * @param indexes the indexes to be created on the table. Each entry is a list of columns.
   * @param viewRelationKey the relation key for the view.
   * @param viewSchema the schema of the view
   * @param condition the WHERE clause for the view. If none is needed, null should be passed.
   */
  public DbInsertWithView(final Operator child, final RelationKey relationKey, final ConnectionInfo connectionInfo,
      final boolean overwriteTable, final List<List<IndexRef>> indexes, final RelationKey viewRelationKey,
      final Schema viewSchema, final String condition) {
    super(child, relationKey, connectionInfo, overwriteTable, indexes);
    this.viewRelationKey = viewRelationKey;
    this.viewSchema = viewSchema;
    this.condition = condition;
  }

  @Override
  protected void childEOS() throws DbException {
    super.childEOS();
    /* When the child is done. We create a view on top of the relation. */
    super.getAccessmethod().createView(viewRelationKey, super.getRelationKey(), viewSchema, condition);
  }

  /**
   * @return the name of the relation that this operator will write to.
   */
  @Override
  public RelationKey getRelationKey() {
    return viewRelationKey;
  }

  /**
   * @return the view schema
   */
  public Schema getViewSchema() {
    return viewSchema;
  }
}