package edu.washington.escience.myria.expression.evaluate;

import java.util.HashMap;
import java.util.List;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;

/**
 * Parameters passed to expressions.
 */
public class SqlExpressionOperatorParameter {

  /** The schemas of the base relations. */
  private HashMap<RelationKey, Schema> schemas;

  /**
   * @param schemas the schemas to set
   */
  public void setSchemas(final HashMap<RelationKey, Schema> schemas) {
    this.schemas = schemas;
  }

  /** The id of the worker that is running the expression. */
  private final Integer workerID;

  /** The dbms. */
  private final String dbms;

  /** Aliases for relations. */
  private HashMap<RelationKey, String> aliases;

  /**
   * @return the dbms
   */
  public String getDbms() {
    return dbms;
  }

  /**
   * Simple constructor.
   */
  public SqlExpressionOperatorParameter() {
    this(null, -1);
  }

  /**
   * @param workerID the worker id
   * @param dbms the dbms
   */
  public SqlExpressionOperatorParameter(final String dbms, final int workerID) {
    this(null, dbms, workerID);
  }

  /**
   * @param schemas schemas of the input relations
   * @param workerID the worker id
   * @param dbms the dbms
   */
  public SqlExpressionOperatorParameter(final HashMap<RelationKey, Schema> schemas, final String dbms,
      final int workerID) {
    aliases = null;
    this.schemas = schemas;
    this.workerID = workerID;
    this.dbms = dbms;
  }

  /**
   * @return the schemas
   */
  public HashMap<RelationKey, Schema> getSchemas() {
    return schemas;
  }

  /**
   * @param relation the relation to return the schema for
   * @return the schema
   */
  public Schema getSchema(final RelationKey relation) {
    return schemas.get(relation);
  }

  /**
   * @return the id of the worker that the expression is executed on
   */
  public int getWorkerId() {
    return workerID;
  }

  /**
   * @return the aliases
   */
  public HashMap<RelationKey, String> getAliases() {
    return aliases;
  }

  /**
   * @param relation the relation to return the schema for
   * @return the alias
   */
  public String getAlias(final RelationKey relation) {
    return aliases.get(relation);
  }

  /**
   * Generate aliases for all relations and add them to the alias list.
   * 
   * @param fromRelations relations to create aliases for
   */
  public void generateAliases(final List<RelationKey> fromRelations) {
    aliases = new HashMap<>();
    int i = 0;
    for (RelationKey relation : fromRelations) {
      aliases.put(relation, "rel" + i++);
    }
  }
}
