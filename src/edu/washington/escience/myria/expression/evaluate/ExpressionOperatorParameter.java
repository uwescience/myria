package edu.washington.escience.myria.expression.evaluate;

import java.util.HashMap;
import java.util.List;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;

/**
 * Object that carries parameters down the expression tree.
 */
public abstract class ExpressionOperatorParameter {
  /* For java generation */
  /** The input schema. */
  private Schema schema;
  /** The schema of the state. */
  private Schema stateSchema;

  /* For sql generation */
  /** The schemas of the base relations. */
  private HashMap<RelationKey, Schema> schemas;
  /** The dbms. */
  private String dbms;
  /** Aliases for relations. */
  private HashMap<RelationKey, String> aliases;

  /* shared */
  /** The id of the node (worker of master) that is running the expression. */
  private Integer nodeId;

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

  /**
   * @param relation the relation to return the schema for
   * @return the alias
   */
  public String getAlias(final RelationKey relation) {
    return aliases.get(relation);
  }

  /**
   * @return the aliases
   */
  public HashMap<RelationKey, String> getAliases() {
    return aliases;
  }

  /**
   * @return the dbms
   */
  public String getDbms() {
    return dbms;
  }

  /**
   * @return the input schema
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * @param relation the relation to return the schema for
   * @return the schema
   */
  public Schema getSchema(final RelationKey relation) {
    return schemas.get(relation);
  }

  /**
   * @return the schemas
   */
  public HashMap<RelationKey, Schema> getSchemas() {
    return schemas;
  }

  /**
   * @return the schema of the state
   */
  public Schema getStateSchema() {
    return stateSchema;
  }

  /**
   * @return the id of the worker that the expression is executed on
   */
  public int getWorkerId() {
    return nodeId;
  }

  /**
   * @param aliases the aliases to set
   */
  public void setAliases(final HashMap<RelationKey, String> aliases) {
    this.aliases = aliases;
  }

  /**
   * @param dbms the dbms to set
   */
  public void setDbms(final String dbms) {
    this.dbms = dbms;
  }

  /**
   * @param schema the schema to set
   */
  public void setSchema(final Schema schema) {
    this.schema = schema;
  }

  /**
   * @param schemas the schemas to set
   */
  public void setSchemas(final HashMap<RelationKey, Schema> schemas) {
    this.schemas = schemas;
  }

  /**
   * @param stateSchema the stateSchema to set
   */
  public void setStateSchema(final Schema stateSchema) {
    this.stateSchema = stateSchema;
  }

  /**
   * @param nodeID the nodeID to set
   */
  public void setNodeID(final Integer nodeID) {
    nodeId = nodeID;
  }
}