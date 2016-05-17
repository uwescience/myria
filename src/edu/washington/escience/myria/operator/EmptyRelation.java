package edu.washington.escience.myria.operator;

import java.util.Objects;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * An empty relation with the given Schema.
 *
 */
public final class EmptyRelation extends LeafOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The Schema of the tuples output by this operator. */
  private final Schema schema;

  /**
   * Constructs an empty relation with the specified schema.
   *
   * @param schema the schema of the relation.
   */
  private EmptyRelation(final Schema schema) {
    this.schema = Objects.requireNonNull(schema, "Empty relations must be created with a Schema");
  }

  /**
   * Constructs an empty relation with the specified schema.
   *
   * @param schema the schema of the relation.
   * @return an empty relation with the specified schema.
   */
  public static EmptyRelation of(final Schema schema) {
    return new EmptyRelation(schema);
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    return null;
  }

  @Override
  protected Schema generateSchema() {
    return schema;
  }
}
