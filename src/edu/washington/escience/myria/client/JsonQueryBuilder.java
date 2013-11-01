package edu.washington.escience.myria.client;

import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.operator.IDBInput;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.parallel.EOSController;

/**
 * Json query builders interface.
 * */
public interface JsonQueryBuilder {

  /**
   * Send data back to user client.
   * 
   * TODO
   * 
   * @return builder.
   * */
  JsonQueryBuilder export();

  /**
   * Begin iterate, denoting an {@link IDBInput}. All iterations built are currently governed by a single
   * {@link EOSController}.
   * 
   * @return builder.
   * */
  JsonQueryBuilder beginIterate();

  /**
   * {@link SymmetricHashJoin}.
   * 
   * @param iterateBeginner iterate the stream of current operator back into the iterate beginner.
   * @return builder.
   * */
  JsonQueryBuilder endIterate(final JsonQueryBuilder iterateBeginner);

  /**
   * 
   * Build the Json query plan. If the current operator is not a {@link RootOperator}, a {@link SinkRoot} is
   * automatically added.
   * 
   * @return The Json string of the query plan.
   * */
  String buildJson();

  /**
   * 
   * Build the Java encoding of the query plan. If the current operator is not a {@link RootOperator}, a
   * {@link SinkRoot} is automatically added.
   * 
   * @return The Java encoding of the query plan.
   * */
  QueryEncoding build();

  /**
   * Set the name of the operator which currently is the root of this json building block.
   * 
   * @param name the name
   * @return a new building block with the setName operation recorded.
   * @throws IllegalArgumentException if the name is duplicated with other operators.
   * */
  JsonQueryBuilder setName(final String name);
}
