/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;

/**
 *
 */
public class CreateIndexEncoding extends MyriaApiEncoding {
  @Required public RelationKey relationKey;
  @Required public Schema schema;
  @Required public List<IndexRef> indexes;
}
