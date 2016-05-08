/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.util.Set;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.Required;
import edu.washington.escience.myria.io.UriSource;

/**
 * Table Description Encoding
 */
public class TableDescriptionEncoding {
  @Required
  public RelationKey relationkey;
  @Required
  public String type;
  @Required
  public UriSource source;
  @Required
  public Schema schema;
  @Required
  public Character delimiter;
  @Required
  public Set<Integer> keys;

}
