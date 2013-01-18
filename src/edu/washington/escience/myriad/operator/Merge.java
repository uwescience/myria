package edu.washington.escience.myriad.operator;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;

public class Merge extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private Operator child1, child2;
  private Schema outputSchema;

  public Merge() {
  }

  public Merge(final Schema outputSchema, final Operator child1, final Operator child2) {
    this.outputSchema = outputSchema;
    this.child1 = child1;
    this.child2 = child2;
  }

  // for Externalizable
  /*
   * @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException { child1 = (Operator)
   * in.readObject(); child2 = (Operator) in.readObject(); outputSchema = (Schema) in.readObject(); name = (String)
   * in.readObject(); }
   * 
   * // for Externalizable
   * 
   * @Override public void writeExternal(ObjectOutput out) throws IOException { out.writeObject(child1);
   * out.writeObject(child2); out.writeObject(outputSchema); out.writeObject(name); }
   */
  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
    TupleBatch tb;
    if (child1 != null && (tb = child1.next()) != null) {
      return tb;
    }
    if (child2 != null) {
      if ((tb = child2.next()) != null) {
        return tb;
      }
    }
    return null;
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child1, child2 };
  }

  @Override
  public Schema getSchema() {
    return outputSchema;
  }

  @Override
  public void init() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child1 = children[0];
    child2 = children[1];
  }

}
