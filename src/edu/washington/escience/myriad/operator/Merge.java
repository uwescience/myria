package edu.washington.escience.myriad.operator;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;

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

  @Override
  protected _TupleBatch fetchNext() throws DbException {
    _TupleBatch tb;
    System.out.println("merge fetch next");
    if (child1 != null && (tb = child1.next()) != null) {
      System.out.println(" child1 fetched");
      return tb;
    }
    if (child2 != null) {
      System.out.println("entering child2 next");
      if ((tb = child2.next()) != null) {
        System.out.println(tb + " child2");
        return tb;
      }
      System.out.println("child2 null");
    }
    System.out.println("returning null");
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
  public _TupleBatch fetchNextReady() throws DbException {
    return null;
  }

}
