package edu.washington.escience.myria.operator;

import java.util.Random;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;

public abstract class Sample extends BinaryOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** Number of tuples to sample from the right operator. */
  protected int sampleSize;

  /** Total number of tuples to expect from the right operator. */
  protected int populationSize;

  protected Random rand;

  /** Value to seed the random generator with. Null if no specified seed value. */
  protected Long randomSeed;

  /**
   * Instantiate a Sample operator using sampling info from the left operator
   * and actual samples from the right operator.
   *
   * @param left
   *          inputs a (WorkerID, PartitionSize, SampleSize) tuple.
   * @param right
   *          tuples that will be sampled from.
   * @param randomSeed
   *          value to seed the random generator with. null if no specified seed
   *          value
   */
  public Sample(final Operator left, final Operator right, Long randomSeed) {
    super(left, right);
    this.rand = new Random();
    this.randomSeed = randomSeed;
  }

  protected void extractSamplingInfo(TupleBatch tb) throws Exception {
    Preconditions.checkArgument(tb != null);

    int workerID;
    Type col0Type = tb.getSchema().getColumnType(0);
    if (col0Type == Type.INT_TYPE) {
      workerID = tb.getInt(0, 0);
    } else if (col0Type == Type.LONG_TYPE) {
      workerID = (int) tb.getLong(0, 0);
    } else {
      throw new DbException("workerID column must be of type INT or LONG");
    }
    Preconditions.checkState(workerID == getNodeID(),
        "Invalid WorkerID in samplingInfo. Expected %s, but received %s",
        getNodeID(), workerID);

    Type col1Type = tb.getSchema().getColumnType(1);
    if (col1Type == Type.INT_TYPE) {
      populationSize = tb.getInt(1, 0);
    } else if (col1Type == Type.LONG_TYPE) {
      populationSize = (int) tb.getLong(1, 0);
    } else {
      throw new DbException("populationSize column must be of type INT or LONG");
    }
    Preconditions.checkState(populationSize >= 0,
        "populationSize cannot be negative");

    Type col2Type = tb.getSchema().getColumnType(2);
    if (col2Type == Type.INT_TYPE) {
      sampleSize = tb.getInt(2, 0);
    } else if (col2Type == Type.LONG_TYPE) {
      sampleSize = (int) tb.getLong(2, 0);
    } else {
      throw new DbException("sampleSize column must be of type INT or LONG");
    }
    Preconditions.checkState(sampleSize >= 0, "sampleSize cannot be negative");
  }

  protected Random getRandom() {
    Random rand = new Random();
    if (randomSeed != null) {
      rand.setSeed(randomSeed);
    }
    return rand;
  }

  @Override
  public Schema generateSchema() {
    Operator right = getRight();
    if (right == null) {
      return null;
    }
    return right.getSchema();
  }

}
