package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.nio.file.Paths;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestEnvVars;

public class LeapFrogJoinTest {
  @Test
  public void testLeapFrogJoinOnMultipleTBInBuffer() throws DbException {
    final Schema schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id2", "name2"));
    TupleBatchBuffer leftTbb = new TupleBatchBuffer(schema);
    TupleBatchBuffer rightTbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < 2; ++i) {
      leftTbb.putLong(0, 0);
      leftTbb.putString(1, "hello world");
    }

    for (int i = 0; i < TupleBatch.BATCH_SIZE + 1; ++i) {
      rightTbb.putLong(0, 0);
      rightTbb.putString(1, "hello world");
    }

    TupleSource[] children = new TupleSource[2];
    children[0] = new TupleSource(leftTbb);
    children[1] = new TupleSource(rightTbb);

    final ImmutableList<String> outputColumnNames =
        ImmutableList.of("id1", "name1", "id2", "name2");
    final Schema outputSchema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE, Type.STRING_TYPE),
            outputColumnNames);

    int[][][] fieldMap = new int[][][] {{{0, 0}, {1, 0}}};
    int[][] outputMap = new int[][] {{0, 0}, {0, 1}, {1, 0}, {1, 1}};
    NAryOperator join = new LeapFrogJoin(children, fieldMap, outputMap, outputColumnNames, null);
    join.open(null);
    TupleBatch tb;
    TupleBatchBuffer batches = new TupleBatchBuffer(outputSchema);
    while (!join.eos()) {
      tb = join.nextReady();
      if (tb != null) {
        batches.appendTB(tb);
      }
    }
    join.close();
    assertEquals(2 * (TupleBatch.BATCH_SIZE + 1), batches.numTuples());
  }

  @Test
  public void testLeapFrogUsingTwitterTriangularJoin() throws DbException {
    final Schema r_schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("r_x", "r_y"));
    final Schema s_schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("s_y", "s_z"));
    final Schema t_schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("t_z", "t_x"));
    /* read data from files. */
    final String realFilename = Paths.get("testdata", "twitter", "TwitterK.csv").toString();
    FileScan fileScanR = new FileScan(realFilename, r_schema);
    FileScan fileScanS = new FileScan(realFilename, s_schema);
    FileScan fileScanT = new FileScan(realFilename, t_schema);
    /* order the tables. */
    InMemoryOrderBy orderR =
        new InMemoryOrderBy(fileScanR, new int[] {0, 1}, new boolean[] {true, true});
    InMemoryOrderBy orderS =
        new InMemoryOrderBy(fileScanS, new int[] {0, 1}, new boolean[] {true, true});
    InMemoryOrderBy orderT =
        new InMemoryOrderBy(fileScanT, new int[] {1, 0}, new boolean[] {true, true});
    /* leapfrog join. */
    int[][][] fieldMap = new int[][][] {{{0, 0}, {2, 1}}, {{0, 1}, {1, 0}}, {{1, 1}, {2, 0}}};
    int[][] outputMap = new int[][] {{0, 0}, {0, 1}, {1, 1}};
    final ImmutableList<String> outputColumnNames = ImmutableList.of("x", "y", "z");
    final Schema outputSchema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE), outputColumnNames);
    LeapFrogJoin join =
        new LeapFrogJoin(
            new Operator[] {orderR, orderS, orderT},
            fieldMap,
            outputMap,
            outputColumnNames,
            new boolean[] {true, true, true});
    join.open(null);
    TupleBatch tb;
    TupleBatchBuffer batches = new TupleBatchBuffer(outputSchema);
    while (!join.eos()) {
      tb = join.nextReady();
      if (tb != null) {
        batches.appendTB(tb);
      }
    }
    join.close();
    assertEquals(16826, batches.numTuples());
  }

  @Test
  public void binaryJoinWithMoreColumns() throws DbException {
    /* Query: Result(x,y,z) :- R(x,y),T(x,y,z). */
    final Schema r_schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("r_x", "r_y"));
    final Schema t_schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE),
            ImmutableList.of("t_x", "t_z", "t_y"));
    /* read data from files. */
    final String r_path = Paths.get("testdata", "multiwayjoin", "R.csv").toString();
    final String t_path = Paths.get("testdata", "multiwayjoin", "T.csv").toString();
    FileScan fileScanR = new FileScan(r_path, r_schema);
    FileScan fileScanT = new FileScan(t_path, t_schema);
    /* order the tables. */
    InMemoryOrderBy orderR =
        new InMemoryOrderBy(fileScanR, new int[] {0, 1}, new boolean[] {true, true});
    InMemoryOrderBy orderT =
        new InMemoryOrderBy(fileScanT, new int[] {0, 1}, new boolean[] {true, true});
    /* leapfrog join. */
    int[][][] fieldMap = new int[][][] {{{0, 0}, {1, 0}}, {{0, 1}, {1, 2}}};
    int[][] outputMap = new int[][] {{0, 0}, {0, 1}};
    final ImmutableList<String> outputColumnNames = ImmutableList.of("x", "y");
    final Schema outputSchema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), outputColumnNames);
    LeapFrogJoin join =
        new LeapFrogJoin(
            new Operator[] {orderR, orderT},
            fieldMap,
            outputMap,
            outputColumnNames,
            new boolean[] {false, false});
    join.open(null);
    TupleBatch tb;
    TupleBatchBuffer batches = new TupleBatchBuffer(outputSchema);
    while (!join.eos()) {
      tb = join.nextReady();
      if (tb != null) {
        batches.appendTB(tb);
      }
    }
    join.close();
    assertEquals(0, batches.numTuples());
  }

  @Test
  public void strangeTriangle() throws DbException {
    /* Query: Result(x,y,z) :- R(x,y),S(y,z),T(x,y,z). */
    final Schema r_schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("r_x", "r_y"));
    final Schema s_schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("s_y", "s_z"));
    final Schema t_schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE),
            ImmutableList.of("t_x", "t_y", "t_z"));
    /* read data from files. */
    final String r_path = Paths.get("testdata", "multiwayjoin", "R.csv").toString();
    final String s_path = Paths.get("testdata", "multiwayjoin", "S.csv").toString();
    final String t_path = Paths.get("testdata", "multiwayjoin", "T.csv").toString();
    FileScan fileScanR = new FileScan(r_path, r_schema);
    FileScan fileScanS = new FileScan(s_path, s_schema);
    FileScan fileScanT = new FileScan(t_path, t_schema);
    /* order the tables. */
    InMemoryOrderBy orderR =
        new InMemoryOrderBy(fileScanR, new int[] {0, 1}, new boolean[] {true, true});
    InMemoryOrderBy orderS =
        new InMemoryOrderBy(fileScanS, new int[] {0, 1}, new boolean[] {true, true});
    InMemoryOrderBy orderT =
        new InMemoryOrderBy(fileScanT, new int[] {0, 1, 2}, new boolean[] {true, true, true});
    /* leapfrog join. */
    int[][][] fieldMap =
        new int[][][] {{{0, 0}, {2, 0}}, {{0, 1}, {1, 0}, {2, 1}}, {{1, 1}, {2, 2}}};
    int[][] outputMap = new int[][] {{0, 0}, {0, 1}, {1, 1}};
    final ImmutableList<String> outputColumnNames = ImmutableList.of("x", "y", "z");
    final Schema outputSchema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE), outputColumnNames);
    LeapFrogJoin join =
        new LeapFrogJoin(
            new Operator[] {orderR, orderS, orderT},
            fieldMap,
            outputMap,
            outputColumnNames,
            new boolean[] {true, true, false});
    join.open(null);
    TupleBatch tb;
    TupleBatchBuffer batches = new TupleBatchBuffer(outputSchema);
    while (!join.eos()) {
      tb = join.nextReady();
      if (tb != null) {
        batches.appendTB(tb);
      }
    }
    join.close();
    assertEquals(8, batches.numTuples());
  }

  @Test
  public void strangeRectangle() throws DbException {
    /* Rectangle(x,y,z,p) :- R(x,y),S(y,z),T(z,p),K(p,x),M(x,y,z). */
    final Schema r_schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("r_x", "r_y"));
    final Schema s_schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("s_y", "s_z"));
    final Schema t_schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("t_z", "t_p"));
    final Schema k_schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("k_p", "k_x"));
    final Schema m_schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE),
            ImmutableList.of("m_x", "m_y", "m_z"));
    /* read data from files. */
    final String r_path = Paths.get("testdata", "multiwayjoin", "rectangles.csv").toString();
    final String m_path = Paths.get("testdata", "multiwayjoin", "rec_2_hop.csv").toString();
    FileScan fileScanR = new FileScan(r_path, r_schema);
    FileScan fileScanS = new FileScan(r_path, s_schema);
    FileScan fileScanT = new FileScan(r_path, t_schema);
    FileScan fileScanK = new FileScan(r_path, k_schema);
    FileScan fileScanM = new FileScan(m_path, m_schema);
    /* order the tables. */
    InMemoryOrderBy orderR =
        new InMemoryOrderBy(fileScanR, new int[] {0, 1}, new boolean[] {true, true});
    InMemoryOrderBy orderS =
        new InMemoryOrderBy(fileScanS, new int[] {0, 1}, new boolean[] {true, true});
    InMemoryOrderBy orderT =
        new InMemoryOrderBy(fileScanT, new int[] {0, 1}, new boolean[] {true, true});
    InMemoryOrderBy orderK =
        new InMemoryOrderBy(fileScanK, new int[] {1, 0}, new boolean[] {true, true});
    InMemoryOrderBy orderM =
        new InMemoryOrderBy(fileScanM, new int[] {0, 1, 2}, new boolean[] {true, true, true});

    /* leapfrog join, Rectangle(x,y,z,p) :- R(x,y),S(y,z),T(z,p),K(p,x),M(x,y,z). */
    int[][][] fieldMap =
        new int[][][] {
          {{0, 0}, {3, 1}, {4, 0}},
          {{0, 1}, {1, 0}, {4, 1}},
          {{1, 1}, {2, 0}, {4, 2}},
          {{2, 1}, {3, 0}}
        };
    int[][] outputMap = new int[][] {{0, 0}, {0, 1}, {1, 1}, {2, 1}};
    final ImmutableList<String> outputColumnNames = ImmutableList.of("x", "y", "z", "p");
    final Schema outputSchema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE),
            outputColumnNames);
    LeapFrogJoin join =
        new LeapFrogJoin(
            new Operator[] {orderR, orderS, orderT, orderK, orderM},
            fieldMap,
            outputMap,
            outputColumnNames,
            new boolean[] {false, false, false, false, true});
    join.open(TestEnvVars.get());
    TupleBatch tb;
    TupleBatchBuffer batches = new TupleBatchBuffer(outputSchema);
    while (!join.eos()) {
      tb = join.nextReady();
      if (tb != null) {
        batches.appendTB(tb);
      }
    }
    join.close();
    assertEquals(4, batches.numTuples());
  }

  @Test
  public void tridentQuery() throws DbException {
    /* Query: Result(x,y,z) :- R(x),S(x),T(y,x). */
    final Schema r_schema =
        new Schema(ImmutableList.of(Type.INT_TYPE), ImmutableList.of("film_id_a"));
    final Schema s_schema =
        new Schema(ImmutableList.of(Type.INT_TYPE), ImmutableList.of("film_id_b"));
    final Schema t_schema =
        new Schema(
            ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE),
            ImmutableList.of("film_id", "perform_id"));
    /* read data from files, note */
    final String r_path = Paths.get("testdata", "multiwayjoin", "fa.csv").toString();
    final String s_path = Paths.get("testdata", "multiwayjoin", "fb.csv").toString();
    final String t_path = Paths.get("testdata", "multiwayjoin", "pf.csv").toString();
    FileScan fileScanR = new FileScan(r_path, r_schema);
    FileScan fileScanS = new FileScan(s_path, s_schema);
    FileScan fileScanT = new FileScan(t_path, t_schema);
    /* order the tables. */
    InMemoryOrderBy orderR = new InMemoryOrderBy(fileScanR, new int[] {0}, new boolean[] {true});
    InMemoryOrderBy orderS = new InMemoryOrderBy(fileScanS, new int[] {0}, new boolean[] {true});
    InMemoryOrderBy orderT = new InMemoryOrderBy(fileScanT, new int[] {1}, new boolean[] {true});
    /* leapfrog join. */
    int[][][] fieldMap = new int[][][] {{{0, 0}, {1, 0}, {2, 1}}};
    int[][] outputMap = new int[][] {{0, 0}, {2, 0}};
    final ImmutableList<String> outputColumnNames = ImmutableList.of("x", "y");
    final Schema outputSchema =
        new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE), outputColumnNames);
    LeapFrogJoin join =
        new LeapFrogJoin(
            new Operator[] {orderR, orderS, orderT},
            fieldMap,
            outputMap,
            outputColumnNames,
            new boolean[] {false, false, false});
    join.open(null);
    TupleBatch tb;
    TupleBatchBuffer batches = new TupleBatchBuffer(outputSchema);
    while (!join.eos()) {
      tb = join.nextReady();
      if (tb != null) {
        batches.appendTB(tb);
      }
    }
    join.close();
    assertEquals(2, batches.numTuples());
  }

  @Test
  public void intersect3tables() throws DbException {
    /* I(x) :- A(x),B(x),C(x). */
    final Schema a_schema = new Schema(ImmutableList.of(Type.INT_TYPE), ImmutableList.of("a_x"));
    final Schema b_schema = new Schema(ImmutableList.of(Type.INT_TYPE), ImmutableList.of("b_x"));
    final Schema c_schema = new Schema(ImmutableList.of(Type.INT_TYPE), ImmutableList.of("c_x"));
    /* read data from files, note */
    final String a_path = Paths.get("testdata", "multiwayjoin", "a.csv").toString();
    final String b_path = Paths.get("testdata", "multiwayjoin", "b.csv").toString();
    final String c_path = Paths.get("testdata", "multiwayjoin", "c.csv").toString();
    FileScan fileScanA = new FileScan(a_path, a_schema);
    FileScan fileScanB = new FileScan(b_path, b_schema);
    FileScan fileScanC = new FileScan(c_path, c_schema);
    /* order the tables. */
    InMemoryOrderBy orderA = new InMemoryOrderBy(fileScanA, new int[] {0}, new boolean[] {true});
    InMemoryOrderBy orderB = new InMemoryOrderBy(fileScanB, new int[] {0}, new boolean[] {true});
    InMemoryOrderBy orderC = new InMemoryOrderBy(fileScanC, new int[] {0}, new boolean[] {true});
    /* leapfrog join. */
    int[][][] fieldMap = new int[][][] {{{0, 0}, {1, 0}, {2, 0}}};
    int[][] outputMap = new int[][] {{0, 0}};
    final ImmutableList<String> outputColumnNames = ImmutableList.of("x");
    final Schema outputSchema = new Schema(ImmutableList.of(Type.INT_TYPE), outputColumnNames);
    LeapFrogJoin join =
        new LeapFrogJoin(
            new Operator[] {orderA, orderB, orderC},
            fieldMap,
            outputMap,
            outputColumnNames,
            new boolean[] {false, false, true});
    join.open(null);
    TupleBatch tb;
    TupleBatchBuffer batches = new TupleBatchBuffer(outputSchema);
    while (!join.eos()) {
      tb = join.nextReady();
      if (tb != null) {
        batches.appendTB(tb);
      }
    }
    join.close();
    assertEquals(2, batches.numTuples());
  }

  @Test
  public void outputFreeVariable() throws DbException {
    /* Result(x):- o(k,x), p(x,y), q(y,z) */
    final Schema o_schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("o_k", "o_x"));
    final Schema p_schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("p_x", "p_y"));
    final Schema q_schema =
        new Schema(
            ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("q_y", "q_z"));
    /* order the tables. */
    final String o_path = Paths.get("testdata", "multiwayjoin", "o.csv").toString();
    final String p_path = Paths.get("testdata", "multiwayjoin", "p.csv").toString();
    final String q_path = Paths.get("testdata", "multiwayjoin", "q.csv").toString();
    FileScan fileScanO = new FileScan(o_path, o_schema);
    FileScan fileScanP = new FileScan(p_path, p_schema);
    FileScan fileScanQ = new FileScan(q_path, q_schema);
    InMemoryOrderBy orderO = new InMemoryOrderBy(fileScanO, new int[] {1}, new boolean[] {true});
    InMemoryOrderBy orderP =
        new InMemoryOrderBy(fileScanP, new int[] {0, 1}, new boolean[] {true, true});
    InMemoryOrderBy orderQ = new InMemoryOrderBy(fileScanQ, new int[] {0}, new boolean[] {true});
    /* leapfrog join. */
    int[][][] fieldMap = new int[][][] {{{0, 1}, {1, 0}}, {{1, 1}, {2, 0}}};
    int[][] outputMap = new int[][] {{0, 0}};
    final ImmutableList<String> outputColumnNames = ImmutableList.of("k");
    final Schema outputSchema = new Schema(ImmutableList.of(Type.LONG_TYPE), outputColumnNames);
    LeapFrogJoin join =
        new LeapFrogJoin(
            new Operator[] {orderO, orderP, orderQ},
            fieldMap,
            outputMap,
            outputColumnNames,
            new boolean[] {false, false, false});
    join.open(TestEnvVars.get());
    TupleBatch tb;
    TupleBatchBuffer batches = new TupleBatchBuffer(outputSchema);
    while (!join.eos()) {
      tb = join.nextReady();
      if (tb != null) {
        batches.appendTB(tb);
      }
    }
    join.close();
    assertEquals(9, batches.numTuples());
  }
}
