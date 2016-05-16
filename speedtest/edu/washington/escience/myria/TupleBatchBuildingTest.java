package edu.washington.escience.myria;

import java.util.Arrays;

import org.junit.Test;

import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.LongColumnBuilder;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.DateTimeUtils;
import edu.washington.escience.myria.util.TestUtils;

public class TupleBatchBuildingTest {

  /**
   * 1: 0.125 0.317
   *
   * 10: 0.255 0.413
   *
   * 100: 0.579 0.135
   *
   * 1000: 0.304 0.224
   *
   * 10000: 2.619 1.115
   *
   * 100000: 22.299 8.408
   *
   * 1000000: 222.134 82.492
   * */
  final Schema testSchema =
      Schema.of(
          Arrays.asList(new Type[] {Type.LONG_TYPE, Type.LONG_TYPE}),
          Arrays.asList(new String[] {"1", "2"}));

  final long testSize = 1000000l;

  @Test
  public void buildImmutableTB() {

    long[] icb1Source = TestUtils.randomLong(10, Integer.MAX_VALUE, TupleBatch.BATCH_SIZE);
    long[] icb2Source = TestUtils.randomLong(10, Integer.MAX_VALUE, TupleBatch.BATCH_SIZE);
    Column<?>[] lcs = new Column<?>[2];
    long totalCount = 0;

    long start = System.nanoTime();
    for (long i = 0; i < testSize; i++) {
      LongColumnBuilder icb1 = new LongColumnBuilder();
      LongColumnBuilder icb2 = new LongColumnBuilder();
      for (int j = 0; j < TupleBatch.BATCH_SIZE; j++) {
        icb1.appendLong(icb1Source[j]);
        icb2.appendLong(icb2Source[j]);
      }
      lcs[0] = icb1.build();
      lcs[1] = icb2.build();
      totalCount += new TupleBatch(testSchema, Arrays.asList(lcs)).numTuples();
    }
    long end = System.nanoTime();
    System.out.println(totalCount);
    System.out.println(DateTimeUtils.nanoElapseToHumanReadable(end - start));
  }

  @Test
  public void buildMutableTB() {

    LongColumnBuilder icb1 = new LongColumnBuilder();
    LongColumnBuilder icb2 = new LongColumnBuilder();
    long[] icb1Source = TestUtils.randomLong(10, Integer.MAX_VALUE, TupleBatch.BATCH_SIZE);
    long[] icb2Source = TestUtils.randomLong(10, Integer.MAX_VALUE, TupleBatch.BATCH_SIZE);
    long totalCount = 0;

    icb1.expandAll();
    icb2.expandAll();

    long start = System.nanoTime();
    for (long i = 0; i < testSize; i++) {
      for (int j = 0; j < TupleBatch.BATCH_SIZE; j++) {
        icb1.replaceLong(icb1Source[j], j);
        icb2.replaceLong(icb2Source[j], j);
      }
      totalCount += icb1.size();
    }
    long end = System.nanoTime();
    System.out.println(totalCount);
    System.out.println(DateTimeUtils.nanoElapseToHumanReadable(end - start));
  }
}
