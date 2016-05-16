package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.column.builder.IntColumnBuilder;
import edu.washington.escience.myria.column.builder.StringColumnBuilder;
import edu.washington.escience.myria.storage.TupleBatch;

public class RenameTest {

  /**
   * The original schema of the TupleBatch to be renamed.
   */
  private static final Schema originalSchema =
      Schema.of(
          ImmutableList.of(Type.STRING_TYPE, Type.INT_TYPE), ImmutableList.of("string", "int"));

  /**
   * The original TupleBatch.
   */
  private static TupleBatch originalTuples;

  /**
   * The original data;
   */
  private static List<Column<?>> originalData;

  /**
   * The number of tuples to build.
   */
  private static final int TUPLES_TO_BUILD = 100;

  @BeforeClass
  public static void setup() {
    List<ColumnBuilder<?>> dataBuilder = ColumnFactory.allocateColumns(originalSchema);
    for (int i = 0; i < TUPLES_TO_BUILD; ++i) {
      ((StringColumnBuilder) dataBuilder.get(0)).appendString("val" + i);
      ((IntColumnBuilder) dataBuilder.get(1)).appendInt(i);
    }
    ImmutableList.Builder<Column<?>> dataColumnBuilder = new ImmutableList.Builder<Column<?>>();
    for (ColumnBuilder<?> builder : dataBuilder) {
      dataColumnBuilder.add(builder.build());
    }
    originalData = dataColumnBuilder.build();
    originalTuples = new TupleBatch(originalSchema, originalData);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testTupleBatch() {
    /* Sanity check originalTuples */
    assertEquals(originalSchema, originalTuples.getSchema());
    assertEquals(TUPLES_TO_BUILD, originalTuples.numTuples());

    /* Do the renaming using TupleBatch.rename() */
    final List<String> newNames = ImmutableList.of("stringNew", "intNew");
    final TupleBatch renamed = originalTuples.rename(newNames);

    /* Verify the renamed TB's schema */
    assertEquals(originalSchema.getColumnTypes(), renamed.getSchema().getColumnTypes());
    assertEquals(newNames, renamed.getSchema().getColumnNames());

    /* Verify the renamed TB's size */
    assertEquals(originalTuples.numColumns(), renamed.numColumns());
    assertEquals(originalTuples.numTuples(), renamed.numTuples());

    /* Verify the renamed TB's data */
    for (int row = 0; row < renamed.numTuples(); ++row) {
      for (int column = 0; column < renamed.numColumns(); ++column) {
        assertEquals(originalTuples.getObject(column, row), renamed.getObject(column, row));
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTupleBatchTooManyColumns() {
    /* Sanity check originalTuples */
    assertEquals(originalSchema, originalTuples.getSchema());
    assertEquals(TUPLES_TO_BUILD, originalTuples.numTuples());

    /* Do the renaming using TupleBatch.rename() */
    final List<String> newNames = ImmutableList.of("stringNew", "intNew", "badExtraColumn");
    originalTuples.rename(newNames);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTupleBatchTooFewColumns() {
    /* Sanity check originalTuples */
    assertEquals(originalSchema, originalTuples.getSchema());
    assertEquals(TUPLES_TO_BUILD, originalTuples.numTuples());

    /* Do the renaming using TupleBatch.rename() */
    final List<String> newNames = ImmutableList.of("onlyOneColumn");
    originalTuples.rename(newNames);
  }

  @Test(expected = NullPointerException.class)
  public void testTupleBatchNull() {
    /* Sanity check originalTuples */
    assertEquals(originalSchema, originalTuples.getSchema());
    assertEquals(TUPLES_TO_BUILD, originalTuples.numTuples());

    /* Do the renaming using TupleBatch.rename() */
    originalTuples.rename(null);
  }
}
