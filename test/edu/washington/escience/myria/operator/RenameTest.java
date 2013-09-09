package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.ColumnBuilder;
import edu.washington.escience.myria.column.ColumnFactory;

public class RenameTest {

  /**
   * The original schema of the TupleBatch to be renamed.
   */
  private static final Schema originalSchema = Schema.of(ImmutableList.of(Type.STRING_TYPE, Type.INT_TYPE),
      ImmutableList.of("string", "int"));

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

  @SuppressWarnings("unchecked")
  @BeforeClass
  public static void setup() {
    List<ColumnBuilder<?>> dataBuilder = ColumnFactory.allocateColumns(originalSchema);
    for (int i = 0; i < TUPLES_TO_BUILD; ++i) {
      ((ColumnBuilder<String>) dataBuilder.get(0)).append("val" + i);
      ((ColumnBuilder<Integer>) dataBuilder.get(1)).append(i);
    }
    ImmutableList.Builder<Column<?>> dataColumnBuilder = new ImmutableList.Builder<Column<?>>();
    for (ColumnBuilder<?> builder : dataBuilder) {
      dataColumnBuilder.add(builder.build());
    }
    originalData = dataColumnBuilder.build();
    originalTuples = new TupleBatch(originalSchema, originalData);
  }

  @Test
  public void testTupleBatch() {
    /* Sanity check originalTuples */
    assertTrue(originalTuples.getSchema().equals(originalSchema));
    assertTrue(originalTuples.numTuples() == TUPLES_TO_BUILD);

    /* Do the renaming using TupleBatch.rename() */
    final List<String> newNames = ImmutableList.of("stringNew", "intNew");
    final TupleBatch renamed = originalTuples.rename(newNames);

    /* Verify the renamed TB's schema */
    assertTrue(renamed.getSchema().getColumnTypes().equals(originalSchema.getColumnTypes()));
    assertTrue(renamed.getSchema().getColumnNames().equals(newNames));

    /* Verify the renamed TB's size */
    assertTrue(renamed.numColumns() == originalTuples.numColumns());
    assertTrue(renamed.numTuples() == originalTuples.numTuples());

    /* Verify the renamed TB's data */
    for (int row = 0; row < renamed.numTuples(); ++row) {
      for (int column = 0; column < renamed.numColumns(); ++column) {
        assertTrue(renamed.getObject(column, row).equals(originalTuples.getObject(column, row)));
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTupleBatchTooManyColumns() {
    /* Sanity check originalTuples */
    assertTrue(originalTuples.getSchema().equals(originalSchema));
    assertTrue(originalTuples.numTuples() == TUPLES_TO_BUILD);

    /* Do the renaming using TupleBatch.rename() */
    final List<String> newNames = ImmutableList.of("stringNew", "intNew", "badExtraColumn");
    originalTuples.rename(newNames);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTupleBatchTooFewColumns() {
    /* Sanity check originalTuples */
    assertTrue(originalTuples.getSchema().equals(originalSchema));
    assertTrue(originalTuples.numTuples() == TUPLES_TO_BUILD);

    /* Do the renaming using TupleBatch.rename() */
    final List<String> newNames = ImmutableList.of("onlyOneColumn");
    originalTuples.rename(newNames);
  }

  @Test(expected = NullPointerException.class)
  public void testTupleBatchNull() {
    /* Sanity check originalTuples */
    assertTrue(originalTuples.getSchema().equals(originalSchema));
    assertTrue(originalTuples.numTuples() == TUPLES_TO_BUILD);

    /* Do the renaming using TupleBatch.rename() */
    originalTuples.rename(null);
  }

  @Test
  public void testRenameOperator() throws DbException {
    /* Sanity check originalTuples */
    assertTrue(originalTuples.getSchema().equals(originalSchema));
    assertTrue(originalTuples.numTuples() == TUPLES_TO_BUILD);

    /* Do the renaming using the Rename operator */
    final List<String> newNames = ImmutableList.of("stringNew", "intNew");

    TupleSource source = new TupleSource(ImmutableList.of(originalTuples, originalTuples));
    Rename rename = new Rename(source, newNames);
    rename.open(null);
    while (!rename.eos()) {
      TupleBatch renamed = rename.nextReady();
      if (renamed == null) {
        continue;
      }
      /* Verify the renamed TB's schema */
      assertTrue(renamed.getSchema().getColumnTypes().equals(originalSchema.getColumnTypes()));
      assertTrue(renamed.getSchema().getColumnNames().equals(newNames));

      /* Verify the renamed TB's size */
      assertTrue(renamed.numColumns() == originalTuples.numColumns());
      assertTrue(renamed.numTuples() == originalTuples.numTuples());

      /* Verify the renamed TB's data */
      for (int row = 0; row < renamed.numTuples(); ++row) {
        for (int column = 0; column < renamed.numColumns(); ++column) {
          assertTrue(renamed.getObject(column, row).equals(originalTuples.getObject(column, row)));
        }
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRenameOperatorTooManyColumns() throws DbException {
    /* Sanity check originalTuples */
    assertTrue(originalTuples.getSchema().equals(originalSchema));
    assertTrue(originalTuples.numTuples() == TUPLES_TO_BUILD);

    /* Do the renaming using the Rename operator */
    final List<String> newNames = ImmutableList.of("stringNew", "intNew", "extraColumn");

    TupleSource source = new TupleSource(ImmutableList.of(originalTuples, originalTuples));
    new Rename(source, newNames);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRenameOperatorTooFewColumns() throws DbException {
    /* Sanity check originalTuples */
    assertTrue(originalTuples.getSchema().equals(originalSchema));
    assertTrue(originalTuples.numTuples() == TUPLES_TO_BUILD);

    /* Do the renaming using the Rename operator */
    final List<String> newNames = ImmutableList.of("onlyOneColumn");

    TupleSource source = new TupleSource(ImmutableList.of(originalTuples, originalTuples));
    new Rename(source, newNames);
  }

  @Test(expected = NullPointerException.class)
  public void testRenameOperatorNull() throws DbException {
    /* Sanity check originalTuples */
    assertTrue(originalTuples.getSchema().equals(originalSchema));
    assertTrue(originalTuples.numTuples() == TUPLES_TO_BUILD);

    TupleSource source = new TupleSource(ImmutableList.of(originalTuples, originalTuples));
    new Rename(source, null);
  }

  @Test
  public void testRenameOperatorDelayedChild() throws DbException {
    /* Sanity check originalTuples */
    assertTrue(originalTuples.getSchema().equals(originalSchema));
    assertTrue(originalTuples.numTuples() == TUPLES_TO_BUILD);

    /* Do the renaming using the Rename operator */
    final List<String> newNames = ImmutableList.of("stringNew", "intNew");

    TupleSource source = new TupleSource(ImmutableList.of(originalTuples, originalTuples));
    Rename rename = new Rename(newNames);
    rename.setChild(source);
    rename.open(null);
    while (!rename.eos()) {
      TupleBatch renamed = rename.nextReady();
      if (renamed == null) {
        continue;
      }
      /* Verify the renamed TB's schema */
      assertTrue(renamed.getSchema().getColumnTypes().equals(originalSchema.getColumnTypes()));
      assertTrue(renamed.getSchema().getColumnNames().equals(newNames));

      /* Verify the renamed TB's size */
      assertTrue(renamed.numColumns() == originalTuples.numColumns());
      assertTrue(renamed.numTuples() == originalTuples.numTuples());

      /* Verify the renamed TB's data */
      for (int row = 0; row < renamed.numTuples(); ++row) {
        for (int column = 0; column < renamed.numColumns(); ++column) {
          assertTrue(renamed.getObject(column, row).equals(originalTuples.getObject(column, row)));
        }
      }
    }
  }

  /* TODO(dhalperi) make this test pass @Test(expected = IllegalArgumentException.class) */
  public void testRenameOperatorDelayedChildTooManyColumns() throws DbException {
    /* Sanity check originalTuples */
    assertTrue(originalTuples.getSchema().equals(originalSchema));
    assertTrue(originalTuples.numTuples() == TUPLES_TO_BUILD);

    /* Do the renaming using the Rename operator */
    final List<String> newNames = ImmutableList.of("stringNew", "intNew", "extraColumn");

    TupleSource source = new TupleSource(ImmutableList.of(originalTuples, originalTuples));
    Rename rename = new Rename(newNames);
    rename.setChild(source);
  }

  /* TODO(dhalperi) make this test pass @Test(expected = IllegalArgumentException.class) */
  public void testRenameOperatorDelayedChildTooFewColumns() throws DbException {
    /* Sanity check originalTuples */
    assertTrue(originalTuples.getSchema().equals(originalSchema));
    assertTrue(originalTuples.numTuples() == TUPLES_TO_BUILD);

    /* Do the renaming using the Rename operator */
    final List<String> newNames = ImmutableList.of("onlyOneColumn");

    TupleSource source = new TupleSource(ImmutableList.of(originalTuples, originalTuples));
    Rename rename = new Rename(newNames);
    rename.setChild(source);
  }

  @Test(expected = NullPointerException.class)
  public void testRenameOperatorDelayedChildNull() throws DbException {
    /* Sanity check originalTuples */
    assertTrue(originalTuples.getSchema().equals(originalSchema));
    assertTrue(originalTuples.numTuples() == TUPLES_TO_BUILD);

    TupleSource source = new TupleSource(ImmutableList.of(originalTuples, originalTuples));
    Rename rename = new Rename(null);
    rename.setChild(source);
  }
}
