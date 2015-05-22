package edu.washington.escience.myria.column;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.MyriaMatrix;
import edu.washington.escience.myria.Type;

/**
 * A column of Date values. Now Matrix values
 * 
 */
public final class MyriaMatrixColumn extends Column<MyriaMatrix> {
	/** Required for Java serialization. */
	private static final long serialVersionUID = 1;
	/** Internal representation of the column data. */
	private final MyriaMatrix[] data;
	/** The number of existing rows in this column. */
	private final int position;

	/**
	 * Constructs a new column.
	 * 
	 * @param data
	 *            the data
	 * @param numData
	 *            number of tuples.
	 * */
	public MyriaMatrixColumn(final MyriaMatrix[] data, final int numData) {
		this.data = data;
		position = numData;
	}

	@Override
	public MyriaMatrix getObject(final int row) {
		return getMyriaMatrix(row);
	}

	/**
	 * Returns the element at the specified row in this column.
	 * 
	 * @param row
	 *            row of element to return.
	 * @return the element at the specified row in this column.
	 */
	@Override
	public MyriaMatrix getMyriaMatrix(final int row) {
		Preconditions.checkElementIndex(row, position);
		return data[row];
	}

	@Override
	public Type getType() {
		return Type.MYRIAMATRIX_TYPE;
	}

	@Override
	public int size() {
		return position;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append(size()).append(" elements: [");
		for (int i = 0; i < size(); ++i) {
			if (i > 0) {
				sb.append(", ");
			}
			sb.append(data[i]);
		}
		sb.append(']');
		return sb.toString();
	}
}