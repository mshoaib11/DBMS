package dbs_project.query.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import dbs_project.storage.RowCursor;
import dbs_project.storage.RowMetaData;
import dbs_project.storage.impl.ColumnImpl;

/**
 * Created by Xedos2308 on 12.01.15.
 */
public class RelationRowCursorImpl implements RowCursor
{


	private List<ColumnImpl> colList;
	private int currentRow;
	private int totalRowCount;
	RelationImpl rel;

	public RelationRowCursorImpl(ArrayList<ColumnImpl> colList,
			RelationImpl rel)
	{
		this.rel = rel;
		this.colList = colList;
		totalRowCount = rel.getTotalRows();
		currentRow = -1;
	}

	/**
	 *  getter functions
	 */

	@Override
	public RowMetaData getMetaData()
	{
		int temporaryRowId;
		if (currentRow == -1)
		{
			temporaryRowId = 0;
		}
		else
		{
			temporaryRowId = currentRow;
		}

		while (temporaryRowId < totalRowCount)
		{
			while (rel.getRow(temporaryRowId).getMetaData() != null)
			{
				RowMetaData result = rel.getRow(temporaryRowId).getMetaData();
				return result;
			}
		}
		return null;
	}

	@Override
	public int getInteger(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		return colList.get(index).getInteger(currentRow);
	}

	@Override
	public boolean getBoolean(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		return colList.get(index).getBoolean(currentRow);
	}

	@Override
	public double getDouble(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		return colList.get(index).getDouble(currentRow);
	}

	@Override
	public Date getDate(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		return colList.get(index).getDate(currentRow);
	}

	@Override
	public String getString(int index) throws IndexOutOfBoundsException
	{
		return colList.get(index).getString(currentRow);
	}

	@Override
	public Object getObject(int index) throws IndexOutOfBoundsException
	{
		return colList.get(index).getObject(currentRow);
	}

	@Override
	public boolean isNull(int index) throws IndexOutOfBoundsException
	{
		return colList.get(index).isNull(currentRow);
	}

	@Override
	public boolean next()
	{
		boolean isNext = false;
		while (currentRow + 1 < totalRowCount)
		{
			currentRow++;
			isNext = true;
			break;
		}
		return isNext;
	}

	@Override
	public void close() throws IOException
	{
		reset();
	}

	public int getRowCount()
	{
		return totalRowCount;
	}

	public void reset()
	{
		currentRow = -1;
	}
}
