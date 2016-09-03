package dbs_project.storage.impl;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.commons.collections.primitives.ArrayIntList;

import dbs_project.exceptions.NoSuchRowException;
import dbs_project.storage.RowCursor;
import dbs_project.storage.RowMetaData;

/**
 * Created by Xedos2308 on 05.11.14.
 */
public class RowCursorImpl implements RowCursor
{

	private List<ColumnImpl> columnList;
	private ArrayIntList rowIdList;
	private int rowPointer;
	private int currentRowID;
	private int rowCounter;


	public RowCursorImpl(List<ColumnImpl> cols, ArrayIntList ids)
	{
		columnList = cols;
		rowPointer = -1;
		rowIdList = ids;
		currentRowID = 0;
		if (columnList.size() > 0)
		{
			this.rowCounter = getTable().getTotalRows();
		}
		else
		{
			this.rowCounter = 0;
		}
	}

	public TableImpl getTable()
	{

		return (TableImpl) columnList.get(0).getMetaData().getSourceTable();
	}

	@Override
	public RowMetaData getMetaData()
	{
		int rowIdd;
		if (rowPointer == -1)
		{
			rowIdd = 0;
		}
		else
		{
			rowIdd = rowPointer;
		}

		TableImpl parent = getTable();
		while (rowIdd < rowCounter)
		{
			try
			{
				while (parent.getRow(rowIdd).getMetaData() != null)
				{
					RowMetaData result = parent.getRow(rowIdd).getMetaData();
					return result;
				}
			}
			catch (NoSuchRowException e)
			{
				rowIdd++;
			}
		}
		return null;
	}

	@Override
	public int getInteger(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		return columnList.get(index).getInteger(rowPointer); //get the boolean for current col(index) and current row(rowPointer)
	}

	@Override
	public boolean getBoolean(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		return columnList.get(index).getBoolean(rowPointer); //get the boolean for current col(index) and current row(rowPointer)
	}

	@Override
	public double getDouble(int index) throws IndexOutOfBoundsException, 
			ClassCastException
	{
		return columnList.get(index).getDouble(rowPointer); //get the double for current col(index) and current row(rowPointer)
	}

	@Override
	public Date getDate(int index) throws IndexOutOfBoundsException, 
			ClassCastException
	{
		return columnList.get(index).getDate(rowPointer); //get the date for current col(index) and current row(rowPointer)
	}

	@Override
	public String getString(int index) throws IndexOutOfBoundsException
	{
		return columnList.get(index).getString(rowPointer); //get the string for current col(index) and current row(rowPointer)
	}

	@Override
	public Object getObject(int index) throws IndexOutOfBoundsException
	{
		return columnList.get(index).getObject(rowPointer); //get the object for current col(index) and current row(rowPointer)
	}

	@Override
	public boolean next()
	{
		if (rowIdList != null && currentRowID < rowIdList.size())
		{
			rowPointer = rowIdList.get(currentRowID);
			currentRowID++;
			return true;
		}
		while (rowPointer + 1 < rowCounter)
		{
			try
			{
				rowPointer++;
				getTable().getRow(rowPointer).getMetaData();
				return true;

			}
			catch (NoSuchRowException e)
			{
			}
		}
		return false;
	}

	@Override
	public void close() throws IOException
	{
		reset();
	}

	public int getRowCount()
	{
		return rowCounter;
	}

	public void reset()
	{
		rowPointer = -1;
		currentRowID = 0;
	}

	@Override
	public boolean isNull(int index) throws IndexOutOfBoundsException
	{
		return columnList.get(index).isNull(rowPointer);
	}

}