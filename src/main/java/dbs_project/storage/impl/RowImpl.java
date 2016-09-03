package dbs_project.storage.impl;

import java.io.Serializable;
import java.util.Date;
import dbs_project.exceptions.NoSuchColumnException;
import dbs_project.storage.Column;
import dbs_project.storage.Row;
import dbs_project.storage.RowMetaData;

/**
 * Created by Xedos2308 on 05.11.14.
 */
public class RowImpl implements Row, Serializable
{

	private RowMetaDataImpl metaData;
	private int rowID;


	public RowImpl(int rId, TableImpl parent)
	{
		metaData = new RowMetaDataImpl(rId, parent);
	}

	@Override
	public int getInteger(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		rowID = metaData.getId();
		Column column;
		try
		{
			column = metaData.getSourceTable().getColumn(index);
		}
		catch (NoSuchColumnException e)
		{
			throw new IndexOutOfBoundsException();
		}
		int value = column.getInteger(rowID);
		return value;
	}

	@Override
	public boolean getBoolean(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		rowID = metaData.getId();
		Column column;
		try
		{
			column = metaData.getSourceTable().getColumn(index);
		}
		catch (NoSuchColumnException e)
		{
			throw new IndexOutOfBoundsException();
		}
		boolean value = column.getBoolean(rowID);
		return value;
	}

	@Override
	public double getDouble(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		rowID = metaData.getId();
		Column column;
		try
		{
			column = metaData.getSourceTable().getColumn(index);
		}
		catch (NoSuchColumnException e)
		{
			throw new IndexOutOfBoundsException();
		}
		double value = column.getDouble(rowID);
		return value;
	}

	@Override
	public Date getDate(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		rowID = metaData.getId();
		Column column;
		try
		{
			column = metaData.getSourceTable().getColumn(index);
		}
		catch (NoSuchColumnException e)
		{
			throw new IndexOutOfBoundsException();
		}
		Date value = column.getDate(rowID);
		return value;
	}

	@Override
	public String getString(int index) throws IndexOutOfBoundsException
	{
		rowID = metaData.getId();
		Column column;
		try
		{
			column = metaData.getSourceTable().getColumn(index);
		}
		catch (NoSuchColumnException e)
		{
			throw new IndexOutOfBoundsException();
		}
		String value = column.getString(rowID);
		return value;
	}

	@Override
	public Object getObject(int index) throws IndexOutOfBoundsException
	{
		rowID = metaData.getId();
		Column column;
		try
		{
			column = metaData.getSourceTable().getColumn(index);
		}
		catch (NoSuchColumnException e)
		{
			throw new IndexOutOfBoundsException();
		}
		Object value = column.getObject(rowID);
		return value;
	}

	@Override
	public RowMetaData getMetaData()
	{
		return metaData;
	}

	@Override
	public boolean isNull(int index) throws IndexOutOfBoundsException
	{
		rowID = metaData.getId();
		Column column;
		try
		{
			column = metaData.getSourceTable().getColumn(index);
		}
		catch (NoSuchColumnException e)
		{
			throw new IndexOutOfBoundsException();
		}
		return column.isNull(rowID);
	}
}
