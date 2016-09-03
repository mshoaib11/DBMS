package dbs_project.storage.impl;

import java.io.Serializable;
import java.util.Date;
import dbs_project.storage.Column;

/**
 * Created by Xedos2308 on 05.11.14.
 */
public class ColumnImpl implements Column, Serializable
{


	protected ColumnMetaDataImpl columnMetaData; //a bridge towards ColumnMetaDataImpl methods
	
	private static final long serialVersionUID = 189820610141778734L;

	public void renameParentTable(String newParentTableName)
	{

		columnMetaData.renameParentTable(newParentTableName);
	}
	public String getColumnName()
	{
		return columnMetaData.getName();
	}

	@Override
	public ColumnMetaDataImpl getMetaData()
	{

		return columnMetaData; //a bridge towards ColumnMetaDataImpl methods
	}

	@Override
	public int getInteger(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		throw new ClassCastException();
	}

	@Override
	public boolean getBoolean(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		throw new ClassCastException();
	}

	@Override
	public double getDouble(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		throw new ClassCastException();
	}

	@Override
	public Date getDate(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		throw new ClassCastException();
	}

	@Override
	public String getString(int index) throws IndexOutOfBoundsException
	{
		return null;
	}

	@Override
	public Object getObject(int index) throws IndexOutOfBoundsException
	{
		return null;
	}

	public void addInteger(int i, boolean nullFlag)
	{
	}

	public void addBoolean(boolean b, boolean nullFlag)
	{
	}

	public void addDouble(double d, boolean nullFlag)
	{
	}

	public void addString(String s)
	{
	}

	public void addDate(Date d)
	{
	}

	protected int getArraySize()
	{
		return -1;
	}

	public void set(int index, Object value, boolean nullFlag)
	{
	}

	@Override
	public boolean isNull(int index) throws IndexOutOfBoundsException
	{
		return false;
	}
}