package dbs_project.storage.impl;


import org.apache.commons.collections.primitives.ArrayIntList;

import dbs_project.storage.Type;

import java.io.Serializable;

/**
 * Created by Xedos2308 on 05.11.14.
 */
public class IntColumnImpl extends ColumnImpl implements Serializable
{

	private ArrayIntList dataList;
	private ArrayBoolList nullData;
	private static final long serialVersionUID = -4775198331542693062L;

	public IntColumnImpl(String columnName, Type type, int columnID, String tableName, TableImpl sourceTable)
	{
		columnMetaData = new ColumnMetaDataImpl(columnID, columnName, tableName, type, sourceTable);
		dataList = new ArrayIntList(10);
		nullData = new ArrayBoolList(10);
	}

	@Override
	public int getInteger(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		if (index >= getArraySize())
		{
			throw new IndexOutOfBoundsException();
		}

		if (isNull(index) == true)
		{
			return Type.NULL_VALUE_INTEGER;
		}

		return dataList.get(index);
	}

	@Override
	public double getDouble(int index) throws IndexOutOfBoundsException, //for testCastsRow()... index is row index. want to return the integer in double type
			ClassCastException
	{
		if (index >= getArraySize())
		{
			throw new IndexOutOfBoundsException();
		}

		if (isNull(index) == true)
		{
			return Type.NULL_VALUE_INTEGER;
		}

		return dataList.get(index);
	}

	@Override
	public String getString(int index) throws IndexOutOfBoundsException //for testCastsRow()... index is row index. want to return the integer in string type
	{
		if (index >= getArraySize())
		{
			throw new IndexOutOfBoundsException();
		}

		if (isNull(index) == true)
		{
			return String.valueOf(Type.NULL_VALUE_INTEGER);
		}

		return String.valueOf(dataList.get(index));
	}

	@Override
	public Object getObject(int index) throws IndexOutOfBoundsException //for testCastsRow()... index is row index. want to return the integer in object type
	{
		if (index >= getArraySize())
		{
			throw new IndexOutOfBoundsException();
		}

		if (isNull(index) == true)
		{
			return null;
		}

		return dataList.get(index);
	}

	public void addInteger(int i, boolean nullFlag)
	{
		if (columnMetaData.getType().equals(Type.INTEGER))
		{
			dataList.add(i);
			nullData.add(nullFlag); //add null values for a newly created int column
		}
	}

	@Override
	public void set(int index, Object value, boolean nullFlag) //update row
	{
		dataList.set(index, Integer.parseInt(value.toString()));
		nullData.set(index, nullFlag);
	}

	@Override
	public boolean isNull(int index) throws IndexOutOfBoundsException
	{
		if (dataList != null)
		{
			if (dataList.get(index) == Type.NULL_VALUE_INTEGER)
			{
				if (nullData.get(index))
				{
					return true;
				}
				else
					return false;
			}
		}
		return false;
	}

	protected int getArraySize()
	{
		return dataList.size();
	}

}
