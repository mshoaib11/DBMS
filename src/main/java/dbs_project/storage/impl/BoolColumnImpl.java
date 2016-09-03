package dbs_project.storage.impl;

import dbs_project.storage.Type;

import java.io.Serializable;

/**
 * Created by Xedos2308 on 05.11.14.
 */
public class BoolColumnImpl extends ColumnImpl implements Serializable
{

	private ArrayBoolList dataList;
	private ArrayBoolList nullData;
	private static final long serialVersionUID = -5232406713420573150L;

	public BoolColumnImpl(String columnName, Type columnType, int columnID, String tableName, TableImpl parentTable)
	{
		columnMetaData = new ColumnMetaDataImpl(columnID, columnName, tableName, columnType, parentTable);
		dataList = new ArrayBoolList(10);
		nullData = new ArrayBoolList(10);
	}

	@Override
	public boolean getBoolean(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		if (index >= dataList.getSize())
		{
			throw new IndexOutOfBoundsException();
		}

		if (isNull(index))
		{
			return Type.NULL_VALUE_BOOLEAN;
		}

		return dataList.get(index);
	}

	@Override
	public String getString(int index) throws IndexOutOfBoundsException //for testCastsRow()... index is row index. want to return the integer in string type
	{
		if (index >= dataList.getSize())
		{
			throw new IndexOutOfBoundsException();
		}

		if (isNull(index))
		{
			return String.valueOf(Type.NULL_VALUE_BOOLEAN);
		}

		return String.valueOf(dataList.get(index));
	}

	@Override
	public Object getObject(int index) throws IndexOutOfBoundsException //for testCastsRow()... index is row index. want to return the integer in object type
	{
		if (index >= dataList.getSize())
		{
			throw new IndexOutOfBoundsException();
		}
		if (isNull(index) == true)
		{
			return null;
		}

		return dataList.get(index);
	}

	public void addBoolean(boolean boolValue, boolean isNullFlag)
	{
		if (columnMetaData.getType().equals(Type.BOOLEAN))
		{
			dataList.add(boolValue);
			nullData.add(isNullFlag); //add null values for a newly created bool column
		}
	}

	@Override
	public void set(int index, Object value, boolean isNullFlag) //update row
	{
		dataList.set(index, (Boolean) value);
		nullData.set(index, isNullFlag);
	}

	@Override
	public boolean isNull(int index) throws IndexOutOfBoundsException
	{
		if (dataList != null)
		{
			if (dataList.get(index) == Type.NULL_VALUE_BOOLEAN)
			{
				if (nullData.get(index))
				{
					return true;
				}
				else
				{
					return false;
				}
			}
		}

		return false;
	}

	protected int getArraySize()
	{
		return dataList.getSize();
	}
}
