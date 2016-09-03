package dbs_project.storage.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import dbs_project.storage.Type;

/**
 * Created by Xedos2308 on 05.11.14.
 */
public class DateColumnImpl extends ColumnImpl implements Serializable
{


	private ArrayList<Date> dataList;
	private static final long serialVersionUID = -3531558341176336261L;

	public DateColumnImpl(String columnName, Type type, int columnID, String tableName, TableImpl sourceTable)
	{
		columnMetaData = new ColumnMetaDataImpl(columnID, columnName, tableName, type, sourceTable);
		dataList = new ArrayList<>(10);
	}

	@Override
	public Date getDate(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		if (index >= getArraySize())
		{
			throw new IndexOutOfBoundsException();
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
		if (isNull(index))
		{
			return null;
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

	@Override
	public boolean isNull(int index) throws IndexOutOfBoundsException
	{
		if (dataList != null)
		{
			if (dataList.get(index) == null)
				return true;
		}

		return false;
	}

	public void addDate(Date d)
	{
		if (columnMetaData.getType().equals(Type.DATE))
		{
			dataList.add(d);
		}
	}

	@Override
	public void set(int index, Object value, boolean nullFlag) //update row
	{

		dataList.set(index, (Date) value);
	}

	protected int getArraySize()
	{

		return dataList.size();
	}

}
