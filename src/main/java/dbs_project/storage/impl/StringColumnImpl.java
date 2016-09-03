package dbs_project.storage.impl;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import dbs_project.storage.Type;

/**
 * Created by Xedos2308 on 05.11.14.
 */
public class StringColumnImpl extends ColumnImpl implements Serializable
{

	
	private static final long serialVersionUID = 6381706312690990467L;

	//store String as byte array.
	private List<byte[]> stringList;

	public StringColumnImpl(String columnName, Type type, int columnID,	String tableName, TableImpl parentTable)
	{
		super.columnMetaData = new ColumnMetaDataImpl(columnID, columnName, tableName, type, parentTable);
		stringList = new ArrayList<>(10);
	}

	//get String
	@Override
	public String getString(int index) throws IndexOutOfBoundsException
	{
		if (index >= getArraySize())
		{
			throw new IndexOutOfBoundsException();
		}

		if (isNull(index) == true)
		{
			return null;
		}
		return new String(stringList.get(index));
	}

	//get Object
	@Override
	public Object getObject(int index) throws IndexOutOfBoundsException //for testCastsRow()... index is row index. want to return the object in string type

	{
		if (index >= getArraySize())
		{
			throw new IndexOutOfBoundsException();
		}

		if (isNull(index) == true)
		{
			return null;
		}
		return new String(stringList.get(index));
	}

	//Add String to List
	public void addString(String s)
	{
		if (columnMetaData.getType().equals(Type.STRING))
		{
			if (s != null)
			{
				stringList.add(s.getBytes(Charset.forName("UTF-8")));
			}
			else
			{
				stringList.add(null);
			}
		}
	}

	//change value at given index
	@Override
	public void set(int index, Object value, boolean nullFlag)
	{
		String s = (String) value;
		if (s != null)
		{
			stringList.set(index, s.getBytes(Charset.forName("UTF-8")));
		}
		else
		{
			stringList.set(index, null);
		}
	}

	//check null
	@Override
	public boolean isNull(int index) throws IndexOutOfBoundsException
	{
		if (stringList != null)
		{
			if (stringList.get(index) == null)
				return true;
		}
		return false;
	}

	protected int getArraySize()
	{
		return stringList.size();
	}
}
