package dbs_project.storage.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.List;

import dbs_project.storage.ColumnCursor;
import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.Type;

/**
 * Created by Xedos2308 on 05.11.14.
 */
public class ColumnCursorImpl implements ColumnCursor, Serializable
{

	private static final long serialVersionUID = 4762403305530877571L;
	
	private List<ColumnImpl> colList;
	private ColumnImpl currentCol = new ColumnImpl();
	private int position;
	

	public int getSize()
	{
		return colList.size();
	}


	public ColumnCursorImpl(List<ColumnImpl> colList) //a list of all the columns of a table with reference to columnImpl
	{
		this.colList = colList; //also return the complete list of cols along with the current column in getColumns()
		position = 0;
		if (this.colList.size() > 0)
		{
			currentCol = this.colList.get(0);
		}
	}

	@Override
	public ColumnMetaData getMetaData()
	{

		return currentCol.getMetaData(); 	//return the metaData of current col
											//currentCol reference to ColumnImpl... getMetaData() is of ColumnImpl.
	}

	@Override
	public int getInteger(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		if (getColumnType() == Type.INTEGER)
		{
			return currentCol.getInteger(index);
		}
		else
			if (getColumnType() == Type.DOUBLE)
			{
				return (int) currentCol.getDouble(index);
			}
			else
				if (isNull(index) == true)
				{
					return Type.NULL_VALUE_INTEGER;
				}
		throw new ClassCastException();
	}

	@Override
	public boolean getBoolean(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		if (getColumnType() == Type.BOOLEAN)
		{
			return currentCol.getBoolean(index);
		}
		else
			if (isNull(index) == true)
			{
				return Type.NULL_VALUE_BOOLEAN;
			}
		throw new ClassCastException();
	}

	@Override
	public double getDouble(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		if (getColumnType() == Type.INTEGER)
		{
			return currentCol.getInteger(index);
		}
		else
			if (getColumnType() == Type.DOUBLE)
			{
				return currentCol.getDouble(index);
			}
			else
				if (isNull(index) == true)
				{
					return Type.NULL_VALUE_DOUBLE;
				}
		throw new ClassCastException();
	}

	@Override
	public Date getDate(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		return currentCol.getDate(index);
	}

	@Override
	public String getString(int index) throws IndexOutOfBoundsException
	{
		if (isNull(index) == true)
		{
			return null;
		}
		else
		{
			String result;
			switch (getColumnType())
			{
				case BOOLEAN:
					result = String.valueOf(currentCol.getBoolean(index));
					break;
				case DATE:
					result = String.valueOf(currentCol.getDate(index));
					break;
				case DOUBLE:
					result = String.valueOf(currentCol.getDouble(index));
					break;
				case INTEGER:
					result = String.valueOf(currentCol.getInteger(index));
					break;
				case STRING:
					result = String.valueOf(currentCol.getString(index));
					break;
				default:
					result = null;
					break;
			}
			return result;
		}
	}

	@Override
	public Object getObject(int index) throws IndexOutOfBoundsException
	{
		if (isNull(index) == true)
		{
			return null;
		}
		else
		{
			Object toReturn;
			switch (getColumnType())
			{
				case BOOLEAN:
					toReturn = new Boolean(currentCol.getBoolean(index));
					break;
				case DATE:
					toReturn = currentCol.getDate(index);
					break;
				case DOUBLE:
					toReturn = new Double(currentCol.getDouble(index));
					break;
				case INTEGER:
					toReturn = new Integer(currentCol.getInteger(index));
					// String.valueOf(current.getInteger(index));
					break;
				case STRING:
					toReturn = currentCol.getString(index);
					break;
				default:
					toReturn = null;
					break;
			}
			return toReturn;
		}
	}

	public void set(int index, Object value, boolean nullFlag)
	{
		currentCol.set(index, value, nullFlag); 
	}

	public void addInteger(int i, boolean nullFlag)
	{
		currentCol.addInteger(i, nullFlag);
	}

	public void addBoolean(boolean b, boolean nullFlag)
	{
		currentCol.addBoolean(b, nullFlag);
	}

	public void addDouble(double d, boolean nullFlag)
	{
		currentCol.addDouble(d, nullFlag);
	}

	public void addString(String s)
	{
		currentCol.addString(s);
	}

	public void addDate(Date d)
	{
		currentCol.addDate(d);
	}

	public Type getColumnType()
	{
		return currentCol.getMetaData().getType();
	}

	@Override
	public boolean next() //to iterate over the cols in colList
	{
		if (position < colList.size())
		{
			currentCol = colList.get(position);
			position++;
			return true;
		}
		else
		{
			return false;
		}
	}

	@Override
	public void close() throws IOException
	{
	}

	public void reset()
	{

		position = 0;
	}

	@Override
	public boolean isNull(int index) throws IndexOutOfBoundsException
	{
		return false;
	}
}
