package dbs_project.query.impl;

import java.util.Date;
import dbs_project.storage.Column;
import dbs_project.storage.Row;
import dbs_project.storage.RowMetaData;

/**
 * Created by Xedos2308 on 12.01.15.
 */
public class RelationRowImpl implements Row
{


	private RelationRowMetaDataImpl metaData;

	public Column getColumnAtIndex(int index)
	{

		return metaData.getSourceTable().getColumnsAsList().get(index);
	}

	public RelationRowImpl(int rowId, RelationImpl relation)
	{

		metaData = new RelationRowMetaDataImpl(rowId, relation);
	}

	/**
	 * getter functions
	 **/
	@Override
	public int getInteger(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		int rowID = metaData.getId() - 1;
		Column column;
		column = getColumnAtIndex(index);
		return column.getInteger(rowID);
	}

	@Override
	public boolean getBoolean(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		int rowID = metaData.getId() - 1;
		Column column = getColumnAtIndex(index);
		return column.getBoolean(rowID);
	}

	@Override
	public double getDouble(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		int rowID = metaData.getId() - 1;
		Column column = getColumnAtIndex(index);
		return column.getDouble(rowID);
	}

	@Override
	public Date getDate(int index) throws IndexOutOfBoundsException,
			ClassCastException
	{
		int rowID = metaData.getId() - 1;
		Column column = getColumnAtIndex(index);
		return column.getDate(rowID);
	}

	@Override
	public String getString(int index) throws IndexOutOfBoundsException
	{
		int rowID = metaData.getId() - 1;
		Column column = getColumnAtIndex(index);
		return column.getString(rowID);
	}

	@Override
	public Object getObject(int index) throws IndexOutOfBoundsException
	{
		int rowID = metaData.getId() - 1;
		Column column = getColumnAtIndex(index);
		return column.getObject(rowID);
	}

	@Override
	public boolean isNull(int index) throws IndexOutOfBoundsException
	{
		int rowID = metaData.getId();
		Column column = getColumnAtIndex(index);
		return column.isNull(rowID);
	}

	@Override
	public RowMetaData getMetaData()
	{
		return metaData;
	}
}
