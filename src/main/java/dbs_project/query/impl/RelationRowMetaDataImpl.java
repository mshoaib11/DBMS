package dbs_project.query.impl;

import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.RowMetaData;

/**
 * Created by Xedos2308 on 12.01.15.
 */
public class RelationRowMetaDataImpl implements RowMetaData
{

	private int totalRowId;
	private RelationImpl rel;

	public RelationRowMetaDataImpl(int totalRowId, RelationImpl rel)
	{
		this.totalRowId = totalRowId;
		this.rel = rel;
	}

	//getter functions
	@Override
	public int getId()
	{
		return totalRowId;
	}

	@Override
	public int getColumnCount()
	{

		return rel.getColumnsAsList().size();
	}

	public RelationImpl getSourceTable()
	{

		return rel;
	}

	@Override
	public ColumnMetaData getColumnMetaData(int positionInTheRow)
			throws IndexOutOfBoundsException
	{
		if (getColumnCount() > positionInTheRow)
		{
			return rel.getColumnsAsList().get(positionInTheRow)
					.getMetaData();
		}
		else
		{
			throw new IndexOutOfBoundsException();
		}
	}

}
