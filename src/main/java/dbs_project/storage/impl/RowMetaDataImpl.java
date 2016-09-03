package dbs_project.storage.impl;


import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.RowMetaData;

import java.io.Serializable;

/**
 * Created by Xedos2308 on 05.11.14.
 */
public class RowMetaDataImpl implements RowMetaData, Serializable
{


	private int rowId;
	//Source table
	private TableImpl sourceTable;

	public RowMetaDataImpl(int rowId, TableImpl sourceTable)
	{
		this.rowId = rowId;
		this.sourceTable = sourceTable;
	}
	//get Source table
	public TableImpl getSourceTable()
	{

		return sourceTable;
	}

	//get ID
	@Override
	public int getId()
	{

		return rowId;
	}

	//return Number of Columns
	@Override
	public int getColumnCount()
	{
		return sourceTable.getTableMetaData().getTableSchema().size();
	}

	@Override
	public ColumnMetaData getColumnMetaData(int positionInTheRow)
			throws IndexOutOfBoundsException
	{
		//check bounds of positionInTheRow
		if (sourceTable.getColumnsList().size() > positionInTheRow)
		{
			//return Meta Data that describes the given column in the row
			return sourceTable.getColumnsList().get(positionInTheRow)
					.getMetaData();
		}
		else
		{
			throw new IndexOutOfBoundsException();
		}
	}

}