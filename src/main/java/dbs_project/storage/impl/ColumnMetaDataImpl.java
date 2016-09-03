package dbs_project.storage.impl;

import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.Table;
import dbs_project.storage.Type;
import dbs_project.util.Named;

import java.io.Serializable;

/**
 * Created by Xedos2308 on 05.11.14.
 */
public class ColumnMetaDataImpl implements ColumnMetaData, Serializable
{

	private int colId;
	private String colName;
	private Type colType;
	private TableImpl sourceTable;
	private String sourceTableName;

	public ColumnMetaDataImpl(int colId, String colName, String sourceTableName, Type colType, TableImpl sourceTable)
	{
		this.colId = colId;
		this.colName = colName;
		this.sourceTableName = sourceTableName;
		this.colType = colType;
		this.sourceTable = sourceTable;
	}

	public void renameParentTable(String newParentTableName)
	{

		sourceTableName = newParentTableName;
	}

	public void renameColumn(String newName)
	{

		colName = newName; //change the name of the column from old to newName
	}

	@Override
	public int getId()
	{

		return colId;
	}

	@Override
	public String getName()
	{
		return colName;
	}

	@Override
	public int getRowCount()
	{

		return sourceTable.getTableMetaData().getRowCount();
	}

	@Override
	public Table getSourceTable()
	{
		return sourceTable;
	}

	@Override
	public String getLabel()
	{
		return sourceTableName + "." + colName;		
	}

	@Override
	public Type getType()
	{

		return colType;
	}

	@Override
	public int getRowId(int positionInColumn) throws IndexOutOfBoundsException
	{
		return 0;
	}

	@Override
	public boolean equals(Object meta)
	{
		Type type = ((ColumnMetaData) meta).getType();
		String name = ((Named) meta).getName();

		boolean t2 = type == colType;
		boolean t3 = name == colName;
		return t2 && t3;
	}

}
