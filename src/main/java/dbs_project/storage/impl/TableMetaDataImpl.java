package dbs_project.storage.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.collections.primitives.ArrayIntList;

import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.TableMetaData;

/**
 * Created by Xedos2308 on 05.11.14.
 */
public class TableMetaDataImpl implements TableMetaData, Serializable
{


	private static final long serialVersionUID = 1L;
	//unique table ID
	private int tableId;
	//table name
	private String tableName;
	//MetaData mapping columnName|columnMetaData
	private Map<String, ColumnMetaData> columnMetaDataMap;
	//data array
	private ArrayList<RowImpl> rowArrayList;
	//arrayList for deleted rows
	private ArrayIntList deletedRowsArrayList;
	
	static int no_of_rows = 0;
	public TableMetaDataImpl(){
		no_of_rows++;
	}
	
	public TableMetaDataImpl(int tableId, String tableName, Map<String, ColumnMetaData> columnMetaDataMap, ArrayList<RowImpl> rowArrayList,	ArrayIntList deletedRowsArrayList)
	{
		this.tableId = tableId;
		this.tableName = tableName;
		this.columnMetaDataMap = columnMetaDataMap;
		this.rowArrayList = rowArrayList;
		this.deletedRowsArrayList = deletedRowsArrayList;
	}
	//set new table name
	public void renameTable(String newName)
	{
		tableName = newName; //test case 1: changing the tableName(table_1) to newName(table_2) so that when getName() is called it must return the newName(table_2)
							 //test case 2: changing the tableName(table_2) to newName original (table_1) so that when getName() is called it must return the original (table_1)
	}

	//getter
	@Override
	public int getId()
	{

		return this.tableId;
	}
	//getter
	@Override
	public String getName()
	{

		return this.tableName;
	}

	//return the schema for the table (column name -> column meta data, no ordering assumed)
	@Override
	public Map<String, ColumnMetaData> getTableSchema()
	{

		return this.columnMetaDataMap;
	}

	//return number of rows in the table if known
	@Override
	public int getRowCount()
	{
		//int rowCounter = no_of_rows;
		int rowCounter = rowArrayList.size() - deletedRowsArrayList.size();
		return rowCounter;
	}
}
