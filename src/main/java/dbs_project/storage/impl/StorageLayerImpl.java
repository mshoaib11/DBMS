package dbs_project.storage.impl;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import dbs_project.exceptions.NoSuchTableException;
import dbs_project.exceptions.QueryExecutionException;
import dbs_project.exceptions.TableAlreadyExistsException;
import dbs_project.index.IndexLayer;
import dbs_project.index.IndexableTable;
import dbs_project.storage.Table;
import dbs_project.storage.TableMetaData;
import dbs_project.storage.Type;

/**
 * Created by Xedos2308 on 05.11.14.
 */
public class StorageLayerImpl implements IndexLayer, Serializable
{

	//table map <tableID,table>
	private HashMap<Integer, IndexableTable> tableHashMap;
	//number of tables
	private int tableCounter;
	//constructor
	public StorageLayerImpl()
	{
		tableHashMap = new HashMap<>(5);
		tableCounter = -1;
	}

	// receive HashMap with the Updatetables from persistence Layer
	// and set the reference
	public void restoreTablesFromTheFile(
			HashMap<Integer, IndexableTable> tableHashMap) {
		this.tableHashMap=tableHashMap;

	}

	//create new table
	@Override
	public int createTable(String tableName, Map<String, Type> schema)
			throws TableAlreadyExistsException
	{
		Set<Integer> keys = tableHashMap.keySet(); //tablehashMap has tablecounter and refernece to table... initially empty.
		Iterator<Integer> it = keys.iterator();
		//if TableName is not unique throw and exception
		while (it.hasNext())
		{
			Integer table_id = it.next();
			if (((TableImpl) tableHashMap.get(table_id)).getTableName().equals(tableName))
			{
				throw new TableAlreadyExistsException();
			}
		}

		//unique->create new table and add to hashMap
		tableCounter++;
		TableImpl newTab = new TableImpl(tableName, schema, tableCounter);
		tableHashMap.put(tableCounter, newTab);
		return tableCounter;
	}

	//delete table
	@Override
	public void deleteTable(int tableId) throws NoSuchTableException
	{
		//check if there is a table with given ID
		if (tableHashMap.containsKey(tableId))
		{
			tableHashMap.remove(tableId);
		}
		else
		{
			throw new NoSuchTableException();
		}
	}

	//rename table by ID
	@Override
	public void renameTable(int tableId, String newName)
			throws TableAlreadyExistsException, NoSuchTableException
	{
		//check ID existence
		if (tableHashMap.get(tableId) == null)
		{
			throw new NoSuchTableException();
		}

		Set<Integer> keys = tableHashMap.keySet();
		Iterator<Integer> it = keys.iterator();
		//check if newName is unique
		while (it.hasNext())
		{
			Integer table_id = it.next();
			if (((TableImpl) tableHashMap.get(table_id)).getTableName().equals(newName)) //check the table name duplication when renaming... if the newName already exists in tableHashMap
			{
				throw new TableAlreadyExistsException();
			}
		}
		//unique->rename
		((TableImpl) tableHashMap.get(tableId)).renameTable(newName); //test case 1: rename the table in tableMetaDataImpl from table_1 to table_2 for tablecounter=0
																	  //test case 2: rename the table in tableMetaDataImpl from table_2 to table_1(original) for tablecounter=0
	}

	//getTable
	@Override
	public IndexableTable getTable(int tableId) throws NoSuchTableException
	{
		return tableHashMap.get(tableId);

	}

	// return Collection of tables.
	@Override
	public Collection<Table> getTables()
	{

		return (Collection) tableHashMap.values();
	}

	//Mapping table name -> table meta data for all tables in the db
	@Override
	public Map<String, TableMetaData> getDatabaseSchema()
	{
		Map<String, TableMetaData> metaDataMap = new TreeMap<String, TableMetaData>();

		Set<Integer> keys = tableHashMap.keySet();
		Iterator<Integer> it = keys.iterator();
		while (it.hasNext())
		{
			Integer nm = it.next();
			metaDataMap.put(tableHashMap.get(nm).getTableMetaData().getName(), tableHashMap.get(nm).getTableMetaData());
		}
		return metaDataMap;
	}
	//return table for relation for queryLayer
	public TableImpl getTableNameFromColumnName(String columnName)
	{
		Collection<Table> tables = getTables();
		for (Table t : tables)
		{
			if (((TableImpl) t).getColumnId(columnName) != -1)
			{
				return (TableImpl) t;
			}
		}
		return null;
	}

	//return collection of indexTable
	@Override
	public Collection<IndexableTable> getIndexableTables()
	{
		return ((HashMap<Integer, IndexableTable>) tableHashMap.clone()).values();
	}

	//get Table by its Name
	public TableImpl getTable(String tableName) throws QueryExecutionException
	{
		Collection<Table> tables = getTables();
		for (Table t : tables)
		{
			if (t.getTableMetaData().getName().equalsIgnoreCase(tableName))
			{
				return (TableImpl) t;
			}
		}
		throw new QueryExecutionException();
	}

}
