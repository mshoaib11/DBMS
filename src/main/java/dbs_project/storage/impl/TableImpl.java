package dbs_project.storage.impl;

import java.io.Serializable;
import java.util.*;
import dbs_project.index.impl.HashIndex;
import dbs_project.index.impl.TreeIndexBTreeImpl;
import dbs_project.storage.*;
import org.apache.commons.collections.primitives.ArrayIntList;
import dbs_project.exceptions.*;
import dbs_project.index.Index;
import dbs_project.index.IndexType;
import dbs_project.index.IndexableTable;
import dbs_project.util.IdCursor;


/**
 * Created by Xedos2308 on 05.11.14.
 */
public class TableImpl implements IndexableTable, Serializable
{
	
	//storage
	private static final long serialVersionUID = -4486599984584016633L;
	private int colCounter = -1;
	private int rowCounter = -1;
	private ArrayList<ColumnImpl> columnList;
	private TableMetaDataImpl tableMetaData;
	private Map<String, ColumnMetaData> columnsMap;
	private ArrayList<RowImpl> rowList;
	private ArrayIntList deletedRows;
	private ColumnCursorImpl colCursor = null;
	private int mapping_Array[];
	private int totalRowCounter = 0;
	//storage
	
	
	private int query_Mappings[];	
	private boolean is_Bulk_Rows_Insertion = false;
	private ObjectHashMap indexes;
	private int indexId;
	private Map<String, Index> NameToIndexMap;

	public TableImpl(String name, Map<String, Type> schema, int tableId)
	{
		indexId = -1;
		indexes = new ObjectHashMap();
		NameToIndexMap = new HashMap<>();

		
		if (schema != null)
		{
			columnList = new ArrayList<>(); //contains reference to columnImpl for each column.
			columnsMap = new HashMap<>(5); //columnsMap --> HashMap of colName and reference to columnMetaData
			rowList = new ArrayList<>(10); 
			deletedRows = new ArrayIntList(10);
			tableMetaData = new TableMetaDataImpl(tableId, name, columnsMap, rowList, deletedRows); //columnsMap initially empty coz no column created yet.
			
			Set keys = schema.entrySet(); 
			Iterator it = keys.iterator();
			while (it.hasNext())
			{
				Map.Entry column = (Map.Entry)it.next();
				try
				{
					createColumn(column.getKey().toString(), (Type) column.getValue());
				}
				catch (ColumnAlreadyExistsException e)
				{
					e.printStackTrace();
				}
			}
		}
	}




	public void renameTable(String newName)
	{
		tableMetaData.renameTable(newName); //test case 1: rename the table in tableMetaDataImpl from table_1 to table_2 for tablecounter=0
											//test case 2: rename the table in tableMetaDataImpl from table_2 to table_1(original) for tablecounter=0

		/*for (int i = 0; i < columnList.size(); i++)
		{
			columnList.get(i).renameParentTable(newName);
		}*/
	}


	public String getTableName()
	{

		return tableMetaData.getName();
	}

	@Override
	public RowCursor getRows()
	{
		ArrayIntList idList = new ArrayIntList(2);
		for (int i = 0; i < rowList.size(); i++)
		{
			if (rowList.get(i) != null)
				idList.add(i);
		}
		return new RowCursorImpl(columnList, idList);
	}

	@Override
	public ColumnCursor getColumns()
	{
		ColumnCursorImpl colCur = new ColumnCursorImpl(columnList);
		return colCur;
	}

	public List<ColumnImpl> getColumnsList()
	{

		return columnList;
	}

	public int getColumnId(String colName)
	{
		try
		{
			return tableMetaData.getTableSchema().get(colName).getId();
		}
		catch (Exception e)
		{
			return -1;
		}
	}
	public void addRowsInQuery(List<String> columnNames,
							   Iterator<List<String>> dataForRows) throws SchemaMismatchException
	{
		findColMapWithColNames(columnNames);
		if (colCursor == null)
		{
			colCursor = (ColumnCursorImpl) getColumns();
		}
		while (dataForRows.hasNext())
		{
			colCursor.reset();
			addRowInQuery(dataForRows.next());
		}

	}
	private int addRowInQuery(List<String> row) throws SchemaMismatchException
	{
		int position = -1;
		if (deletedRows.size() > 0)
		{
			rowCounter = deletedRows.get(deletedRows.size() - 1);
			while (colCursor.next())
			{
				Type columnType = colCursor.getMetaData().getType();
				position++;
				switch (columnType)
				{
					case BOOLEAN:
						colCursor.set(rowCounter, Boolean.parseBoolean(row
								.get(query_Mappings[position])), false);
						break;
					case DATE:
						colCursor.set(rowCounter, new Date(java.sql.Date
								.valueOf(row.get(query_Mappings[position]))
								.getTime()), false);
						break;
					case DOUBLE:
						colCursor.set(rowCounter, Double.parseDouble(row
								.get(query_Mappings[position])), false);
						break;
					case INTEGER:
						colCursor.set(rowCounter, Integer.parseInt(row
								.get(query_Mappings[position])), false);
						break;
					case STRING:
						colCursor.set(rowCounter,
								row.get(query_Mappings[position]), false);
						break;
					default:
						break;
				}
			}
			RowImpl RowImpl = new RowImpl(rowCounter, this);
			deletedRows.removeElementAt(deletedRows.size() - 1);
			rowList.set(rowCounter, RowImpl);
		}
		else
		{
			while (colCursor.next())
			{
				Type columnType = colCursor.getMetaData().getType();
				position++;
				switch (columnType)
				{
					case BOOLEAN:
						colCursor.addBoolean(Boolean.parseBoolean(row
								.get(query_Mappings[position])), false);
						break;
					case DATE:
						colCursor.addDate(new Date(java.sql.Date.valueOf(
								row.get(query_Mappings[position])).getTime()));
						break;
					case DOUBLE:
						colCursor.addDouble(Double.parseDouble(row
								.get(query_Mappings[position])), false);
						break;
					case INTEGER:
						colCursor.addInteger(Integer.parseInt(row
								.get(query_Mappings[position])), false);
						break;
					case STRING:
						colCursor
								.addString(row.get(query_Mappings[position]));
						break;
					default:
						break;
				}
			}
			rowCounter++;
			RowImpl RowImpl = new RowImpl(rowCounter, this);
			rowList.add(RowImpl);
		}
		addRowToIndex(rowCounter);
		totalRowCounter++;
		return rowCounter;
	}
	private void findColMapWithColNames(List<String> columnNames)
			throws SchemaMismatchException
	{
		if (query_Mappings != null)
		{
			return;
		}
		if (getColumnsList().size() != columnNames.size())
		{
			throw new SchemaMismatchException();
		}

		query_Mappings = new int[columnNames.size()];
		ColumnCursorImpl columnCursor = (ColumnCursorImpl) getColumns();
		int i = -1;
		while (columnCursor.next())
		{
			i++;
			String name = columnCursor.getMetaData().getName();
			for (int j = 0; j < columnNames.size(); j++)
			{
				if (columnNames.get(j).equals(name))
				{
					query_Mappings[i] = j;
					break;
				}
			}
		}
	}

	@Override
	public void renameColumn(int colId, String newColName)
			throws ColumnAlreadyExistsException, NoSuchColumnException
	{
		if (columnsMap.containsKey(newColName)) //columnsMap --> HashMap of colName and reference to columnMetaData
		{
			throw new ColumnAlreadyExistsException();
		}
		if(colId > columnsMap.size()-1){
			throw new NoSuchColumnException();
		}
		columnsMap.remove(columnList.get(colId).getMetaData().getName()); //columnsMap --> HashMap of colName and reference to columnMetaData
		columnList.get(colId).columnMetaData.renameColumn(newColName); //private ArrayList<ColumnImpl> columnList; (e.g. IntColumnImpl, BoolcolumnImpl)
			 														   //columnList.add(column); //contains reference to columnImpl for each column.
		columnsMap.put(newColName, columnList.get(colId).getMetaData());
	}

	@Override
	public int createColumn(String colName, Type colType)
			throws ColumnAlreadyExistsException
	{
		if (columnsMap.containsKey(colName))
		{
			throw new ColumnAlreadyExistsException();
		}
		colCounter++;
		ColumnImpl column = null;
		switch (colType)
		{
			case BOOLEAN:
				column = new BoolColumnImpl(colName, colType, colCounter, tableMetaData.getName(), this); //columnMetaData is stored through columnMetaDataImpl via BoolColumnImpl
				break;
			case DATE:
				column = new DateColumnImpl(colName, colType, colCounter, tableMetaData.getName(), this); //columnMetaData is stored through columnMetaDataImpl via DateColumnImpl
				break;
			case DOUBLE:
				column = new DoubleColumnImpl(colName, colType, colCounter, tableMetaData.getName(), this); //columnMetaData is stored through columnMetaDataImpl via DoubleColumnImpl
				break; 
			case INTEGER:
				column = new IntColumnImpl(colName, colType, colCounter, tableMetaData.getName(), this); //columnMetaData is stored through columnMetaDataImpl via IntColumnImpl
				break;
			case STRING:
				column = new StringColumnImpl(colName, colType,	colCounter, tableMetaData.getName(), this); //columnMetaData is stored through columnMetaDataImpl via StringColumnImpl
				break;
			default:
				column = new ColumnImpl();
				break;
		}
		//add null values for a newly created column
		for (int i = 0; i < tableMetaData.getRowCount(); i++)
		{
			switch (colType)
			{
				case BOOLEAN:
					column.addBoolean(Type.NULL_VALUE_BOOLEAN, true);
					break;
				case DATE:
					column.addDate(null);
					break;
				case DOUBLE:
					column.addDouble(Type.NULL_VALUE_DOUBLE, true);
					break;
				case INTEGER:
					column.addInteger(Type.NULL_VALUE_INTEGER, true);
					break;
				case STRING:
					column.addString(null);
					break;
				default:
					break;
			}
		}
		columnsMap.put(colName, column.getMetaData()); ////columnsMap --> HashMap of colName and reference to columnMetaData (column.getMetaData() gives e.g. StringColumnImpl)
		columnList.add(column); //contains reference to columnImpl for each column.
		indexes.put(colCounter, new ArrayList<Index>());
		return colCounter;
	}

	private void findColMap(Row row) throws SchemaMismatchException
	{
		if (mapping_Array != null)
		{
			return;
		}
		if (columnList.size() != row.getMetaData().getColumnCount()) //get the columnCount for this row
		{
			throw new SchemaMismatchException();
		}

		mapping_Array = new int[row.getMetaData().getColumnCount()]; //initially like [0,0,0,0,0] for table_1
																	 //columnCursor has 5 indexes for colList for table_1
																	 //the ordering of indexes is different from that in actual tables 
		ColumnCursorImpl columnCursor = (ColumnCursorImpl) getColumns(); // get the complete list of cols and the current col i.e. colList.get(0); ordering not assumed 
		int i = -1;
		while (columnCursor.next()) //to iterate over the cols in colList
		{
			i++;
			Type columnType = columnCursor.getMetaData().getType(); //get the type of the current col
			String name = columnCursor.getMetaData().getName(); //get the name of the current col
			
			//for each index(col) in columnCursor, find the mapping of that col in actual table from 'row' that has actual mapping
			//compare 'columnType' and 'name' to that of each column contained in 'row'
			for (int positionInTheRow = 0; positionInTheRow < row.getMetaData().getColumnCount(); positionInTheRow++)
			{
				if (row.getMetaData().getColumnMetaData(positionInTheRow).getName().equals(name) && columnType.equals(row.getMetaData().getColumnMetaData(positionInTheRow).getType()))
				{
					mapping_Array[i] = positionInTheRow;
					break;
				}
			}
		}
	}


	@Override
	public int addRow(Row row) throws SchemaMismatchException
	{

		if (mapping_Array == null || colCursor == null || !is_Bulk_Rows_Insertion)
		{
			findColMap(row);
			colCursor = (ColumnCursorImpl) getColumns();
		}
		int position = -1;
		if (deletedRows.size() > 0)
		{
			rowCounter = deletedRows.get(deletedRows.size() - 1);
			while (colCursor.next())
			{
				Type colType = colCursor.getMetaData().getType();
				position++;
				switch (colType)
				{
					case BOOLEAN:
						colCursor.set(rowCounter, row.getBoolean(mapping_Array[position]), false);
						break;
					case DATE:
						colCursor.set(rowCounter, row.getDate(mapping_Array[position]), false);
						break;
					case DOUBLE:
						colCursor.set(rowCounter, row.getDouble(mapping_Array[position]), false);
						break;
					case INTEGER:
						colCursor.set(rowCounter, row.getInteger(mapping_Array[position]), false);
						break;
					case STRING:
						colCursor.set(rowCounter, row.getString(mapping_Array[position]), false);
						break;
					default:
						break;
				}
			}
			RowImpl newRow = new RowImpl(rowCounter, this);
			deletedRows.removeElementAt(deletedRows.size() - 1);
			rowList.set(rowCounter, newRow);
		}
		else
		{   //for each col, store the value and go from left to right and then switch to the next row and do the same.
			while (colCursor.next())
			{
				Type columnType = colCursor.getMetaData().getType();
				position++; //position in the column for current row
				switch (columnType)
				{
					case BOOLEAN:
						colCursor.addBoolean(row.getBoolean(mapping_Array[position]), false);
						break;
					case DATE:
						colCursor.addDate(row.getDate(mapping_Array[position]));
						break;
					case DOUBLE:
						colCursor.addDouble(row.getDouble(mapping_Array[position]), false);
						break;
					case INTEGER:
						colCursor.addInteger(row.getInteger(mapping_Array[position]), false);
						break;
					case STRING:
						colCursor.addString(row.getString(mapping_Array[position])); //go to simpleRowCursor to getString on that position/index of mapping_array for particular table
						break;
					default:
						break;
				}
			}
			rowCounter++;
			RowImpl newRow = new RowImpl(rowCounter, this);
			rowList.add(newRow);
		}
		addRowToIndex(rowCounter);
		totalRowCounter++;
		return rowCounter;
	}


	private void addRowToIndex(int rowId)
	{

		for (Index index : NameToIndexMap.values())
		{
			switch (getIndexType(index))
			{
				case HASH:
					((HashIndex) index).addRowInIndex(rowId);
					break;
				case TREE:
					((TreeIndexBTreeImpl) index).addRowInIndex(rowId);
					break;
				case BITMAP:
					break;
				case OTHER:
					break;
				default:
					break;
			}
		}
	}

	@Override
	public IdCursor addRows(RowMetaData meta, RowCursor rowCur) throws SchemaMismatchException
	{
		/*{
		IdCursorImpl idCur = null;
		while (rowCur.next()){
				idCur = new IdCursorImpl();
				new TableMetaDataImpl();
		}
		
	
		return idCur;
	}
	}*/
		IdCursorImpl idCur = new IdCursorImpl();
		if (rowCur.next())
		{
			is_Bulk_Rows_Insertion = true;
			findColMap(rowCur);
			colCursor = (ColumnCursorImpl) getColumns();
			do
			{
				idCur.addID(addRow(rowCur)); //add row id to id_list
				colCursor.reset(); //go to position/col 0 for the next row
			} while (rowCur.next());
		}
		mapping_Array = null;
		is_Bulk_Rows_Insertion = false;
		return idCur;
		
	}


	@Override
	public int addColumn(Column col) throws SchemaMismatchException,
			ColumnAlreadyExistsException
	{
		if (columnsMap.containsKey(getColumnName(col)))
		{
			throw new ColumnAlreadyExistsException();
		}
		colCounter++;
		columnList.add((ColumnImpl) col);
		columnsMap.put(getColumnName(col), col.getMetaData());
		indexes.put(colCounter, new ArrayList<Index>());
		return colCounter;
	}

	@Override
	public IdCursor addColumns(ColumnCursor colCursor)
			throws SchemaMismatchException, ColumnAlreadyExistsException
	{
		IdCursorImpl idCursor = new IdCursorImpl();
		while (colCursor.next())
		{
			idCursor.addID(addColumn(colCursor));
		}
		return idCursor;
	}

	@Override
	public void deleteRow(int rowId) throws NoSuchRowException
	{
		if (rowId < rowList.size() && rowList.get(rowId) != null)
		{
			rowList.set(rowId, null);
			deletedRows.add(rowId);
		}
		else
		{
			throw new NoSuchRowException();
		}
		deleteRowInIndex(rowId);
	}

	private IndexType getIndexType(Index index)
	{
		return index.getIndexMetaInfo().getIndexType();
	}

	private void deleteRowInIndex(int rowId)
	{
		for (Index index : NameToIndexMap.values())
		{
			switch (getIndexType(index))
			{
				case HASH:
					Object key = getKey(rowId, index);
					((HashIndex) index).removeRowIdFromIndex(rowId, key);
					break;
				case TREE:
					((TreeIndexBTreeImpl) index).removeRowIdFromIndex(rowId);
					break;
				case BITMAP:
					break;
				case OTHER:
					break;
				default:
					break;
			}
		}
	}

	@Override
	public void deleteRows(IdCursor rowIds) throws NoSuchRowException
	{
		while (rowIds.next())
		{
			deleteRow(rowIds.getId());
		}
	}

	@Override
	public void dropColumn(int columnId) throws NoSuchColumnException
	{
		//remove the columns from columnMap and columnList
		boolean flag = false;
		for (int i = 0; i < columnList.size(); i++)
		{
			if (columnList.get(i).getMetaData().getId() == columnId)
			{
				flag = true;
				ColumnImpl c = columnList.get(i);
				columnsMap.remove(c.getMetaData().getName());
				columnList.remove(i);
				
				
				deleteIndexOfColumnGivenColID(columnId); //drop the column after dropping the index
				break;
			}
		}

		if (flag == false)
		{
			throw new NoSuchColumnException();
		}
	}

	@Override
	public void dropColumns(IdCursor colIds) throws NoSuchColumnException
	{
		while (colIds.next())
		{
			dropColumn(colIds.getId());
		}
	}


	private void deleteIndexOfColumnGivenColID(int colId)
	{
		ArrayList<Index> toDelete_List = (ArrayList<Index>) indexes.get(colId);
		for (Index ind : toDelete_List)
		{
			NameToIndexMap.remove(ind.getIndexMetaInfo().getName());
		}
		indexes.remove(colId);
	}

	
	@Override
	public Column getColumn(int columnId) throws NoSuchColumnException
	{
		for (int i = 0; i < columnList.size(); i++)
		{
			if (columnList.get(i).getMetaData().getId() == columnId)
			{
				return columnList.get(i);
			}
		}
		throw new NoSuchColumnException();
	}

	@Override
	public ColumnCursor getColumns(IdCursor columnIds)
			throws NoSuchColumnException
	{
		return null;
	}

	@Override
	public RowCursor getRows(IdCursor rowIds) throws NoSuchRowException //return the rowIdList,columnList, rowCounter in 'rowCur' for the test case
	{
		ArrayIntList idsList = new ArrayIntList(2);
		while (rowIds.next())
		{
			int currentId = rowIds.getId();
			if (getRow(currentId) != null)
				idsList.add(rowIds.getId());
		}
		RowCursor rowCur = new RowCursorImpl(columnList, idsList);
		return rowCur;
		
		/*ArrayIntList idsList = new ArrayIntList(2);
		while (rowIds.next())
		{
		idsList.add(rowIds.getId());
		}
		RowCursor rowCur = new RowCursorImpl(columnList, idsList);
		return rowCur;*/
	}


	@Override
	public Row getRow(int rowId) throws NoSuchRowException
	{
		if (rowId < rowList.size() && rowList.get(rowId) != null) //rowList is updated in addRow(Row row) when addRows(rows.getMetaData(), rows) is called from TestTableBuilder.
		{
			return rowList.get(rowId);
		}
		else
		{
			throw new NoSuchRowException();
		}
	}



	@Override
	public void updateRow(int rowId, Row newRow)
			throws SchemaMismatchException, NoSuchRowException
	{
		if (rowId >= rowList.size() || rowList.get(rowId) == null)
		{
			throw new NoSuchRowException();
		}
		else
			if (mapping_Array == null || colCursor == null || !is_Bulk_Rows_Insertion)
			{
				findColMap(newRow);
			}
		int columnId = -1;
		while (colCursor.next()) //update one complete row till last column and then jump to the next row
		{
			boolean shouldUpdate = false;
			ColumnMetaData columnMeta = colCursor.getMetaData(); //get the colMetaData for the current column
			columnId++;
			Object value = colCursor.getObject(rowId); //get the current value at current rowId
			switch (columnMeta.getType())
			{
				case BOOLEAN:
					boolean newBool = newRow.getBoolean(mapping_Array[columnId]);
					if (newBool != colCursor.getBoolean(rowId))
					{
						shouldUpdate = true;
					}
					colCursor.set(rowId, newBool, false);
					break;
				case DATE:
					Date newDate = newRow.getDate(mapping_Array[columnId]);
					if (newDate != null && !newDate.equals(colCursor.getDate(rowId)))
					{
						shouldUpdate = true;
					}
					colCursor.set(rowId, newDate, false);
					break;
				case DOUBLE:
					double newDouble = newRow.getDouble(mapping_Array[columnId]);
					if (newDouble != colCursor.getDouble(rowId))
					{
						shouldUpdate = true;
					}

					colCursor.set(rowId, newDouble, false);
					break;
				case INTEGER:
					int newInt = newRow.getInteger(mapping_Array[columnId]); //get the new integer value at current columnId
					if (newInt != colCursor.getInteger(rowId))
					{
						shouldUpdate = true;
					}
					colCursor.set(rowId, newInt, false);
					break;
				case STRING:
					String newString = newRow.getString(mapping_Array[columnId]);
					if (newString != null
							&& !newString.equals(colCursor.getString(rowId)))
					{
						shouldUpdate = true;
					}
					colCursor.set(rowId, newString, false);
					break;
				default:
					break;
			}
			if (shouldUpdate)
			{
				updateIndexNewRow(rowId, value, columnId);
			}
		}
	}

	private void updateIndexNewRow(int rowId, Object value, int columnId)
	{
		try
		{
			for (Index index : getIndexes(columnId))
			{

				switch (getIndexType(index))
				{
					case HASH:
						((HashIndex) index).updateRowInIndexes(rowId, value);
						break;
					case TREE:
						((TreeIndexBTreeImpl) index).updateRowInIndexes(rowId);
						break;
					case BITMAP:
						break;
					case OTHER:
						break;
					default:
						break;
				}
			}
		}
		catch (NoSuchColumnException e)
		{
			e.printStackTrace();
		}
	}

	private Object getKey(int rowId, Index index)
	{
		Object key;
		Column colu = index.getIndexMetaInfo().getKeyColumn();
		key = colu.getObject(rowId);
		return key;
	}

	public boolean compareColumns(Column col1, Column col2)
	{
		return col1.getMetaData().equals(col2.getMetaData());
	}

	@Override
	public void updateRows(RowMetaData meta, IdCursor rowIds, RowCursor newRows)
			throws SchemaMismatchException, NoSuchRowException
	{
		if (rowIds.next())
		{
			is_Bulk_Rows_Insertion = true;
			newRows.next();
			findColMap(newRows);
			colCursor = (ColumnCursorImpl) getColumns();
			do
			{
				updateRow(rowIds.getId(), newRows);
				colCursor.reset();
			} while (rowIds.next() && newRows.next());
		}
		mapping_Array = null;
		is_Bulk_Rows_Insertion = false;
	}

	@Override
	public void updateColumns(IdCursor colIds, ColumnCursor newCols)
			throws SchemaMismatchException, NoSuchColumnException
	{
		while (colIds.next())
		{
			if (newCols.next())
			{
				int deleteId = colIds.getId();
				updateColumn(deleteId, newCols);
			}
		}
	}

	public int colPosition(int columnId)
	{
		int position = 0;
		for (Column current : columnList)
		{
			if (current.getMetaData().getId() == columnId)
			{
				return position;
			}
			else
				position++;
		}
		return -1;
	}

	@Override
	public void updateColumn(int columnId, Column newCol)
			throws SchemaMismatchException, NoSuchColumnException
	{
		if (!columnExists(columnId))
			throw new NoSuchColumnException();
		Column oldColumn = getColumn(columnId);
		if (!compareColumns(oldColumn, newCol))
			throw new SchemaMismatchException();
		int insertPosition = colPosition(columnId);
		columnList.remove(insertPosition);
		columnList.add(insertPosition, (ColumnImpl) newCol);
	}

	public boolean columnExists(int columnId)
	{
		for (Column current : columnList)
		{
			if (current.getMetaData().getId() == columnId)
				return true;
		}
		return false;
	}

	@Override
	public TableMetaDataImpl getTableMetaData()
	{
		return tableMetaData;
	}


	@Override
	public int createIndex(String indexName, int keyColumnId, IndexType indexType) throws IndexAlreadyExistsException,
			NoSuchColumnException
	{

		if (indexes.get(keyColumnId).equals(MapMarker.NOT_FOUND))
		{
			throw new NoSuchColumnException();
		}
		if (NameToIndexMap.containsKey(indexName)) //NameToIndexMap--> hashmap of indexname and current index
		{
			throw new IndexAlreadyExistsException(); //handle duplicate indexes
		}
		indexId++;
		Index currentIndex = null;
		switch (indexType)
		{
			case HASH:
				currentIndex = new HashIndex(indexId, indexName, this, keyColumnId,
						indexType);
				break;
			case TREE:
				currentIndex = new TreeIndexBTreeImpl(indexId, indexName, this,
						keyColumnId, indexType);
				break;
			case BITMAP:
				break;
			case OTHER:
				break;
			default:
				break;
		}
		ArrayList<Index> list = (ArrayList<Index>) indexes.get(keyColumnId); //return type of indexes is ObjectHashMap... has getters and setters methods for indexes
		NameToIndexMap.put(indexName, currentIndex);
		list.add(currentIndex);
		return indexId;
	}

	@Override
	public void dropIndex(int indexId) throws NoSuchIndexException
	{
		Collection<Index> collectionOfIndexes = NameToIndexMap.values();
		boolean found = false;
		for (Index ind : collectionOfIndexes)
		{
			if (ind.getIndexMetaInfo().getId() == indexId)
			{
				found = true;
				break;
			}
		}
		if (!found)
		{
			throw new NoSuchIndexException();
		}
		else
		{
			int columnId = getColumnOnIndex(indexId);
			ArrayList<Index> indexList = (ArrayList<Index>) indexes.get(columnId);
			for (int i = 0; i < indexList.size(); i++)
			{
				Index currentIndex = indexList.get(i);
				if (currentIndex.getIndexMetaInfo().getId() == indexId)
				{
					NameToIndexMap.remove(currentIndex.getIndexMetaInfo().getName());
					indexList.remove(i);
					break;
				}
			}
		}
	}

	public int getColumnOnIndex(int indexId)
	{
		int[] columnIds = indexes.keySet();
		for (int i = 0; i < columnIds.length; i++)
		{
			@SuppressWarnings("unchecked")
			ArrayList<Index> indList = (ArrayList<Index>) indexes.get(columnIds[i]);
			for (int j = 0; j < indList.size(); j++)
			{
				Index in = indList.get(j);
				if (in.getIndexMetaInfo().getId() == indexId)
				{
					return columnIds[i];
				}
			}
		}
		return -1;
	}

	public int getIndexId(String indexName)
	{
		Collection<Index> indexes = getIndexes();
		for (Index ind : indexes)
		{
			if (ind.getIndexMetaInfo().getName().equalsIgnoreCase(indexName))
			{
				return ind.getIndexMetaInfo().getId();
			}
		}
		return -1;
	}


	@Override
	public Collection<Index> getIndexes(int keyColumnId)
			throws NoSuchColumnException
	{
		return (Collection<Index>) indexes.get(keyColumnId);

	}

	@Override
	public Collection<Index> getIndexes()
	{
		Collection<ArrayList<Index>> indexLists = indexes.values();
		ArrayList<Index> result = new ArrayList<>();
		Iterator indexIterator = indexLists.iterator();
		while (indexIterator.hasNext())
		{
			ArrayList currentIndexList = (ArrayList) indexIterator.next();
			for (int i = 0; i < currentIndexList.size(); i++)
			{
				result.add((Index) currentIndexList.get(i));
			}
		}
		return result;
	}

	@Override
	public Index getIndex(int indexId) throws NoSuchIndexException
	{
		Collection<Index> tempIndexes = NameToIndexMap.values();
		boolean found = false;
		Index foundIndex = null;
		for (Index ind : tempIndexes)
		{
			if (ind.getIndexMetaInfo().getId() == indexId)
			{
				foundIndex = ind;
				found = true;
				break;
			}
		}
		if (!found)
		{
			throw new NoSuchIndexException();
		}
		else
		{
			return foundIndex;
		}
	}

	public ArrayIntList getRowIds()
	{

		ArrayIntList ids = new ArrayIntList();
		for (int i = 0; i < rowList.size(); i++)
		{
			if (rowList.get(i) != null)
			{
				ids.add(i);
			}
		}
		return ids;
	}

	public int getTotalRows()
	{
		return totalRowCounter;
	}


	private String getColumnName(Column column)
	{
		return column.getMetaData().getName();
	}



}
