package dbs_project.query.impl;

import java.util.*;

import dbs_project.exceptions.SchemaMismatchException;
import dbs_project.storage.Column;
import dbs_project.storage.ColumnCursor;
import dbs_project.storage.Relation;
import dbs_project.storage.Row;
import dbs_project.storage.RowCursor;
import dbs_project.storage.Type;
import dbs_project.storage.impl.*;

/**
 * Created by Xedos2308 on 12.01.15.
 */
public class RelationImpl implements Relation
{


	private int currentCols;
	private ArrayList<ColumnImpl> colList;
	private int colMaps[];
	private int totalRowCounter;
	private ArrayList<dbs_project.query.impl.RelationRowImpl> rowList;
	private Map<String, ColumnImpl> colMap;

	public RelationImpl(List<Column> colList)
	{
		colMap = new HashMap<>(colList.size());
		this.colList = new ArrayList<>(colList.size());
		rowList = new ArrayList<>();
		currentCols = -1;
		totalRowCounter = 0;
		for (Column column : colList)
		{
			createColumn(column);
		}
	}

	public ColumnImpl getColumn(String columnName)
	{

		return colMap.get(columnName);
	}

	public String getColumnName(Column column)
	{

		return column.getMetaData().getName();
	}

	public Type getColumnType(Column column)
	{

		return column.getMetaData().getType();
	}

	public String getTableName(Column column)
	{
		return column.getMetaData().getSourceTable().getTableMetaData()
				.getName();
	}

	public TableImpl getTable(Column column)
	{
		return (TableImpl) column.getMetaData().getSourceTable();
	}

	public int getColumnCount(Row row)
	{

		return row.getMetaData().getColumnCount();
	}

	//create a new Column, of the outputRelation
	public void createColumn(Column column)
	{
		currentCols++;
		ColumnImpl columnImpl;
		switch (column.getMetaData().getType())
		{
			case BOOLEAN:
				columnImpl = new BoolColumnImpl(getColumnName(column),
						getColumnType(column), currentCols,
						getTableName(column), getTable(column));
				break;
			case DATE:
				columnImpl = new DateColumnImpl(getColumnName(column),
						getColumnType(column), currentCols,
						getTableName(column), getTable(column));
				break;
			case DOUBLE:
				columnImpl = new DoubleColumnImpl(getColumnName(column),
						getColumnType(column), currentCols,
						getTableName(column), getTable(column));
				break;
			case INTEGER:
				columnImpl = new IntColumnImpl(getColumnName(column),
						getColumnType(column), currentCols,
						getTableName(column), getTable(column));
				break;
			case STRING:
				columnImpl = new StringColumnImpl(getColumnName(column),
						getColumnType(column), currentCols,
						getTableName(column), getTable(column));
				break;
			default:
				columnImpl = new ColumnImpl();
				break;
		}
		colMap.put(getColumnName(column), columnImpl);
		colList.add(columnImpl);
	}


	/**
	 * Getter Functions
	 * @return
	 */
	@Override
	public RowCursor getRows()
	{
		return new RelationRowCursorImpl(colList, this);
	}

	@Override
	public ColumnCursor getColumns()
	{
		return new ColumnCursorImpl(colList);
	}

	public int getTotalRows()
	{
		return totalRowCounter;
	}

	public List<ColumnImpl> getColumnsAsList()
	{
		return colList;
	}

	public RelationRowImpl getRow(int rId)
	{
		return rowList.get(rId);
	}

	//searchting for colums on given columnames
	private void findMappingOfColFromColNames(List<String> columnNames)
			throws SchemaMismatchException
	{
		if (colMaps != null)
		{
			return;
		}

		if (getColumnsAsList().size() != columnNames.size())
		{
			throw new SchemaMismatchException();
		}

		//colMaps is an IntArray
		colMaps = new int[columnNames.size()];

		ColumnCursorImpl columnCursor = (ColumnCursorImpl) getColumns();

		int i = -1;
		while (columnCursor.next())
		{
			i++;
			String name = columnCursor.getMetaData().getName();
			for (int j = 0; j < columnNames.size(); j++)
			{
				//mapps the names to Cols
				if (columnNames.get(j).equals(name))
				{
					colMaps[i] = j;
					break;
				}
			}
		}
	}

	// find the Cols
	// insert the rows to relation
	public void addRowsFromQuery(List<String> columnNames,
			Iterator<List<Object>> dataForRows) throws SchemaMismatchException
	{
		findMappingOfColFromColNames(columnNames);
		while (dataForRows.hasNext())
		{
			totalRowCounter++;
			addRowFromQuery(dataForRows.next());
		}
		colMaps = null;
	}

	// add one Row from QueryResult to relation
	private void addRowFromQuery(List<Object> rowObject)
			throws SchemaMismatchException
	{
		ColumnCursorImpl colCursor = (ColumnCursorImpl) getColumns();

		int position = -1;
		while (colCursor.next())
		{
			Type columnType = colCursor.getMetaData().getType();
			position++;
			switch (columnType)
			{
				case BOOLEAN:
					colCursor.addBoolean(
							Boolean.parseBoolean(rowObject.get(
									colMaps[position]).toString()),
							false);
					break;
				case DATE:
					colCursor.addDate((Date) rowObject
							.get(colMaps[position]));
					break;
				case DOUBLE:
					colCursor.addDouble(
							Double.parseDouble(rowObject.get(
									colMaps[position]).toString()),
							false);
					break;
				case INTEGER:
					colCursor.addInteger(
							Integer.parseInt(rowObject.get(
									colMaps[position]).toString()),
							false);
					break;
				case STRING:
					colCursor.addString((String) rowObject
							.get(colMaps[position]));
					break;
				default:
					break;
			}
		}

		RelationRowImpl row = new RelationRowImpl(totalRowCounter, this);
		rowList.add(row);
	}

}
