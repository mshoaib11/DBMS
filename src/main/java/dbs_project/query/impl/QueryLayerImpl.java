package dbs_project.query.impl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import dbs_project.exceptions.*;
import dbs_project.storage.impl.ColumnImpl;
import dbs_project.storage.impl.IdCursorImpl;
import dbs_project.storage.impl.StorageLayerImpl;
import dbs_project.storage.impl.TableImpl;
import org.apache.commons.collections.primitives.ArrayIntList;
import dbs_project.index.Index;
import dbs_project.query.QueryLayer;
import dbs_project.query.predicate.Constant;
import dbs_project.query.predicate.Constant.ConstantType;
import dbs_project.query.predicate.Expression;
import dbs_project.query.predicate.ExpressionElement;
import dbs_project.query.predicate.Operator;
import dbs_project.query.statement.CreateColumnStatement;
import dbs_project.query.statement.CreateIndexStatement;
import dbs_project.query.statement.CreateTableStatement;
import dbs_project.query.statement.DeleteRowsStatement;
import dbs_project.query.statement.DropColumnStatement;
import dbs_project.query.statement.DropIndexStatement;
import dbs_project.query.statement.DropTableStatement;
import dbs_project.query.statement.InsertRowsStatement;
import dbs_project.query.statement.QueryStatement;
import dbs_project.query.statement.RenameColumnStatement;
import dbs_project.query.statement.RenameTableStatement;
import dbs_project.query.statement.UpdateRowsStatement;
import dbs_project.storage.Column;
import dbs_project.storage.Relation;
import dbs_project.storage.RowCursor;
import dbs_project.storage.Type;


/**
 * Created by Xedos2308 on 12.01.15.
 */
public class QueryLayerImpl implements QueryLayer
{

	protected StorageLayerImpl storageLayer;
	public QueryLayerImpl()
	{
       storageLayer = new StorageLayerImpl();
	}

	// select with * or not
	public boolean areAllColumnsRequired(List<String> requiredColNames)
	{
		return requiredColNames.size() == 1
				&& requiredColNames.get(0).endsWith("*");
	}

	//select with some columns, given ColNames and the whole Relation
	public List<Column> getRequiredColForRel(
			List<String> requiredColNames, RelationImpl relation)
	{
		List<Column> requiredColForRel = new ArrayList<>();
		for (int i = 0; i < requiredColNames.size(); i++)
		{
			requiredColForRel.add(relation
					.getColumn((requiredColNames.get(i))));
		}

		return requiredColForRel;
	}

    // same as the method before, given ColNames and the whole Table
	public List<Column> getRequiredColForRel(
			List<String> requiredColNames, TableImpl table)
	{
		List<Column> requiredColForRel = new ArrayList<>();
		for (int i = 0; i < requiredColNames.size(); i++)
		{
			try
			{
				requiredColForRel.add(table.getColumn(table
						.getColumnId(requiredColNames.get(i))));
			}
			catch (NoSuchColumnException e)
			{
				e.printStackTrace();
			}
		}

		return requiredColForRel;
	}

	//convert columnList in columnMap
	public Map<String, ColumnImpl> covertColListInColMap(
			List<ColumnImpl> columnList)
	{
		Map<String, ColumnImpl> columnMap = new HashMap<>();
		for (int i = 0; i < columnList.size(); i++)
		{
			columnMap.put(columnList.get(i).getColumnName(), columnList.get(i));
		}

		return columnMap;
	}

	//get RowData for interesting columns. given relation
	public List<Object> getRowData(List<String> columnNames,
			RelationImpl relation, int rowId)
	{
		List<Object> rowData = new ArrayList<>();
		for (int i = 0; i < columnNames.size(); i++)
		{
			rowData.add(relation.getColumn(columnNames.get(i)).getObject(
					(rowId)));
		}

		return rowData;
	}

	//get RowData for interesting columns. given table
	public List<Object> getRowData(List<String> columnNames, TableImpl table,
			int rowId)
	{
		List<Object> rowData = new ArrayList<>();
		for (int i = 0; i < columnNames.size(); i++)
		{
			try
			{
				rowData.add(table.getColumn(
						table.getColumnId(columnNames.get(i))).getObject(rowId));
			}
			catch (IndexOutOfBoundsException e)
			{
				e.printStackTrace();
			}
			catch (NoSuchColumnException e)
			{
				e.printStackTrace();
			}
		}

		return rowData;
	}

	public Type getColumnType(ColumnImpl column)
	{

		return column.getMetaData().getType();
	}

	@Override
	public Relation executeQuery(QueryStatement queryStatement)
			throws QueryExecutionException
	{
		List<String> tableNames = queryStatement.getTableNames();
		if (tableNames.size() == 0)
		{
			throw new QueryExecutionException();
		}

		try
		{
			// if more than one table required
			if (tableNames.size() > 1)
			{
				RelationImpl temporaryRel = getRelation(queryStatement);
				List<String> requiredColNames = queryStatement.getColumnNames();

				if (areAllColumnsRequired(requiredColNames))
				{
					requiredColNames = new ArrayList<>();
					// create ColumnList with all Columns
					List<ColumnImpl> columnList = temporaryRel.getColumnsAsList();
					for (ColumnImpl col : columnList)
					{
						//create List with all ColumnNames
						requiredColNames.add(col.getColumnName());
					}
				}

				List<Column> requiredColForRel = getRequiredColForRel(requiredColNames, temporaryRel);

				//create new Relation only with required Columns
				RelationImpl rel = new RelationImpl(requiredColForRel);
				List<List<Object>> data = new ArrayList<>();

				//iterate through the data, each row
				if (queryStatement.getPredicate() == null)
				{
					RelationRowCursorImpl rc = (RelationRowCursorImpl) temporaryRel.getRows();
					while (rc.next())
					{
						List<Object> rowData = new ArrayList<>();
						for (int i = 0; i < requiredColNames.size(); i++)
						{
							rowData.add(temporaryRel.getColumn(
									requiredColNames.get(i)).getObject(
									(rc.getMetaData().getId() - 1)));
						}
						data.add(rowData);
					}
				}
				else

				//iterate through the data, with given Predicate
				{
					//should be clear here
					List<ColumnImpl> columnList = temporaryRel.getColumnsAsList();
					Map<String, ColumnImpl> columnMap = covertColListInColMap(columnList);
					VisitorImpl tempVisitorImpl = new VisitorImpl(columnMap);

					//Visitor for queryStatement
					queryStatement.getPredicate().accept(tempVisitorImpl);
					DynamicPredicate predicate;
					predicate = tempVisitorImpl.createPredicate();
					RelationRowCursorImpl rowCursor = (RelationRowCursorImpl) temporaryRel
							.getRows();

					while (rowCursor.next())
					{
						int rowId = rowCursor.getMetaData().getId() - 1;
						if (predicate.eval(rowId))
						{
							List<Object> rowData = getRowData(
									requiredColNames, temporaryRel, rowId);
							data.add(rowData);
						}
					}
				}

				//create relation with required Rows of a Query
				rel.addRowsFromQuery(requiredColNames, data.iterator());
				return rel;
			}
			else

			// if just one table required
			{
				List<String> requiredColNames = queryStatement.getColumnNames();
				TableImpl table = storageLayer.getTable(tableNames.get(0));
				//should be clear
				if (areAllColumnsRequired(requiredColNames))
				{
					requiredColNames = new ArrayList<>();
					List<ColumnImpl> colList = table.getColumnsList();
					for (ColumnImpl column : colList)
					{
						requiredColNames.add(column.getColumnName());
					}
				}
				// create List for Columns which are required from  only one Table
				List<Column> requiredColumnsForRelation = getRequiredColForRel(
						requiredColNames, table);

				RelationImpl rel = new RelationImpl(requiredColumnsForRelation);
				List<List<Object>> data = new ArrayList<>();

				//Predicate = null?? create RowData
				if (queryStatement.getPredicate() == null)
				{
					RowCursor rowCursor = table.getRows();
					while (rowCursor.next())
					{
						List<Object> rowData = new ArrayList<>();
						for (int i = 0; i < requiredColNames.size(); i++)
						{
							rowData.add(table.getColumn(
									table.getColumnId(requiredColNames
											.get(i))).getObject(
									(rowCursor.getMetaData().getId())));
						}
						data.add(rowData);
					}
				}
				else
				//Predicate not null, create RowData
				//check the Predicate-Value
				{
					Expression express = (Expression) queryStatement.getPredicate();

					if (isEqualOperator(express.getOperator()))
					{
						Constant left = (Constant) express.getOperand(0);
						Constant right = (Constant) express.getOperand(1);

						if (isConstantTypeColumnName(left.getType()))
						{
							ColumnImpl column = (ColumnImpl) table
									.getColumn(table.getColumnId(left
											.getValue()));
							Collection<Index> indexes = table.getIndexes(column
									.getMetaData().getId());
							for (Index index : indexes)
							{
								Object value = right.getValue();
								switch (getColumnType(column))
								{
									case BOOLEAN:
										value = Boolean.parseBoolean(right
												.getValue());
										break;
									case DOUBLE:
										value = Double.parseDouble(right
												.getValue());
										break;
									case INTEGER:
										value = Integer.parseInt(right
												.getValue());
										break;
									case DATE:
										break;
									case STRING:
										break;
									default:
										break;
								}

								RowCursor rowCursor = index.pointQuery(value);
								while (rowCursor.next())
								{
									int rowId = rowCursor.getMetaData().getId();
									List<Object> rowData = getRowData(
											requiredColNames, table, rowId);
									data.add(rowData);
								}
								rel.addRowsFromQuery(requiredColNames,
										data.iterator());
								return rel;
							}
						}
					}

					List<ColumnImpl> colList = table.getColumnsList();
					Map<String, ColumnImpl> colMap = new HashMap<>();

					for (int i = 0; i < colList.size(); i++)
					{
						colMap.put(colList.get(i).getColumnName(),
								colList.get(i));
					}

					VisitorImpl tvI = new VisitorImpl(colMap);
					queryStatement.getPredicate().accept(tvI);
					DynamicPredicate predicate;
					predicate = tvI.createPredicate();
					RowCursor rowCursor = table.getRows();

					// adding the qualifying rows that are required to be
					// returned to the list
					while (rowCursor.next())
					{
						int rowId = rowCursor.getMetaData().getId();
						if (predicate.eval(rowId))
						{
							List<Object> rowData = getRowData(
									requiredColNames, table, rowId);
							data.add(rowData);
						}
					}
				}
				rel.addRowsFromQuery(requiredColNames, data.iterator());
				return rel;
			}
		}
		catch (SchemaMismatchException | IndexOutOfBoundsException
				| NoSuchColumnException e)
		{
			throw new QueryExecutionException();
		}
		catch (Exception e)
		{
			throw new QueryExecutionException();
		}
	}

	public static QueryStatement buildQueryStatement(
			final List<String> tableNames, final List<String> columnNames,
			final ExpressionElement predicate)
	{
		return new QueryStatement()
		{
			@Override
			public ExpressionElement getPredicate()
			{
				return predicate;
			}

			@Override
			public List<String> getColumnNames()
			{
				return columnNames;
			}

			@Override
			public List<String> getTableNames()
			{
				return tableNames;
			}
		};
	}

	public static QueryStatement buildQueryStatement(
			final List<String> columnNames, final ExpressionElement predicate,
			String... tableName)
	{
		return buildQueryStatement(Arrays.asList(tableName), columnNames,
				predicate);
	}

	private RelationImpl getIntermediateResult(Expression exp,
			List<String> tableNames, List<String> columnNames)
	{
		try
		{
			if (!isBooleanOperator(exp.getOperator()))
			{
				ExpressionElement left = exp.getOperand(0);
				ExpressionElement right = exp.getOperand(1);
				if (left instanceof Constant && right instanceof Constant)
				{
					Constant leftConstant = (Constant) left;
					Constant rightConstant = (Constant) right;
					if (isConstantTypeColumnName(leftConstant.getType())
							&& isConstantTypeColumnName(rightConstant.getType()))
					{
						// table for left relation
						TableImpl leftTable = storageLayer
								.getTableNameFromColumnName(leftConstant
										.getValue());
						// required columns for left relation
						List<String> leftRelationRequiredColumnNames = new ArrayList<>();
						leftRelationRequiredColumnNames.add(leftConstant
								.getValue());
						for (String columnName : columnNames)
						{
							if (leftTable.getColumnId(columnName) != -1)
							{
								leftRelationRequiredColumnNames.add(columnName);
							}
						}

						QueryStatement leftStatement = QueryLayerImpl
								.buildQueryStatement(
										leftRelationRequiredColumnNames, null,
										leftTable.getTableName());

						// table for right relation
						TableImpl rightTable = storageLayer
								.getTableNameFromColumnName(rightConstant
										.getValue());
						// required columns for right relation
						List<String> rightRelationRequiredColumnNames = new ArrayList<>();
						rightRelationRequiredColumnNames.add(rightConstant
								.getValue());
						for (String colName : columnNames)
						{
							if (rightTable.getColumnId(colName) != -1)
							{
								rightRelationRequiredColumnNames.add(colName);
							}
						}

						QueryStatement rightStatement = QueryLayerImpl
								.buildQueryStatement(
										rightRelationRequiredColumnNames, null,
										rightTable.getTableName());

						RelationImpl tempLeftRelation;
						RelationImpl tempRightRelation;
						tempLeftRelation = (RelationImpl) executeQuery(leftStatement);
						tempRightRelation = (RelationImpl) executeQuery(rightStatement);

						return joinRelations(tempLeftRelation,
								tempRightRelation, leftConstant.getValue(),
								rightConstant.getValue());
					}
					else
						if (isConstantTypeColumnName(leftConstant.getType())
								&& isConstantTypeValueLiteral(rightConstant
										.getType()))
						{
							return null;
						}
						else
							if (isConstantTypeValueLiteral(leftConstant
									.getType())
									&& isConstantTypeColumnName(rightConstant
											.getType()))
							{
								return null;
							}
							else
							{
								throw new RuntimeException(
										""
												+ leftConstant.getType()
												+ rightConstant.getType());
							}
				}
			}
			else
			{
				Expression rightSide = (Expression) exp.getOperand(1);

				List<String> leftSideNewColumnNames = new ArrayList<>();
				if (!isBooleanOperator(rightSide.getOperator()))
				{
					if (isConstantTypeColumnName(((Constant) rightSide
							.getOperand(0)).getType()))
					{
						leftSideNewColumnNames
								.add(((Constant) rightSide.getOperand(0))
										.getValue());
					}
					if (isConstantTypeColumnName(((Constant) rightSide
							.getOperand(1)).getType()))
					{
						leftSideNewColumnNames
								.add(((Constant) rightSide.getOperand(1))
										.getValue());
					}
				}
				for (int i = 0; i < columnNames.size(); i++)
				{
					leftSideNewColumnNames.add(columnNames.get(i));
				}
				RelationImpl leftRel = getIntermediateResult(
						(Expression) exp.getOperand(0), tableNames,
						leftSideNewColumnNames);


				Expression leftSide = (Expression) exp.getOperand(0);

				List<String> rightSideNewColumnNames = new ArrayList<>();
				if (!isBooleanOperator(leftSide.getOperator()))
				{
					if (isConstantTypeColumnName(((Constant) leftSide
							.getOperand(0)).getType()))
					{
						rightSideNewColumnNames
								.add(((Constant) leftSide.getOperand(0))
										.getValue());
					}
					if (isConstantTypeColumnName(((Constant) leftSide
							.getOperand(1)).getType()))
					{
						rightSideNewColumnNames
								.add(((Constant) leftSide.getOperand(1))
										.getValue());
					}
				}
				for (int i = 0; i < columnNames.size(); i++)
				{
					rightSideNewColumnNames.add(columnNames.get(i));
				}

				RelationImpl rightRel = getIntermediateResult(rightSide,
						tableNames, rightSideNewColumnNames);

				if (leftRel == null)
				{
					return rightRel;
				}
				if (rightRel == null)
				{
					return leftRel;
				}

				List<ColumnImpl> leftTableCols = leftRel.getColumnsAsList();
				List<ColumnImpl> rightTableCols = rightRel
						.getColumnsAsList();

				HashMap<String, String> predicateMap;
				String keyColumn = null;

				if (leftTableCols.size() < rightTableCols.size())
				{
					predicateMap = new HashMap<>(leftTableCols.size());
					for (int i = 0; i < leftTableCols.size(); i++)
					{
						String columnName = leftTableCols.get(i)
								.getColumnName();
						predicateMap.put(columnName, columnName);
					}
					for (int i = 0; i < rightTableCols.size(); i++)
					{
						String name = rightTableCols.get(i).getColumnName();
						if (predicateMap.containsKey(name))
						{
							keyColumn = name;
							break;
						}
					}
				}
				else
				{
					predicateMap = new HashMap<>(rightTableCols.size());
					for (int i = 0; i < rightTableCols.size(); i++)
					{
						String columnName = rightTableCols.get(i)
								.getColumnName();
						predicateMap.put(columnName, columnName);
					}
					for (int i = 0; i < leftTableCols.size(); i++)
					{
						String name = leftTableCols.get(i).getColumnName();
						if (predicateMap.containsKey(name))
						{
							keyColumn = name;
							break;
						}
					}
				}

				return joinRelations(leftRel, rightRel, keyColumn, keyColumn);
			}
		}
		catch (QueryExecutionException e)
		{
			e.printStackTrace();
		}
		return null;
	}

	private RelationImpl joinRelations(RelationImpl leftRelation,
			RelationImpl rightRelation, String leftColumnName,
			String rightColumnName)
	{
		try
		{

			List<ColumnImpl> leftRelationColumns = leftRelation
					.getColumnsAsList();
			List<ColumnImpl> rightRelationColumns = rightRelation
					.getColumnsAsList();

			List<Column> columnList = new ArrayList<>(
					leftRelationColumns.size() + rightRelationColumns.size());
			List<String> requiredColumnNames = new ArrayList<>(
					leftRelationColumns.size() + rightRelationColumns.size());

			for (int i = 0; i < leftRelationColumns.size(); i++)
			{
				String tempColumnName = leftRelationColumns.get(i)
						.getColumnName();
				if (!(tempColumnName.equalsIgnoreCase(leftColumnName) && tempColumnName.equalsIgnoreCase(rightColumnName)))
				{
					columnList.add(leftRelationColumns.get(i));
					requiredColumnNames.add(leftRelationColumns.get(i)
							.getColumnName());
				}
			}

			for (int i = 0; i < rightRelationColumns.size(); i++)
			{
				columnList.add(rightRelationColumns.get(i));
				requiredColumnNames.add(rightRelationColumns.get(i).getColumnName());
			}

			RelationImpl resultRelation = new RelationImpl(columnList);
			Map<Object, ArrayIntList> hashTable = new HashMap<>();
			ColumnImpl leftColumn = leftRelation.getColumn(leftColumnName);
			ColumnImpl rightColumn = rightRelation.getColumn(rightColumnName);
			RelationRowCursorImpl leftRowCursor = (RelationRowCursorImpl) leftRelation.getRows();
			RelationRowCursorImpl rightRowCursor = (RelationRowCursorImpl) rightRelation.getRows();

			while (leftRowCursor.next())
			{
				int rowId = leftRowCursor.getMetaData().getId() - 1;
				if (!hashTable.containsKey(leftColumn.getObject(rowId)))
				{
					ArrayIntList rowIds = new ArrayIntList();
					rowIds.add(rowId);
					hashTable.put(leftColumn.getObject(rowId), rowIds);
				}
				else
				{
					ArrayIntList rowIds = hashTable.get(leftColumn.getObject(rowId));
					rowIds.add(rowId);
				}
			}

			List<List<Object>> data = new ArrayList<>();
			while (rightRowCursor.next())
			{
				int rowId = rightRowCursor.getMetaData().getId() - 1;
				if (hashTable.containsKey(rightColumn.getObject(rowId)))
				{
					ArrayIntList values = hashTable.get(rightColumn
							.getObject(rowId));
					for (int i = 0; i < values.size(); i++)
					{
						RelationRowImpl leftRow = leftRelation.getRow(values
								.get(i));
						RelationRowImpl rightRow = rightRelation.getRow(rowId);

						List<Object> rowData = new ArrayList<>();
						for (int j = 0; j < leftRow.getMetaData()
								.getColumnCount(); j++)
						{
							if (!(leftRelationColumns.get(j).getColumnName()
									.equalsIgnoreCase(leftColumnName) && leftRelationColumns
									.get(j).getColumnName()
									.equalsIgnoreCase(rightColumnName)))
							{
								rowData.add(leftRow.getObject(j));
							}
						}

						for (int k = 0; k < rightRow.getMetaData()
								.getColumnCount(); k++)
						{
							rowData.add(rightRow.getObject(k));
						}

						data.add(rowData);
					}
				}
			}

			resultRelation.addRowsFromQuery(requiredColumnNames,
					data.iterator());

			return resultRelation;
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return null;
	}

	private static boolean isBooleanOperator(Operator op)
	{
		return op == Operator.AND || op == Operator.OR;
	}

	private static boolean isEqualOperator(Operator op)
	{
		return op == Operator.EQ;
	}

	private RelationImpl getRelation(QueryStatement queryStmnt)
	{
		return getIntermediateResult((Expression) queryStmnt.getPredicate(),
				queryStmnt.getTableNames(), queryStmnt.getColumnNames());
	}

	@Override
	public int executeUpdateRows(UpdateRowsStatement updateStatement)
			throws QueryExecutionException
	{
		TableImpl table = storageLayer.getTable(updateStatement.getTableName());

		List<ColumnImpl> columnList = table.getColumnsList();
		Map<String, ColumnImpl> columnMap = new HashMap<>();
		for (int i = 0; i < columnList.size(); i++)
		{
			columnMap.put(columnList.get(i).getColumnName(), columnList.get(i));
		}

		VisitorImpl tempVisitorImpl = new VisitorImpl(columnMap);
		updateStatement.getPredicate().accept(tempVisitorImpl);
		DynamicPredicate predicate;



		try
		{
			predicate = tempVisitorImpl.createPredicate();
		}
		catch (Exception e)
		{
			throw new QueryExecutionException();
		}

		RowCursor rowCursor = table.getRows();
		List<String> columnNamesToUpdate = updateStatement.getColumnNames();

		int updatedCount = 0;

		while (rowCursor.next())
		{
			int rowId = rowCursor.getMetaData().getId();
			if (predicate.eval(rowId))
			{
				updatedCount++;
				for (final ColumnImpl referenceColumn : columnList)
				{
					for (int i = 0; i < columnNamesToUpdate.size(); i++)
					{
						if (columnNamesToUpdate.get(i).equals(
								referenceColumn.getMetaData().getName()))
						{
							referenceColumn.set(rowId, updateStatement
									.getUpdateRowData().get(i), false);
						}
					}
				}
			}
		}

		return updatedCount;
	}

	@Override
	public int executeDeleteRows(DeleteRowsStatement deleteStatement)
			throws QueryExecutionException
	{

		TableImpl table = storageLayer.getTable(deleteStatement.getTableName());
		List<ColumnImpl> columnList = table.getColumnsList();

		Map<String, ColumnImpl> columnMap = covertColListInColMap(columnList);

		VisitorImpl tempVisitorImpl = new VisitorImpl(columnMap);
		deleteStatement.getPredicate().accept(tempVisitorImpl);

		DynamicPredicate predicate;
		try
		{
			predicate = tempVisitorImpl.createPredicate();
		}
		catch (Exception e)
		{
			throw new QueryExecutionException();
		}

		RowCursor rowCursor = table.getRows();
		IdCursorImpl idCursor = new IdCursorImpl();
		int deletedCount = 0;

		while (rowCursor.next())
		{
			int rowId = rowCursor.getMetaData().getId();
			if (predicate.eval(rowId))
			{
				idCursor.addID(rowId);
				deletedCount++;
			}
		}

		try
		{

			table.deleteRows(idCursor);
		}
		catch (NoSuchRowException e)
		{
			throw new QueryExecutionException();
		}

		return deletedCount;
	}

	@Override
	public void executeInsertRows(InsertRowsStatement insertStatement)
			throws QueryExecutionException
	{
		try
		{
			TableImpl table = storageLayer.getTable(insertStatement
					.getTableName());


			table.addRowsInQuery(insertStatement.getColumnNames(),
					insertStatement.getDataForRows());
		}
		catch (SchemaMismatchException e)
		{
			throw new QueryExecutionException();
		}
	}

	@Override
	public void createTable(CreateTableStatement createTableStatement)
			throws QueryExecutionException
	{
		try
		{
			HashMap<String, Type> map = new HashMap<>();
			List<String> columnNames = createTableStatement.getColumnNames();
			List<Type> columnTypes = createTableStatement.getColumnTypes();


			for (int i = 0; i < columnNames.size(); i++)
			{
				map.put(columnNames.get(i), columnTypes.get(i));
			}

			storageLayer.createTable(createTableStatement.getTableName(), map);
		}
		catch (TableAlreadyExistsException  e)
		{
			throw new QueryExecutionException();
		}
	}

	@Override
	public void createColumn(CreateColumnStatement createColumnStatement)
			throws QueryExecutionException
	{
		TableImpl table = storageLayer.getTable(createColumnStatement
				.getTableName());



		try
		{
			table.createColumn(createColumnStatement.getColumnName(),
					createColumnStatement.getColumnType());
		}
		catch (ColumnAlreadyExistsException e)
		{
			throw new QueryExecutionException();
		}
	}

	@Override
	public void createIndex(CreateIndexStatement createIndexStatement)
			throws QueryExecutionException
	{
		TableImpl table = storageLayer.getTable(createIndexStatement
				.getTableName());
		try
		{


			table.createIndex(createIndexStatement.getIndexName(),
					table.getColumnId(createIndexStatement.getColumnName()),
					createIndexStatement.getIndexType());
		}
		catch (IndexAlreadyExistsException e)
		{
			throw new QueryExecutionException();
		}
		catch (NoSuchColumnException e)
		{
			throw new QueryExecutionException();
		}

		}



	@Override
	public void dropTable(DropTableStatement dropTableStatement)
			throws QueryExecutionException
	{
		try
		{

			String tableName = dropTableStatement.getTableName();
			TableImpl t = storageLayer.getTable(tableName);
			storageLayer.deleteTable(t.getTableMetaData().getId());
			return;
		}
		catch (NoSuchTableException e)
		{
			throw new QueryExecutionException();
		}
	}

	@Override
	public void dropColumn(DropColumnStatement dropColumnStatement)
			throws QueryExecutionException
	{
		TableImpl table = storageLayer.getTable(dropColumnStatement
				.getTableName());


		int columnId = table.getColumnId(dropColumnStatement.getColumnName());
		try
		{
			table.dropColumn(columnId);
		}
		catch (NoSuchColumnException e)
		{
			throw new QueryExecutionException();
		}
	}

	@Override
	public void dropIndex(DropIndexStatement dropIndexStatement)
			throws QueryExecutionException
	{
		try
		{
			TableImpl table = storageLayer.getTable(dropIndexStatement
					.getTableName());


			table.dropIndex(table.getIndexId(dropIndexStatement.getIndexName()));
		}
		catch (NoSuchIndexException e)
		{
			throw new QueryExecutionException();
		}
	}

	@Override
	public void renameTable(RenameTableStatement renameTableStatement)
			throws QueryExecutionException
	{
		String tableName = renameTableStatement.getTableName();
		TableImpl table = storageLayer.getTable(tableName);


		try
		{
			storageLayer.renameTable(table.getTableMetaData().getId(),
					renameTableStatement.getNewTableName());
			return;
		}
		catch (TableAlreadyExistsException e)
		{
			throw new QueryExecutionException();
		}
		catch (NoSuchTableException e)
		{
			throw new QueryExecutionException();
		}
	}

	@Override
	public void renameColumn(RenameColumnStatement renameColumnStatement)
			throws QueryExecutionException
	{
		TableImpl table = storageLayer.getTable(renameColumnStatement
				.getTableName());



		try
		{
			int columnId = table.getColumnId(renameColumnStatement
					.getColumnName());
			table.renameColumn(columnId,
					renameColumnStatement.getNewColumnName());
		}
		catch (NoSuchColumnException e)
		{
			throw new QueryExecutionException();
		}
		catch (ColumnAlreadyExistsException e)
		{
			throw new QueryExecutionException();
		}
	}

	public boolean isConstantTypeColumnName(ConstantType type)
	{
		return type == ConstantType.COLUMN_NAME;
	}

	public boolean isConstantTypeValueLiteral(ConstantType type)
	{
		return type == ConstantType.VALUE_LITERAL;
	}


}
