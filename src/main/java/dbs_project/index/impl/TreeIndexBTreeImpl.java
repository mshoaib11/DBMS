package dbs_project.index.impl;

import java.util.Date;
import dbs_project.storage.impl.ColumnImpl;
import dbs_project.storage.impl.IdCursorImpl;
import dbs_project.storage.impl.RowCursorImpl;
import dbs_project.storage.impl.TableImpl;
import org.apache.commons.collections.primitives.ArrayIntList;

import dbs_project.exceptions.InvalidKeyException;
import dbs_project.exceptions.InvalidRangeException;
import dbs_project.exceptions.RangeQueryNotSupportedException;
import dbs_project.index.Index;
import dbs_project.index.IndexMetaInfo;
import dbs_project.index.IndexType;
import dbs_project.storage.Column;
import dbs_project.storage.RowCursor;
import dbs_project.storage.Type;
import dbs_project.util.IdCursor;

/**
 * Created by Xedos2308 on 29.11.14.
 */
public class TreeIndexBTreeImpl implements Index {

	private IndexMetaInfoImpl indexMetaInfo;
	private BTree bTreeMap;

	private static final String INT_CLASS = "java.lang.Integer";
	private static final String DOUBLE_CLASS = "java.lang.Double";
	private static final String BOOLEAN_CLASS = "java.lang.Boolean";

	public TreeIndexBTreeImpl(int indexId, String indexName, TableImpl table,
			int keyColumnId, IndexType indexType) {
		indexMetaInfo = new IndexMetaInfoImpl(indexId, indexName, table,
				keyColumnId, indexType);
		createIndexFromScratch();
	}

	//getter functions

	public ColumnImpl getKeyColumn() {
		return (ColumnImpl) getIndexMetaInfo().getKeyColumn();
	}

	public ArrayIntList getRowIds() {
		return ((TableImpl) getIndexMetaInfo().getTable()).getRowIds();
	}

	public Type getColumnType(ColumnImpl column) {
		return column.getMetaData().getType();
	}

	public Type getColumnType(Column column) {
		return column.getMetaData().getType();
	}

	//create a new Index on btree, like in hashindex
	public void createIndexFromScratch() {
		bTreeMap = null;
		bTreeMap = new BTree(20, 20);
		ColumnImpl columnToIndex = getKeyColumn();
		ArrayIntList rowIds = getRowIds();
		
		for (int currentRowId = 0; currentRowId < rowIds.size(); currentRowId++) {
			Object key = columnToIndex.getObject(currentRowId);
			addEntryInIndexesMap(key, currentRowId,
					getColumnType(columnToIndex));
		}
	}

	// see HashIndex
	public void addEntryInIndexesMap(Object key, int rowId, Type type) {
		key = getNullType(key, type);
		bTreeMap.insert(key, rowId);
	}

	//see HashIndex
	public void updateRowInIndexes(int rowId) {
		removeRowIdFromIndex(rowId);
		ColumnImpl columnToIndex = getKeyColumn();
		Object key = columnToIndex.getObject(rowId);
		addEntryInIndexesMap(key, rowId, getColumnType(columnToIndex));
	}

	//see Hashindex
	public void removeRowIdFromIndex(int rowId) {
		bTreeMap.deleteValue(rowId);
	}

	//see Hashindex
	@Override
	public RowCursor pointQuery(Object searchKey) throws InvalidKeyException {
		searchKey = getNullType(searchKey,
				getColumnType(indexMetaInfo.getKeyColumn()));

		searchKey = getNullType(searchKey,
				getColumnType(indexMetaInfo.getKeyColumn()));

		if (searchKey.getClass() != getColumnType(indexMetaInfo.getKeyColumn())
				.getJavaClass()) {
			if (searchKey.getClass().equals(INT_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.INTEGER) {
				throw new InvalidKeyException();
			} else if (searchKey.getClass().equals(DOUBLE_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.DOUBLE) {
				throw new InvalidKeyException();
			} else if (searchKey.getClass().equals(BOOLEAN_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.BOOLEAN) {
				throw new InvalidKeyException();
			}
		}

		ArrayIntList rowIds = new ArrayIntList();
		ArrayIntList tempList = bTreeMap.pointQuery(searchKey);
		if (tempList != null) {
			rowIds.addAll(tempList);
		}
		
		return new RowCursorImpl(
				((TableImpl) (indexMetaInfo.getTable())).getColumnsList(),
				rowIds);
	}

	//see Hashindex

	private Object getNullType(Object key, Type t) {
		if (key == null) {
			switch (t) {
			case BOOLEAN:
				key = Type.NULL_VALUE_BOOLEAN;
				break;
			case DATE:
				key = new Date(0);
				break;
			case DOUBLE:
				key = Type.NULL_VALUE_DOUBLE;
				break;
			case INTEGER:
				key = Type.NULL_VALUE_INTEGER;
				break;
			case STRING:
				key = "0";
				break;
			default:
				break;
			}
		}
		return key;
	}

	//rangeQuery is supported here
	@Override
	public RowCursor rangeQuery(Object startSearchKey, Object endSearchKey,
			boolean isStartKeyIncluded, boolean isEndKeyIncluded)
			throws InvalidRangeException, InvalidKeyException,
			RangeQueryNotSupportedException {

		// check the startSearchkey
		startSearchKey = getNullType(startSearchKey,
				getColumnType(indexMetaInfo.getKeyColumn()));

		if (startSearchKey.getClass() != getColumnType(
				indexMetaInfo.getKeyColumn()).getJavaClass()) {
			if (startSearchKey.getClass().equals(INT_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.INTEGER) {
				throw new InvalidKeyException();
			} else if (startSearchKey.getClass().equals(DOUBLE_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.DOUBLE) {
				throw new InvalidKeyException();
			} else if (startSearchKey.getClass().equals(BOOLEAN_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.BOOLEAN) {
				throw new InvalidKeyException();
			}
		}

		//check the Endsearchkey
		endSearchKey = getNullType(endSearchKey,
				getColumnType(indexMetaInfo.getKeyColumn()));

		if (endSearchKey.getClass() != getColumnType(
				indexMetaInfo.getKeyColumn()).getJavaClass()) {
			if (endSearchKey.getClass().equals(INT_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.INTEGER) {
				throw new InvalidKeyException();
			} else if (endSearchKey.getClass().equals(DOUBLE_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.DOUBLE) {
				throw new InvalidKeyException();
			} else if (endSearchKey.getClass().equals(BOOLEAN_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.BOOLEAN) {
				throw new InvalidKeyException();
			}
		}

		switch (getColumnType(indexMetaInfo.getKeyColumn())) {
		case BOOLEAN:
			if (((boolean) startSearchKey) == true
					&& ((boolean) endSearchKey) == false) {
				throw new InvalidRangeException();
			}
			break;
		case DATE:
			if (((Date) startSearchKey).after((Date) endSearchKey)) {
				throw new InvalidRangeException();
			}
			break;
		case DOUBLE:
			if (((double) startSearchKey) > ((double) endSearchKey)) {
				throw new InvalidRangeException();
			}
			break;
		case INTEGER:
			if (((int) startSearchKey) > ((int) endSearchKey)) {
				throw new InvalidRangeException();
			}
			break;
		case STRING:
			if (((String) startSearchKey).compareTo((String) endSearchKey) > 0) {
				throw new InvalidRangeException();
			}
			break;
		default:
			break;
		}

		// rowIds in the range
		ArrayIntList rowIds = bTreeMap.rangeQuery(startSearchKey, endSearchKey,
				isStartKeyIncluded, isEndKeyIncluded);

		//return columnlist with rowIds
		RowCursorImpl rowCursor = new RowCursorImpl(
				((TableImpl) (indexMetaInfo.getTable())).getColumnsList(),
				rowIds); 

		return rowCursor;
	}

	// like in HashIndex
	@Override
	public IdCursor pointQueryRowIds(Object searchKey)
			throws InvalidKeyException {
		searchKey = getNullType(searchKey,
				getColumnType(indexMetaInfo.getKeyColumn()));

		if (searchKey.getClass() != getColumnType(indexMetaInfo.getKeyColumn())
				.getJavaClass()) {
			if (searchKey.getClass().equals(INT_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.INTEGER) {
				throw new InvalidKeyException();
			} else if (searchKey.getClass().equals(DOUBLE_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.DOUBLE) {
				throw new InvalidKeyException();
			} else if (searchKey.getClass().equals(BOOLEAN_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.BOOLEAN) {
				throw new InvalidKeyException();
			}
		}

		IdCursorImpl idCursor = new IdCursorImpl();
		ArrayIntList rowIds = bTreeMap.pointQuery(searchKey);
		if (rowIds != null) {
			for (int i = 0; i < rowIds.size(); i++) {
				idCursor.addID(rowIds.get(i));
			}
		}
		
		return idCursor;
	}

	// shoould be clear
	@Override
	public IdCursor rangeQueryRowIds(Object startSearchKey,
			Object endSearchKey, boolean isStartKeyIncluded,
			boolean isEndKeyIncluded) throws InvalidRangeException,
			InvalidKeyException, RangeQueryNotSupportedException {

		startSearchKey = getNullType(startSearchKey,
				getColumnType(indexMetaInfo.getKeyColumn()));
		endSearchKey = getNullType(endSearchKey,
				getColumnType(indexMetaInfo.getKeyColumn()));
		
		if (startSearchKey.getClass() != getColumnType(
				indexMetaInfo.getKeyColumn()).getJavaClass()) {
			if (startSearchKey.getClass().equals(INT_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.INTEGER) {
				throw new InvalidKeyException();
			} else if (startSearchKey.getClass().equals(DOUBLE_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.DOUBLE) {
				throw new InvalidKeyException();
			} else if (startSearchKey.getClass().equals(BOOLEAN_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.BOOLEAN) {
				throw new InvalidKeyException();
			}
		}
		
		if (endSearchKey.getClass() != getColumnType(
				indexMetaInfo.getKeyColumn()).getJavaClass()) {
			if (endSearchKey.getClass().equals(INT_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.INTEGER) {
				throw new InvalidKeyException();
			} else if (endSearchKey.getClass().equals(DOUBLE_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.DOUBLE) {
				throw new InvalidKeyException();
			} else if (endSearchKey.getClass().equals(BOOLEAN_CLASS)
					&& getColumnType(indexMetaInfo.getKeyColumn()) != Type.BOOLEAN) {
				throw new InvalidKeyException();
			}
		}

		switch (getColumnType(indexMetaInfo.getKeyColumn())) {
		case BOOLEAN:
			if (((boolean) startSearchKey) == true
					&& ((boolean) endSearchKey) == false) {
				throw new InvalidRangeException();
			}
			break;
		case DATE:
			if (((Date) startSearchKey).after((Date) endSearchKey)) {
				throw new InvalidRangeException();
			}
			break;
		case DOUBLE:
			if (((double) startSearchKey) > ((double) endSearchKey)) {
				throw new InvalidRangeException();
			}
			break;
		case INTEGER:
			if (((int) startSearchKey) > ((int) endSearchKey)) {
				throw new InvalidRangeException();
			}
			break;
		case STRING:
			if (((String) startSearchKey).compareTo((String) endSearchKey) > 0) {
				throw new InvalidRangeException();
			}
			break;
		default:
			break;
		}
		
		ArrayIntList rowIds = bTreeMap.rangeQuery(startSearchKey, endSearchKey,
				isStartKeyIncluded, isEndKeyIncluded);
		IdCursorImpl idCursor = new IdCursorImpl();
		idCursor.addIDArray(rowIds);
		
		return idCursor;
	}

	//clear
	@Override
	public IndexMetaInfo getIndexMetaInfo() {
		return indexMetaInfo;
	}

	//clear
	public int getKeyCount() {
		return bTreeMap.getTotalKeyCount();
	}

	//clear
	public void addRowInIndex(int rowId) {
		Object keyToAdd = (indexMetaInfo.getKeyColumn()).getObject(rowId);
		addEntryInIndexesMap(keyToAdd, rowId,
				getColumnType(indexMetaInfo.getKeyColumn()));
	}
}