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
import dbs_project.storage.RowCursor;
import dbs_project.storage.Type;
import dbs_project.util.IdCursor;

/**
 * Created by Xedos2308 on 29.11.14.
 */
public class HashIndex implements Index {

	private IntMapImpl<ArrayIntList> hashMap;
	private IndexMetaInfoImpl indexMetaInfo; //meta info for index
	private static final String INT_CLASS = "java.lang.Integer";
	private static final String DOUBLE_CLASS = "java.lang.Double";
	private static final String BOOLEAN_CLASS = "java.lang.Boolean";

	//constructor
	public HashIndex(int indexId, String name, TableImpl sourceTable,
			int columnId, IndexType indexType) {

		this.indexMetaInfo = new IndexMetaInfoImpl(indexId, name, sourceTable, columnId,
				indexType);
		createIndexFromScratch();
	}

	//create an initial index
	public void createIndexFromScratch() {
		hashMap = null;
		hashMap = new IntMapImpl<>();

		// columns to index
		ColumnImpl columnToIndex = (ColumnImpl) getIndexMetaInfo()
				.getKeyColumn();
		// rowIds to index
		ArrayIntList rowIdList = ((TableImpl) getIndexMetaInfo().getTable())
				.getRowIds();
		// creating the index
		for (int currentRowId = 0; currentRowId < rowIdList.size(); currentRowId++) {
			Object key = columnToIndex.getObject(currentRowId);
			addEntryInIndexesMap(key, currentRowId, columnToIndex.getMetaData()
					.getType());
		}
	}

    // clear
	public void addEntryInIndexesMap(Object key, int rowId, Type type) {

		//modify for nulltype
		key = getNullType(key, type);

		// create the key
		ArrayIntList newValue = hashMap.get(key.hashCode());

		if (newValue == null) {
			newValue = new ArrayIntList();
		}

		if (!newValue.contains(rowId)) {
			newValue.add(rowId);
		}
        // add the key to the index
		hashMap.put(key.hashCode(), newValue);
	}


	public void updateRowInIndexes(int rowId, Object key) {
		//first remove the key on the rowID
		removeRowIdFromIndex(rowId, key);

		ColumnImpl columnToIndex = (ColumnImpl) getIndexMetaInfo()
				.getKeyColumn();
		Object newKey = columnToIndex.getObject(rowId);

		// add a new Key on the given rowID
		addEntryInIndexesMap(newKey, rowId, columnToIndex.getMetaData()
				.getType());
	}

    // remove the Key on given rowID
	public void removeRowIdFromIndex(int rowId, Object key) {
		ArrayIntList values = hashMap.get(key.hashCode());
		values.removeElement(rowId);
		if (values.size() == 0) {
			hashMap.remove(key.hashCode());
		}
	}

	// return the rowCursor on the given searchkey
	@Override
	public RowCursor pointQuery(Object searchKey) throws InvalidKeyException {
		//modify for nulltype
		searchKey = getNullType(searchKey, getColumnType());

		// check whether the searchkey.class match with the columntype
		if (searchKey.getClass() != getColumnType().getJavaClass()) {
			if (searchKey.getClass().equals(INT_CLASS)
					&& getColumnType() != Type.INTEGER) {
				throw new InvalidKeyException();
			} else if (searchKey.getClass().equals(DOUBLE_CLASS)
					&& getColumnType() != Type.DOUBLE) {
				throw new InvalidKeyException();
			} else if (searchKey.getClass().equals(BOOLEAN_CLASS)
					&& getColumnType() != Type.BOOLEAN) {
				throw new InvalidKeyException();
			}
		}
		// return columnlist and rowIdlist
		RowCursorImpl rowCursor = new RowCursorImpl(
				((TableImpl) (indexMetaInfo.getTable())).getColumnsList(), hashMap.get(searchKey.hashCode()));

		return rowCursor;
	}

	// is not supported here
	@Override
	public RowCursor rangeQuery(Object startSearchKey, Object endSearchKey,
			boolean includeStartKey, boolean includeEndKey)
			throws InvalidRangeException, InvalidKeyException,
			RangeQueryNotSupportedException {
		throw new RangeQueryNotSupportedException();
	}

	// same like pointquery, but on the idCursor
	@Override
	public IdCursor pointQueryRowIds(Object searchKey)
			throws InvalidKeyException {
		searchKey = getNullType(searchKey, getColumnType());

		if (searchKey.getClass() != getColumnType().getJavaClass()) {
			if (searchKey.getClass().equals(INT_CLASS)
					&& getColumnType() != Type.INTEGER) {
				throw new InvalidKeyException();
			} else if (searchKey.getClass().equals(DOUBLE_CLASS)
					&& getColumnType() != Type.DOUBLE) {
				throw new InvalidKeyException();
			} else if (searchKey.getClass().equals(BOOLEAN_CLASS)
					&& getColumnType() != Type.BOOLEAN) {
				throw new InvalidKeyException();
			}
		}

		IdCursorImpl idCursor = new IdCursorImpl();
		ArrayIntList rowIdList = hashMap.get(searchKey.hashCode());

		if (rowIdList != null) {
			idCursor.addIDArray(rowIdList);
		}

		return idCursor;
	}

	// check for nulltype and adjust to the key type
	private Object getNullType(Object key, Type type) {
		if (key == null) {
			switch (type) {
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

	// not supported for hash indexes
	@Override
	public IdCursor rangeQueryRowIds(Object startSearchKey,
			Object endSearchKey, boolean includeStartKey, boolean includeEndKey)
			throws InvalidRangeException, InvalidKeyException,
			RangeQueryNotSupportedException {
		throw new RangeQueryNotSupportedException();
	}
    // clear
	@Override
	public IndexMetaInfo getIndexMetaInfo() {
		return indexMetaInfo;
	}

	//clear
	public int getKeyCount() {
		return hashMap.size();
	}

	// add a new Entry in indexMap with given rowID
	public void addRowInIndex(int rowId) {
		Object keyToAdd = (indexMetaInfo.getKeyColumn()).getObject(rowId);
		addEntryInIndexesMap(keyToAdd, rowId, getColumnType());
	}

	// determine the columntype
	public Type getColumnType() {

		return indexMetaInfo.getKeyColumn().getMetaData().getType();
	}
}
