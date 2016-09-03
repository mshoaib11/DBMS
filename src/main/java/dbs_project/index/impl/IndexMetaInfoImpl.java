package dbs_project.index.impl;

import dbs_project.exceptions.NoSuchColumnException;
import dbs_project.exceptions.NoSuchIndexException;
import dbs_project.index.Index;
import dbs_project.index.IndexMetaInfo;
import dbs_project.index.IndexType;
import dbs_project.index.IndexableTable;
import dbs_project.storage.Column;
import dbs_project.storage.impl.TableImpl;

/**
 * Created by Xedos2308 on 29.11.14.
 */
public class IndexMetaInfoImpl implements IndexMetaInfo
{
	private int indexId;
	private String indexName;
	private TableImpl sourceTable;
	private int columnId;
	private IndexType indexType;
	private boolean isSupportRangeQuery;

	//constructor
	public IndexMetaInfoImpl(int indexId, String indexName, TableImpl sourceTable, int columnId,
			IndexType IndexType)
	{
		this.indexId = indexId;
		this.indexName = indexName;
		this.sourceTable = sourceTable;
		this.columnId = columnId;
		indexType = IndexType;
		if (indexType != IndexType.TREE)
		{
			isSupportRangeQuery = false;
		}
		else
		{
			isSupportRangeQuery = true;
		}
	}

	/**
	* getter Functions
	**/

	@Override
	public int getId()
	{
		return indexId;
	}

	@Override
	public String getName()
	{
		return indexName;
	}

	@Override
	public IndexableTable getTable()
	{
		return sourceTable;
	}

	//return the keyColumn used for index
	@Override
	public Column getKeyColumn()
	{
		try
		{
			return sourceTable.getColumn(columnId);
		}
		catch (NoSuchColumnException e)
		{
			return null;
		}
	}

	// number of Keys
	@Override
	public int getKeyCount()
	{
		Index index = null;
		int result = 0;
		try
		{
			index = sourceTable.getIndex(indexId);
		}
		catch (NoSuchIndexException e)
		{
			e.printStackTrace();
		}

		switch (indexType)
		{
			case BITMAP:
				break;
			case HASH:
				result = ((HashIndex) index).getKeyCount();
				break;
			case OTHER:
				break;
			case TREE:
				result = ((TreeIndexBTreeImpl) index).getKeyCount();
				break;
			default:
				break;
		}
		
		return result;
	}

	@Override
	public IndexType getIndexType()
	{
		return indexType;
	}

	@Override
	public boolean supportsRangeQueries()
	{
		return isSupportRangeQuery;
	}
	
}
