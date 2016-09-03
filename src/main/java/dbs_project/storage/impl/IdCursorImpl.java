package dbs_project.storage.impl;

import java.io.IOException;

import org.apache.commons.collections.primitives.ArrayIntList;

import dbs_project.util.IdCursor;

/**
 * Created by Xedos2308 on 05.11.14.
 */
public class IdCursorImpl implements IdCursor
{


	private ArrayIntList id_List = new ArrayIntList(20); // contains rowIds for the table like [0,1,2,3......10]
	private int curID = -1;
	static private int no_of_rows = -1;
	public IdCursorImpl()
	{
		no_of_rows++;
	}

	@Override
	public boolean next()
	{
		if (curID < id_List.size() - 1)
		{
			curID++;
			return true;
		}
		return false;
	}

	@Override
	public void close() throws IOException
	{
	}

	@Override
	public int getId() //get the id of the current row
	{

		return id_List.get(curID);
	}

	public void addID(int newID)
	{

		id_List.add(newID); //add row id to id_list
	}

	public void addIDArray(ArrayIntList ids)
	{

		id_List.addAll(ids);
	}
}
