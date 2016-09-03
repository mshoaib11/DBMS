package dbs_project.storage.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import dbs_project.index.Index;

/**
 * Created by Xedos2308 on 05.11.14.
 */
public class ObjectHashMap implements Serializable
{

	private int[] keys;
	private ArrayList<Object> values;
	private Object zeroValue;
	private boolean returnedZeroValue = false;
	private static final long serialVersionUID = 4895677160924931761L;

	private static final int MAX_LOAD = 90;

	private int mask;
	private int length;
	private int size;
	private int deletedCount;
	private int level;
	private boolean zeroKey;

	private int maxSize, minSize, maxDeleted;

	public ObjectHashMap()
	{

		reset(1);
	}

	protected void reset(int newLevel)
	{
		minSize = size * 3 / 4;
		size = 0;
		level = newLevel;
		length = 2 << level;
		mask = length - 1;
		maxSize = (int) (length * MAX_LOAD / 100L);
		deletedCount = 0;
		maxDeleted = 20 + length / 2;
		keys = new int[length];
		values = new ArrayList<>(length);

		for (int i = 0; i < length; i++)
		{
			values.add(null);
		}
	}
	public void put(int key, Object value)
	{
		if (key == 0)
		{
			zeroKey = true;
			zeroValue = value;
			return;
		}

		try
		{
			checkSizePut();
		}
		catch (Exception e)
		{
		}

		int index = getIndex(key);
		int plus = 1;
		int deleted = -1;

		do
		{
			int k = keys[index];
			if (k == 0)
			{
				if (values.get(index) != MapMarker.DELETED)
				{
					// found an empty record
					if (deleted >= 0)
					{
						index = deleted;
						deletedCount--;
					}
					size++;
					keys[index] = key;
					values.set(index, value);
					return;
				}
				if (deleted < 0)
				{
					deleted = index;
				}
			}
			else
			{
				if (k == key)
				{
					values.set(index, value);
					return;
				}
			}
			index = (index + plus++) & mask;
		} while (plus <= length);
	}

	public void remove(int key)
	{
		if (key == 0)
		{
			zeroKey = false;
			return;
		}

		try
		{
			checkSizeRemove();
		}
		catch (Exception e)
		{
		}

		int index = getIndex(key);
		int plus = 1;

		do
		{
			int k = keys[index];
			if (k == key)
			{
				keys[index] = 0;
				values.set(index, MapMarker.DELETED);
				deletedCount++;
				size--;
				return;
			}
			else
			{
				if (k == 0 && values.get(index) == null)
				{
					return;
				}
			}
			index = (index + plus++) & mask;
		} while (plus <= length);
	}

	protected void rehash(int newLevel)
	{
		int[] oldKeys = keys;
		ArrayList<Object> oldValues = new ArrayList<Object>(values);
		reset(newLevel);

		for (int i = 0; i < oldKeys.length; i++)
		{
			int k = oldKeys[i];
			if (k != 0)
			{
				put(k, oldValues.get(i));
			}
		}
	}

	public Object get(int key)
	{
		if (key == 0)
		{
			return zeroKey ? zeroValue : MapMarker.NOT_FOUND;
		}

		int index = getIndex(key);

		int plus = 1;
		do
		{
			int k = keys[index];
			if (k == 0 && values.get(index) == null)
			{
				return MapMarker.NOT_FOUND;
			}
			else
			{
				if (k == key)
				{

					return values.get(index);
				}
			}
			index = (index + plus++) & mask;

		} while (plus <= length);

		return MapMarker.NOT_FOUND;
	}

	public int size()
	{

		return size + (zeroKey ? 1 : 0);
	}


	void checkSizePut()
	{
		if (deletedCount > size)
		{
			rehash(level);
		}
		if (size + deletedCount >= maxSize)
		{
			rehash(level + 1);
		}
	}

	protected void checkSizeRemove()
	{
		if (size < minSize && level > 0)
		{
			rehash(level - 1);
		}
		else
		{
			if (deletedCount > maxDeleted)
			{
				rehash(level);
			}
		}
	}

	protected int getIndex(int hash)
	{
		return hash & mask;
	}

	public Collection<ArrayList<Index>> values()
	{
		Collection<ArrayList<Index>> collection = new ArrayList<ArrayList<Index>>();
		for (int key : keys)
		{
			Object cur = get(key);
			if (cur != MapMarker.NOT_FOUND && cur != MapMarker.DELETED)
			{
				if (key == 0 && !returnedZeroValue)
				{
					collection.add((ArrayList<Index>) cur);
					returnedZeroValue = true;
				}
				else
					if (key != 0)
					{
						collection.add((ArrayList<Index>) cur);
					}
			}
		}
		returnedZeroValue = false;
		return collection;
	}

	public int[] keySet()
	{
		return keys;
	}

}
