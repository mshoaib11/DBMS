package dbs_project.storage.impl;


import java.io.Serializable;

/**
 * Created by Xedos2308 on 05.11.14.
 */
public class ArrayBoolList implements Cloneable, Serializable
{

	private static final long serialVersionUID = 5137990283533776620L;

	private static final int MIN_GROWTH_SIZE = 4;
	private static final int GROWTH_FACTOR_MULTIPLIER = 3;
	private static final int GROWTH_FACTOR_DIVISOR = 2;
	private int INITIAL_SIZE = 10;
	private boolean[] data;
	private int size;
	private int occupancy;

	//constructor
	public ArrayBoolList(int initialSize)
	{
		super();
		if (initialSize <= 0)
		{
			data = new boolean[INITIAL_SIZE];
		}
		else
		{
			data = new boolean[initialSize];
		}
	}

	public int getSize()
	{
		return size;
	}

	//get element value at given index
	public boolean get(int index)
	{
		//check index range
		checkIndex(index);
		if (index >= occupancy)
		{
			throw new IndexOutOfBoundsException();
		}
		return data[index];
	}
	//checks index range
	public boolean checkIndex(int index)
	{
		try
		{
			boolean b = data[index];
			return true;
		}
		catch (Exception e)
		{
			throw new IndexOutOfBoundsException();
		}
	}
	//adds value at given index
	public void add(int index, boolean value)
	{
		checkIndex(index);
		ensureCapacity(size + 1);
		System.arraycopy(data, index, data, index + 1, size - index);
		data[index] = value;
		size++;
		occupancy++;
	}

	// adds a value to this collection
	public void add(boolean value)
	{
		int index = occupancy;
		ensureCapacity(size + 1);
		data[index] = value;
		size++;
		occupancy++;
	}

	//set given value to index
	public void set(int index, boolean value)
	{
		checkIndex(index);
		if (index >= occupancy)
		{
			throw new IndexOutOfBoundsException();
		}
		data[index] = value;
	}

	protected void ensureCapacity(int capacity)
	{
		int len = data.length;
		if (capacity <= len)
		{
			return;
		}

		int newLen = len * GROWTH_FACTOR_MULTIPLIER / GROWTH_FACTOR_DIVISOR;

		if (newLen < capacity)
		{
			newLen = capacity;
		}

		if (newLen < MIN_GROWTH_SIZE)
		{
			newLen = MIN_GROWTH_SIZE;
		}

		boolean[] newArray = new boolean[newLen];
		System.arraycopy(data, 0, newArray, 0, len);
		data = newArray;
	}

}
