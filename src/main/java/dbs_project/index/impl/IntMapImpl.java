package dbs_project.index.impl;

import java.util.Random;
import org.apache.commons.collections.primitives.ArrayIntList;
import com.esotericsoftware.kryo.util.ObjectMap;

/**
 * Created by Xedos2308 on 29.11.14.
 */

/**
 * Hashmap
 */
public class IntMapImpl<T> {
	private static final int PRIME2 = 0xb4b82e39;
	private static final int PRIME3 = 0xced1c241;
	private static final int EMPTY = 0;

	public int size;
	int[] keyTable;
	T[] valueTable;
	int capacity;
	int stashSize;
	T zeroValue;
	boolean hasZeroValue;

	static Random random = new Random();

	private float loadFactor;
	private int hashShift;
	private int mask;
	private int threshold;
	private int stashCapacity;
	private int pushIterations;

	public IntMapImpl() {
		this(32, 0.8f);
	}

	public IntMapImpl(int initialCapacity, float loadFactor) {
		if (initialCapacity < 0) {
			throw new IllegalArgumentException("initialCapacity must be >= 0: "
					+ initialCapacity);
		}

		if (loadFactor <= 0) {
			throw new IllegalArgumentException("loadFactor must be > 0: "
					+ loadFactor);
		}

		if (capacity > 1 << 30) {
			throw new IllegalArgumentException("initialCapacity is too large: "
					+ initialCapacity);
		}

		capacity = ObjectMap.nextPowerOfTwo(initialCapacity);
		this.loadFactor = loadFactor;

		threshold = (int) (capacity * loadFactor);
		mask = capacity - 1;
		hashShift = 31 - Integer.numberOfTrailingZeros(capacity);
		stashCapacity = Math.max(3, (int) Math.ceil(Math.log(capacity)) * 2);
		pushIterations = Math.max(Math.min(capacity, 8),
				(int) Math.sqrt(capacity) / 8);
		keyTable = new int[capacity + stashCapacity];
		valueTable = (T[]) new Object[keyTable.length];
	}

	public T put(int key, T value) {
		if (key == 0) {
			T oldValue = zeroValue;
			zeroValue = value;
			hasZeroValue = true;
			size++;
			return oldValue;
		}

		int[] keyTable = this.keyTable;

		int index1 = key & mask;
		int key1 = keyTable[index1];
		if (key1 == key) {
			T oldValue = valueTable[index1];
			valueTable[index1] = value;
			return oldValue;
		}

		int index2 = hash2(key);
		int key2 = keyTable[index2];
		if (key2 == key) {
			T oldValue = valueTable[index2];
			valueTable[index2] = value;
			return oldValue;
		}

		int index3 = hash3(key);
		int key3 = keyTable[index3];
		if (key3 == key) {
			T oldValue = valueTable[index3];
			valueTable[index3] = value;
			return oldValue;
		}

		for (int i = capacity, n = i + stashSize; i < n; i++) {
			if (key == keyTable[i]) {
				T oldValue = valueTable[i];
				valueTable[i] = value;
				return oldValue;
			}
		}

		if (key1 == EMPTY) {
			keyTable[index1] = key;
			valueTable[index1] = value;
			if (size++ >= threshold) {
				resize(capacity << 1);
			}
			return null;
		}

		if (key2 == EMPTY) {
			keyTable[index2] = key;
			valueTable[index2] = value;
			if (size++ >= threshold) {
				resize(capacity << 1);
			}
			return null;
		}

		if (key3 == EMPTY) {
			keyTable[index3] = key;
			valueTable[index3] = value;
			if (size++ >= threshold) {
				resize(capacity << 1);
			}
			return null;
		}

		push(key, value, index1, key1, index2, key2, index3, key3);
		return null;
	}

	private void putResize(int key, T value) {
		if (key == 0) {
			zeroValue = value;
			hasZeroValue = true;
			return;
		}

		int index1 = key & mask;
		int key1 = keyTable[index1];
		if (key1 == EMPTY) {
			keyTable[index1] = key;
			valueTable[index1] = value;
			if (size++ >= threshold) {
				resize(capacity << 1);
			}
			return;
		}

		int index2 = hash2(key);
		int key2 = keyTable[index2];
		if (key2 == EMPTY) {
			keyTable[index2] = key;
			valueTable[index2] = value;
			if (size++ >= threshold) {
				resize(capacity << 1);
			}
			return;
		}

		int index3 = hash3(key);
		int key3 = keyTable[index3];
		if (key3 == EMPTY) {
			keyTable[index3] = key;
			valueTable[index3] = value;
			if (size++ >= threshold) {
				resize(capacity << 1);
			}
			return;
		}

		push(key, value, index1, key1, index2, key2, index3, key3);
	}

	private void push(int insertKey, T insertValue, int index1, int key1,
			int index2, int key2, int index3, int key3) {
		int[] keyTable = this.keyTable;
		T[] valueTable = this.valueTable;
		int mask = this.mask;

		int evictedKey;
		T evictedValue;
		int i = 0, pushIterations = this.pushIterations;
		
		do {
			switch (random.nextInt(3)) {
			case 0:
				evictedKey = key1;
				evictedValue = valueTable[index1];
				keyTable[index1] = insertKey;
				valueTable[index1] = insertValue;
				break;
			case 1:
				evictedKey = key2;
				evictedValue = valueTable[index2];
				keyTable[index2] = insertKey;
				valueTable[index2] = insertValue;
				break;
			default:
				evictedKey = key3;
				evictedValue = valueTable[index3];
				keyTable[index3] = insertKey;
				valueTable[index3] = insertValue;
				break;
			}

			index1 = evictedKey & mask;
			key1 = keyTable[index1];
			if (key1 == EMPTY) {
				keyTable[index1] = evictedKey;
				valueTable[index1] = evictedValue;
				if (size++ >= threshold) {
					resize(capacity << 1);
				}
				return;
			}

			index2 = hash2(evictedKey);
			key2 = keyTable[index2];
			if (key2 == EMPTY) {
				keyTable[index2] = evictedKey;
				valueTable[index2] = evictedValue;
				if (size++ >= threshold) {
					resize(capacity << 1);
				}
				return;
			}

			index3 = hash3(evictedKey);
			key3 = keyTable[index3];
			if (key3 == EMPTY) {
				keyTable[index3] = evictedKey;
				valueTable[index3] = evictedValue;
				if (size++ >= threshold) {
					resize(capacity << 1);
				}
				return;
			}

			if (++i == pushIterations) {
				break;
			}

			insertKey = evictedKey;
			insertValue = evictedValue;
		} while (true);

		putStash(evictedKey, evictedValue);
	}

	private void putStash(int key, T value) {
		if (stashSize == stashCapacity) {

			resize(capacity << 1);
			put(key, value);
			return;
		}

		int index = capacity + stashSize;
		keyTable[index] = key;
		valueTable[index] = value;
		stashSize++;
		size++;
	}

	public T get(int key) {
		if (key == 0)
			return zeroValue;
		int index = key & mask;

		if (keyTable[index] != key) {
			index = hash2(key);
			if (keyTable[index] != key) {
				index = hash3(key);
				if (keyTable[index] != key) {
					return getStash(key, null);
				}
			}
		}
		return valueTable[index];
	}

	public T get(int key, T defaultValue) {
		if (key == 0)
			return zeroValue;
		int index = key & mask;
		if (keyTable[index] != key) {
			index = hash2(key);
			if (keyTable[index] != key) {
				index = hash3(key);
				if (keyTable[index] != key) {
					return getStash(key, defaultValue);
				}
			}
		}
		return valueTable[index];
	}

	private T getStash(int key, T defaultValue) {
		int[] keyTable = this.keyTable;
		for (int i = capacity, n = i + stashSize; i < n; i++) {
			if (keyTable[i] == key) {
				return valueTable[i];
			}
		}
		return defaultValue;
	}

	public T remove(int key) {
		if (key == 0) {
			if (!hasZeroValue) {
				return null;
			}
			T oldValue = zeroValue;
			zeroValue = null;
			hasZeroValue = false;
			size--;
			return oldValue;
		}

		int index = key & mask;
		if (keyTable[index] == key) {
			keyTable[index] = EMPTY;
			T oldValue = valueTable[index];
			valueTable[index] = null;
			size--;
			return oldValue;
		}

		index = hash2(key);
		if (keyTable[index] == key) {
			keyTable[index] = EMPTY;
			T oldValue = valueTable[index];
			valueTable[index] = null;
			size--;
			return oldValue;
		}

		index = hash3(key);
		if (keyTable[index] == key) {
			keyTable[index] = EMPTY;
			T oldValue = valueTable[index];
			valueTable[index] = null;
			size--;
			return oldValue;
		}

		return removeStash(key);
	}

	T removeStash(int key) {
		int[] keyTable = this.keyTable;
		for (int i = capacity, n = i + stashSize; i < n; i++) {
			if (keyTable[i] == key) {
				T oldValue = valueTable[i];
				removeStashIndex(i);
				size--;
				return oldValue;
			}
		}
		return null;
	}

	void removeStashIndex(int index) {
		// If the removed location was not last,
		// move the last tuple to the removed location.
		stashSize--;
		int lastIndex = capacity + stashSize;
		if (index < lastIndex) {
			keyTable[index] = keyTable[lastIndex];
			valueTable[index] = valueTable[lastIndex];
			valueTable[lastIndex] = null;
		} else {
			valueTable[index] = null;
		}
	}

	private void resize(int newSize) {
		int oldEndIndex = capacity + stashSize;

		capacity = newSize;
		threshold = (int) (newSize * loadFactor);
		mask = newSize - 1;
		hashShift = 31 - Integer.numberOfTrailingZeros(newSize);
		stashCapacity = Math.max(3, (int) Math.ceil(Math.log(newSize)) * 2);
		pushIterations = Math.max(Math.min(newSize, 8),
				(int) Math.sqrt(newSize) / 8);

		int[] oldKeyTable = keyTable;
		T[] oldValueTable = valueTable;

		keyTable = new int[newSize + stashCapacity];
		valueTable = (T[]) new Object[newSize + stashCapacity];

		if (hasZeroValue) {
			size = 1;
		} else {
			size = 0;
		}
		stashSize = 0;

		for (int i = 0; i < oldEndIndex; i++) {
			int key = oldKeyTable[i];
			if (key != EMPTY) {
				putResize(key, oldValueTable[i]);
			}
		}
	}

	private int hash2(int h) {
		h *= PRIME2;
		return (h ^ h >>> hashShift) & mask;
	}

	private int hash3(int h) {
		h *= PRIME3;
		return (h ^ h >>> hashShift) & mask;
	}

	public String toString() {
		if (size == 0) {
			return "[]";
		}

		StringBuilder buffer = new StringBuilder(32);
		buffer.append('[');
		int[] keyTable = this.keyTable;
		T[] valueTable = this.valueTable;
		int i = keyTable.length;

		while (i-- > 0) {
			int key = keyTable[i];
			if (key == EMPTY) {
				continue;
			}
			buffer.append(key);
			buffer.append('=');
			buffer.append(valueTable[i]);
			break;
		}

		while (i-- > 0) {
			int key = keyTable[i];
			if (key == EMPTY) {
				continue;
			}
			buffer.append(", ");
			buffer.append(key);
			buffer.append('=');
			buffer.append(valueTable[i]);
		}

		buffer.append(']');
		return buffer.toString();
	}

	public int size() {
		int result = size;
		if (hasZeroValue) {
			result -= ((ArrayIntList) zeroValue).size();
			result++;
		}
		return result;
	}

}
