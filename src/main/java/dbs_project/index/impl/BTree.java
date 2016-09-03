package dbs_project.index.impl;

import java.util.ArrayList;
import org.apache.commons.collections.primitives.ArrayIntList;

/**
 * Created by Xedos2308 on 29.11.14.
 */
public class BTree {

	// creating needed elements for b tree
	private Node root;
	private int leafNodesCount;
	private int internalNodesCount;
	private int totalKeyCount;

	//constructor
	public BTree(int leafNodesCount, int internalNodesCount) {
		this.leafNodesCount = leafNodesCount;
		this.internalNodesCount = internalNodesCount;
		totalKeyCount = 0;
		root = new LeafNode();
	}
    // insert a new Object in the tree
	public void insert(Object key, int value) {
		//inserting
		root.insert(key, value);
		// keycounter
		totalKeyCount++;
	}
    //delete a given object
	public void delete(Object key) {

		if (root.delete(key)) {
			//decrease the keycounter only when object was deleted
			totalKeyCount--;
		}
	}

    // return the arrayIntlist for the given key
	public ArrayIntList pointQuery(Object key) {

		return root.pointQuery(key);
	}

	//return the elements in the range for the given start end Endkey
	public ArrayIntList rangeQuery(Object keyStart, Object keyEnd,
			boolean isStartInclusive, boolean isEndInclusive) {

		return root.rangeQuery(keyStart, keyEnd, isStartInclusive,
				isEndInclusive);
	}

	// clear
	public int getTotalKeyCount() {
		return totalKeyCount;
	}

	// delete a value from the leaf-node
	public void deleteValue(int value) {
		root.deleteValue(value);
	}

    // create a news abstract class for the Node
	private abstract class Node {


		protected InternalNode parent;
		protected int keyCount;
		protected int nodeSize;

		//some methods which are abstract and we need to implement
		abstract void insert(Object key, int value);

		abstract void insert(Object key, ArrayIntList values);

		abstract boolean delete(Object key);

		abstract ArrayIntList pointQuery(Object key);

		abstract ArrayIntList rangeQuery(Object keyStart, Object keyEnd,
				boolean isStartInclusive, boolean isEndInclusive);

		abstract void deleteValue(int value);

        // return  the depth, recursive call
		int getDepth() {
			if (this.parent == null) {
				return 0;
			}

			return 1 + this.parent.getDepth();
		}

		// return the number of free buckets in the nodes
		int getFreeCellsCount() {
			return nodeSize - keyCount;
		}

		// create a new parent node
		void createParent(Object aKey, Node newNode) {
			InternalNode parentNode = new InternalNode(aKey, newNode, this);
			this.parent = parentNode;
			root = parentNode;
		}

        // update Parent node with a given key with new node pointer
		void updateParent(Object aKey, Node newNode) {
			this.parent.update(aKey, newNode);
		}

		// clear
		Node findLeftMostChild(int haltingDepth) {

			//check whether right depth
			if (haltingDepth == this.getDepth()) {
				return this;
			}

			//return the node with childnodepointers value 0, which means its the mostleft one
			Node leftMostChildOfCurrentNode = ((InternalNode) this).childNodesPtrs
					.get(0);

			return leftMostChildOfCurrentNode.findLeftMostChild(haltingDepth);
		}

		//clear
		Node getRightChild(int child_index) {

			// check whether the last node is bigger then the child_index
			if (child_index < ((InternalNode) this).childNodesPtrs.size() - 1) {

				// if yes then return the right child of the given index
				Node rightChildFromCurrentPtr = ((InternalNode) this).childNodesPtrs
						.get(child_index + 1);
				return rightChildFromCurrentPtr;
			}

			// null when there are no nodes in the tree
			if (this.parent == null) {
				return null;
			}
			// if there is only one node, then return this
			return this.parent.getRightChild(this.parent.childNodesPtrs
					.indexOf(this));
		}

		// searching for the next node in the "linkedlist"
		Node findNextNode() {

			// check the parentnode
			if (this.parent != null) {

				// initialize the rightchild of this node
				Node RsideChildOfFirstBranchingParent = this.parent
						.getRightChild(this.parent.childNodesPtrs.indexOf(this));

                // if the pointer to the node is not null return the leftmostchild of the initialized node
				if (RsideChildOfFirstBranchingParent != null) {
					return RsideChildOfFirstBranchingParent
							.findLeftMostChild(this.getDepth());

				} else {
					return null;
				}
			} else {
				return null;
			}
		}

		//same like leftmostchild
		Node findRightMostChildAtHaltingDepth(int haltingDepth) {
			if (haltingDepth == this.getDepth()) {
				return this;
			}

			Node rightMostChildOfCurrentNode = ((InternalNode) this).childNodesPtrs
					.get(((InternalNode) this).childNodesPtrs.size() - 1);

			return rightMostChildOfCurrentNode
					.findRightMostChildAtHaltingDepth(haltingDepth);
		}

		// the name of function is clear enough
		Node getLsideChildOfFirstParentBranchingLeft(int child_index) {

			if (child_index > 0) {
				Node prevChildOfCurrentNode = ((InternalNode) this).childNodesPtrs
						.get(child_index - 1);
				return prevChildOfCurrentNode;
			}

			if (this.parent == null) {
				return null;
			}

			return this.parent
					.getLsideChildOfFirstParentBranchingLeft(this.parent.childNodesPtrs
							.indexOf(this));
		}

		// the opposite of findenextNode, should be clear
		Node findPreviousNode() {
			if (this.parent != null) {
				Node LsideChildOfSharedParent = this.parent
						.getLsideChildOfFirstParentBranchingLeft(this.parent.childNodesPtrs
								.indexOf(this));
				if (LsideChildOfSharedParent != null) {
					return LsideChildOfSharedParent
							.findRightMostChildAtHaltingDepth(this.getDepth());
				} else {
					return null;
				}
			} else {
				return null;
			}
		}

	}

	// another class which defines the InternalNode
	private class InternalNode extends Node {

		private ArrayList<Object> nodeKeys;
		private ArrayList<Node> childNodesPtrs;

		//constructor
		InternalNode(Object key, Node rightPtr, Node leftPtr) {
			nodeSize = internalNodesCount;
			nodeKeys = new ArrayList<>(nodeSize + 1);
			childNodesPtrs = new ArrayList<>(nodeSize + 2);
			keyCount = 0;
			create(key, rightPtr, leftPtr);
		}

		//clear
		void create(Object key, Node rightPtr, Node leftPtr) {
			propagateInsert(key, rightPtr, leftPtr);
		}

		//clear
		void update(Object key, Node rightPtr) {
			propagateInsert(key, rightPtr, null);
		}

		//clear
		void deleteValue(int value) {
			childNodesPtrs.get(0).deleteValue(value);
		}

		//clear on one new Value
		void insert(Object newKey, int newValue) {
			ArrayIntList newValues = new ArrayIntList();
			newValues.add(newValue);

			for (int i = 0; i < keyCount; i++) {
				int compareResult = ((Comparable<Object>) newKey)
						.compareTo(nodeKeys.get(i));

				if (compareResult < 0) {
					childNodesPtrs.get(i).insert(newKey, newValues);
					return;
				}
			}

			childNodesPtrs.get(keyCount).insert(newKey, newValues);
		}

		// a new ArrayIntList
		void insert(Object newKey, ArrayIntList newValues) {
			for (int i = 0; i < keyCount; i++) {
				int compareResult = ((Comparable<Object>) newKey)
						.compareTo(nodeKeys.get(i));

				if (compareResult < 0) {
					childNodesPtrs.get(i).insert(newKey, newValues);
					return;
				}
			}

			childNodesPtrs.get(keyCount).insert(newKey, newValues);
		}
		//clear
		boolean delete(Object keyToDelete) {
			for (int i = 0; i < keyCount; i++) {
				int compare_result = ((Comparable<Object>) keyToDelete)
						.compareTo(nodeKeys.get(i));

				if (compare_result < 0) {
					return childNodesPtrs.get(i).delete(keyToDelete);
				}
			}

			return childNodesPtrs.get(keyCount).delete(keyToDelete);
		}

		//clear, to find a key by pointquery
		ArrayIntList pointQuery(Object keyToFind) {
			for (int i = 0; i < keyCount; i++) {
				int compareResult = ((Comparable<Object>) keyToFind)
						.compareTo(nodeKeys.get(i));

				if (compareResult < 0) {
					return childNodesPtrs.get(i).pointQuery(keyToFind);
				}
			}

			return childNodesPtrs.get(keyCount).pointQuery(keyToFind);
		}

		//rangequery like in the Node
		ArrayIntList rangeQuery(Object keyStart, Object keyEnd,
				boolean startInclusive, boolean endInclusive) {
			for (int i = 0; i < keyCount; i++) {
				int compareResult = ((Comparable<Object>) keyStart)
						.compareTo(nodeKeys.get(i));

				if (compareResult < 0) {
					return childNodesPtrs.get(i).rangeQuery(keyStart, keyEnd,
							startInclusive, endInclusive);
				}
			}

			return childNodesPtrs.get(keyCount).rangeQuery(keyStart, keyEnd,
					startInclusive, endInclusive);
		}

		// clear
		int sortedKeyInsert(Object propagatedKey) {
			for (int i = 0; i < keyCount; i++) {
				int compareResult = ((Comparable<Object>) propagatedKey)
						.compareTo(nodeKeys.get(i));

				if (compareResult < 0) {
					nodeKeys.add(i, propagatedKey);
					keyCount++;
					return i;
				}
			}
			nodeKeys.add(propagatedKey);
			keyCount++;
			return keyCount - 1;

		}

		//clear
		void propagateInsert(Object propagatedKey, Node rightPtr, Node leftPtr) {

			// increment keyCount
			if (keyCount == 0) {
				int insertedPos = this.sortedKeyInsert(propagatedKey);
				this.updatePointers(insertedPos, rightPtr, leftPtr);
			} else {
				if (keyCount < nodeSize) {
					int insertedPos = this.sortedKeyInsert(propagatedKey);
					this.updatePointers(insertedPos, rightPtr, null);
				} else {
					int tempInsertedPos = this.sortedKeyInsert(propagatedKey);
					this.updatePointers(tempInsertedPos, rightPtr, null);

					int splitPoint = nodeKeys.size() / 2;

					InternalNode newInternalNode = this.performSplit(
							splitPoint, rightPtr);

					Object moveUpKey = nodeKeys.remove(splitPoint);
					this.keyCount--;

					if (this.parent == null) {
						this.createParent(moveUpKey, newInternalNode);
					} else {
						this.updateParent(moveUpKey, newInternalNode);
					}

					newInternalNode.parent = this.parent;
				}
			}
		}

		//split function
		InternalNode performSplit(int split_point, Node newChildNode) {

			int newNodeStart = split_point + 1;
			Object removedKey = this.nodeKeys.remove(newNodeStart);
			this.keyCount--;
			Node removedPtrRight = this.childNodesPtrs.remove(newNodeStart + 1);
			Node removedPtrLeft = this.childNodesPtrs.remove(newNodeStart);
			InternalNode newInternalNode = new InternalNode(removedKey,
					removedPtrRight, removedPtrLeft);
			removedPtrRight.parent = newInternalNode;
			removedPtrLeft.parent = newInternalNode;

			int num_keys = this.nodeKeys.size();
			for (int i = newNodeStart; i < num_keys; i++) {
				removedKey = this.nodeKeys.remove(newNodeStart);
				this.keyCount--;
				removedPtrRight = this.childNodesPtrs.remove(newNodeStart);
				newInternalNode.update(removedKey, removedPtrRight);
				removedPtrRight.parent = newInternalNode;
			}

			return newInternalNode;
		}

		void modifySplitKey(Object propagatedKey) {
			if (this.parent == null) {
				return;
			}
			int ptrIndexAtParent = this.parent.childNodesPtrs.indexOf(this);
			if (ptrIndexAtParent != 0) {
				this.parent.nodeKeys.set(ptrIndexAtParent - 1, propagatedKey);
				return;
			}
			this.parent.modifySplitKey(propagatedKey);

		}

		void propagateDelete(Node disposedChildNode) {

			int child_index = childNodesPtrs.indexOf(disposedChildNode);

			if (child_index == 0) {
				modifySplitKey(this.nodeKeys.get(0));
			}

			childNodesPtrs.remove(child_index);
			keyCount--;
			int half_capacity = nodeSize / 2;

			if (this.parent == null && keyCount == 0) {
				root = this.childNodesPtrs.get(0);
				root.parent = null;
			} else if (this.parent != null && keyCount < half_capacity) {
				this.merge();
			}

		}

		void updatePointers(int inserted_pos, Node ptr_to_right,
				Node ptr_to_left) {
			if (ptr_to_left != null) {
				childNodesPtrs.add(ptr_to_left);
				childNodesPtrs.add(ptr_to_right);
			} else {
				childNodesPtrs.add(inserted_pos + 1, ptr_to_right);
			}
		}

		Object getSplitKey() {
			if (this.parent == null)
				return null;
			int ptrIndexAtParent = this.parent.childNodesPtrs.indexOf(this);
			if (ptrIndexAtParent != 0)
				return this.parent.nodeKeys.get(ptrIndexAtParent - 1);
			return this.parent.getSplitKey();
		}


		void merge() {
			int freeCells_leftNode = 0;
			int freeCells_rightNode = 0;

			InternalNode prevNode;
			InternalNode nextNode;

			prevNode = (InternalNode) this.findPreviousNode();
			nextNode = (InternalNode) this.findNextNode();

			if (prevNode != null)
				freeCells_leftNode = prevNode.getFreeCellsCount();
			if (nextNode != null)
				freeCells_rightNode = nextNode.getFreeCellsCount();

			if (Math.max(freeCells_leftNode, freeCells_rightNode) < this.keyCount + 1)
				return;

			InternalNode chosenNode;
			if (freeCells_leftNode == freeCells_rightNode)
				chosenNode = (prevNode.parent != this.parent) ? nextNode
						: prevNode;
			else
				chosenNode = (freeCells_leftNode > freeCells_rightNode) ? prevNode
						: nextNode;

			if (chosenNode == prevNode) {
				Object splitKey = this.getSplitKey();
				prevNode.update(splitKey, this.childNodesPtrs.get(0));
				for (int i = 0; i < this.keyCount; i++) {
					prevNode.update(this.nodeKeys.get(i),
							this.childNodesPtrs.get(i + 1));
				}
				this.parent.propagateDelete(this);
			} else {
				Object splitKey = nextNode.getSplitKey();
				this.update(splitKey, nextNode.childNodesPtrs.get(0));
				for (int i = 0; i < nextNode.keyCount; i++) {
					this.update(nextNode.nodeKeys.get(i),
							nextNode.childNodesPtrs.get(i + 1));
				}
				nextNode.parent.propagateDelete(nextNode);
			}
		}

	}

	//leftnode
	// still the same functions
	private class LeafNode extends Node {
		private ArrayList<DataEntry> dataEntries;
		private LeafNode nextLeafNode;

		LeafNode() {

			nodeSize = leafNodesCount;
			dataEntries = new ArrayList<>(nodeSize + 1);
			keyCount = 0;
		}

		void deleteValue(int value) {
			LeafNode currentLeafNode = this;
			int currentNode_keyIndex = 0;

			boolean keyDeleted = false;
			while (currentLeafNode != null && !keyDeleted) {

				ArrayIntList values = currentLeafNode
						.getDataEntry(currentNode_keyIndex).values;
				keyDeleted = values.removeElement(value);
				if (keyDeleted && values.size() == 0) {
					currentLeafNode.delete(currentLeafNode
							.getDataEntry(currentNode_keyIndex).key);
					totalKeyCount--;
				}

				if (!keyDeleted) {
					currentNode_keyIndex++;
					if (currentNode_keyIndex >= currentLeafNode.keyCount) {
						currentLeafNode = currentLeafNode.nextLeafNode;
						currentNode_keyIndex = 0;
					}
				}

			}
			return;
		}

		ArrayIntList pointQuery(Object key_to_find) {
			for (int i = 0; i < keyCount; i++) {
				int compare_result = ((Comparable<Object>) key_to_find)
						.compareTo((Object) dataEntries.get(i).key);

				if (compare_result == 0) {
					return dataEntries.get(i).values;
				}
			}
			return null;
		}

		ArrayIntList rangeQuery(Object key_start, Object key_end,
				boolean start_inclusive, boolean end_inclusive) {

			ArrayIntList search_results = new ArrayIntList();

			LeafNode currentLeafNode = this;
			boolean reached_start = false;
			boolean reached_end = false;
			int currentNode_keyIndex = 0;

			while (!reached_end && currentLeafNode != null) {

				if (!reached_start) {
					int compare_current_with_start = ((Comparable<Object>) key_start)
							.compareTo((Object) currentLeafNode
									.getDataEntry(currentNode_keyIndex).key);
					if (start_inclusive) {
						if (compare_current_with_start <= 0)
							reached_start = true;
					} else {
						if (compare_current_with_start < 0)
							reached_start = true;
					}
				}
				if (reached_start) {
					int compare_current_with_end = ((Comparable<Object>) key_end)
							.compareTo(currentLeafNode
									.getDataEntry(currentNode_keyIndex).key);
					if (end_inclusive) {
						if (compare_current_with_end < 0)
							reached_end = true;
						else {
							search_results.addAll(currentLeafNode
									.getDataEntry(currentNode_keyIndex).values);
						}
					} else {
						if (compare_current_with_end <= 0)
							reached_end = true;
						else {
							search_results.addAll(currentLeafNode
									.getDataEntry(currentNode_keyIndex).values);
						}
					}

				}

				currentNode_keyIndex++;
				if (currentNode_keyIndex >= currentLeafNode.keyCount) {
					currentLeafNode = currentLeafNode.nextLeafNode;
					currentNode_keyIndex = 0;
				}
			}
			return search_results;
		}

		void merge() {
			int freeCells_leftNode = 0;
			int freeCells_rightNode = 0;

			LeafNode pLeafNode;
			LeafNode nLeafNode;

			pLeafNode = (LeafNode) this.findPreviousNode();
			nLeafNode = (LeafNode) this.findNextNode();

			if (pLeafNode != null)
				freeCells_leftNode = pLeafNode.getFreeCellsCount();
			if (nLeafNode != null)
				freeCells_rightNode = nLeafNode.getFreeCellsCount();

			if (Math.max(freeCells_leftNode, freeCells_rightNode) < this.keyCount)
				return;

			LeafNode chosenNode;
			if (freeCells_leftNode == freeCells_rightNode)
				chosenNode = (pLeafNode.parent != this.parent) ? nLeafNode
						: pLeafNode;
			else
				chosenNode = (freeCells_leftNode > freeCells_rightNode) ? pLeafNode
						: nLeafNode;

			if (chosenNode == pLeafNode) {

				for (int i = 0; i < this.keyCount; i++) {
					pLeafNode.insert(this.dataEntries.get(i).key,
							this.dataEntries.get(i).values);
				}
				this.parent.propagateDelete(this);

				if (pLeafNode != null)
					pLeafNode.nextLeafNode = nLeafNode;
			} else {
				for (int i = 0; i < nLeafNode.keyCount; i++) {
					this.insert(nLeafNode.dataEntries.get(i).key,
							nLeafNode.dataEntries.get(i).values);
				}
				nLeafNode.parent.propagateDelete(nLeafNode);
				this.nextLeafNode = nLeafNode.nextLeafNode;

			}

		}

		boolean delete(Object key_to_delete) {
			boolean found_key = false;
			for (int i = 0; i < keyCount && !found_key; i++) {
				int compare_result = ((Comparable<Object>) key_to_delete)
						.compareTo(dataEntries.get(i).key);

				if (compare_result == 0) {
					dataEntries.remove(i);
					keyCount--;
					--i;
					found_key = true;
				}
			}
			float half_capacity = ((float) nodeSize / 2.f);

			if (parent != null && keyCount < half_capacity) {
				this.merge();
			}
			return found_key;
		}

		void sortedDataInsert(Object new_key, ArrayIntList new_values) {
			DataEntry newDataEntry;
			for (int i = 0; i < keyCount; i++) {
				int compare_result = ((Comparable<Object>) new_key)
						.compareTo(dataEntries.get(i).key);

				if (compare_result == 0) {
					dataEntries.get(i).addValues(new_values);
					totalKeyCount--;
					return;
				}
				if (compare_result < 0) {
					newDataEntry = new DataEntry(new_key);
					newDataEntry.addValues(new_values);
					dataEntries.add(i, newDataEntry);
					keyCount++;
					return;
				}
			}
			newDataEntry = new DataEntry(new_key);
			newDataEntry.addValues(new_values);
			dataEntries.add(newDataEntry);
			keyCount++;

		}

		void insert(Object new_key, int new_value) {

			ArrayIntList new_values = new ArrayIntList();
			new_values.add(new_value);

			if (keyCount < nodeSize) {

				this.sortedDataInsert(new_key, new_values);

			} else {

				this.sortedDataInsert(new_key, new_values);
				int split_point = dataEntries.size() / 2;

				LeafNode newLeafNode = this.performSplit(split_point);

				if (this.parent == null) {
					this.createParent(newLeafNode.getDataEntry(0).key,
							newLeafNode);
				} else {
					this.updateParent(newLeafNode.getDataEntry(0).key,
							newLeafNode);
				}
				newLeafNode.parent = this.parent;

				this.nextLeafNode = newLeafNode;
			}

		}

		void insert(Object new_key, ArrayIntList new_values) {

			if (keyCount < nodeSize) {

				this.sortedDataInsert(new_key, new_values);

			} else {
				this.sortedDataInsert(new_key, new_values);
				int split_point = dataEntries.size() / 2;

				LeafNode newLeafNode = this.performSplit(split_point);

				if (this.parent == null) {
					this.createParent(newLeafNode.getDataEntry(0).key,
							newLeafNode);
				} else {
					this.updateParent(newLeafNode.getDataEntry(0).key,
							newLeafNode);
				}
				newLeafNode.parent = this.parent;

				this.nextLeafNode = newLeafNode;
			}

		}

		LeafNode performSplit(int split_point) {

			int newNodeStart = split_point;
			LeafNode newLeafNode = new LeafNode();

			int num_entries = this.dataEntries.size();
			for (int i = split_point; i < num_entries; i++) {
				DataEntry removedEntry = dataEntries.remove(newNodeStart);
				this.keyCount--;
				newLeafNode.insert(removedEntry.key, removedEntry.values);
			}

			return newLeafNode;
		}

		DataEntry getDataEntry(int index) {
			return dataEntries.get(index);
		}

	}

	public class DataEntry extends Object {
		private Object key;
		private ArrayIntList values;

		DataEntry(Object key) {
			this.key = key;
			values = new ArrayIntList();
		}

		void addValues(ArrayIntList new_values) {
			for (int i = 0; i < new_values.size(); i++) {
				values.add(new_values.get(i));
			}
		}
	}

}
