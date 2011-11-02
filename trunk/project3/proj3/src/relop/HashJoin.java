package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;

/**
 * Implements the hash-based join algorithm described in section 14.4.3 of the
 * textbook (3rd edition; see pages 463 to 464). HashIndex is used to partition
 * the tuples into buckets, and HashTableDup is used to store a partition in
 * memory during the matching phase.
 */
public class HashJoin extends Iterator {

	private HeapFile file;

	private Tuple leftTuple = null;
	private Iterator left = null;
	private Iterator right = null;
	private Integer lcol = null;
	private Integer rcol = null;
	// private Schema schema;

	private IndexScan leftBucketScan = null;
	private IndexScan rightBucketScan = null;
	private int leftHashKey = -1;
	private int rightHashKey = -2;

	private Tuple[] matchingTuples = null;
	private int indexInTupleArray = 0;

//	private RID rightRIDToMatch = null;
//	private RID leftRIDToMatch = null;
	
	private Tuple rightTupleToMatch = null;
	private Tuple leftTupleToMatch = null;

	private HashTableDup hashTableDup = null;
	// private HashTableDup hashTabDupLeft = new HashTableDup();
	// private HashTableDup hashTabDupright = new HashTableDup();

	// TODO : Assume join is always equi
	private boolean foundNext = false;
	private Tuple next = null;
	private boolean isOpen = false;

	private int fileNameCtr = 0;
	private ScanType leftScanType = null;;
	private ScanType rightScanType = null;

	private Tuple nextTuple = null;

	/**
	 * Constructs a hash join, given the left and right iterators and which
	 * columns to match (relative to their individual schemas).
	 */
	public HashJoin(Iterator left, Iterator right, Integer lcol, Integer rcol) {
		// this.left = left;
		// this.right = right;
		this.lcol = lcol; // = new Integer(lcol);
		this.rcol = rcol; // = new Integer(rcol);
		leftScanType = ScanType.getScanType(left);
		rightScanType = ScanType.getScanType(right);
		this.schema = Schema.join(left.schema, right.schema);

		createBucketScan(left, right, lcol, rcol);

		// TODO just assign ref or create new obj
		this.left = left;
		this.right = right;

	}

	public void createBucketScan(Iterator left, Iterator right, int lcol,
			int rcol) {

		this.leftBucketScan = createBucketScan(left, lcol, leftScanType);
		this.rightBucketScan = createBucketScan(right, rcol, rightScanType);

	}

	private IndexScan createBucketScan(Iterator iter, int col,
			ScanType scanType) {
		switch (scanType) {
		case FILESCAN:
			HashIndex hashIdx = new HashIndex("HashFileName" + fileNameCtr++);
			hashAndSave(iter, col, hashIdx);
			return new IndexScan(iter.schema, hashIdx, ((FileScan) iter).getHeapFile());
			// break; 
		case KEYSCAN:
			return new IndexScan(iter.schema,((KeyScan) iter).getHashIndex(),((KeyScan) iter).getHeapFile()); 
		case INDEXSCAN:
			return ((IndexScan) iter);
		case HASHJOIN:
			HashIndex joinHashIdx = new HashIndex("HashFileName"
					+ fileNameCtr++);
			hashAndSave(iter, col, joinHashIdx);
			return new IndexScan(iter.schema, joinHashIdx, ((HashJoin)iter).getHeapFile());

		}
		return null;
	}

	public void hashAndSave(Iterator input, int colNo, HashIndex hd) {

		if (input instanceof FileScan) {
			FileScan fs = (FileScan) input;
			while (fs.hasNext()) {
				Tuple t = input.getNext();
				Object colVal = t.getField(colNo);
				hd.insertEntry(new SearchKey(colVal), fs.getLastRID());
			}
		} else if (input instanceof HashJoin) {
			HashJoin hj = (HashJoin) input;
			HeapFile hf = new HeapFile("temp_file_join");
			while (hj.hasNext()) {
				Tuple t = input.getNext();
				Object colVal = t.getField(colNo);
				RID rid = hf.insertRecord(t.getData());
				hd.insertEntry(new SearchKey(colVal), rid);
			}
			hj.setHeapFile(hf);

		} else {
			throw new UnsupportedOperationException("Not implemented");
		}
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		indent(depth);
		// System.out.print("SELECT");
		left.explain(depth + 1);
		right.explain(depth + 1);
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		left.restart();
		right.restart();
		isOpen = true;
		next = null;
		foundNext = false;

		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return isOpen;
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		left.close();
		right.close();
		next = null;
		isOpen = false;
		foundNext = false;
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {

		nextTuple = findNext();
		return !(nextTuple == null);
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {

		return nextTuple;
	}

	public Tuple findNext() {

		if (matchingTuples != null) {
			indexInTupleArray++;
			if (indexInTupleArray < matchingTuples.length) {
				// There are already records that exist that will match so just
				// return the next one
				Tuple joinedTuple = Tuple.join(leftTuple,
						matchingTuples[indexInTupleArray], schema);
				return joinedTuple;
			} else {
				// This means all values for the current outer row is done
				// processing

			}

		}

		// This means all values for the current outer row is done processing.
		// Get the next value from left. If it has the same hash as the previous
		// row, that
		// means they belong to the same bucket and the same HashDupTable can be
		// used for rows in
		// rightScan
		while (leftBucketScan.hasNext()) {

			// leftHashKey = leftBucketScan.getNextHash();
			int nextLeftHash = leftBucketScan.getNextHash();
			leftTupleToMatch = leftBucketScan.getNext();
			boolean newLeftHashId = true;
			if (nextLeftHash == leftHashKey) {
				newLeftHashId = false;
			}
			leftHashKey = nextLeftHash;
			if (newLeftHashId) {
				recreateHashDupForNewHash();
				// TODO may need to restart rightHashIndex here.
			}

			if (rightHashKey != leftHashKey) {
				// get next row in left iter
				continue;
			}
			// Both are pointing to the same bucket now..
			/*switch (leftScanType) {
			case FILESCAN:
				leftTuple = new Tuple(left.schema, ((FileScan) left)
						.getHeapFile().selectRecord(leftRIDToMatch));
				break;
			case KEYSCAN:
				leftTuple = new Tuple(left.schema, ((KeyScan) left)
						.getHeapFile().selectRecord(leftRIDToMatch));
				break;
			case INDEXSCAN:
				leftTuple = new Tuple(left.schema, ((IndexScan) left)
						.getHeapFile().selectRecord(leftRIDToMatch));
				break;
			case HASHJOIN:
				leftTuple = new Tuple(left.schema, ((HashJoin) left)
						.getHeapFile().selectRecord(leftRIDToMatch));

			}*/
			Object fieldVal = leftTuple.getField(lcol);
			SearchKey sk = new SearchKey(fieldVal);
			// All fields in hashTableDup with key = sk are matching the join.
			matchingTuples = hashTableDup.getAll(sk);
			// We have to return tuple 0 index must be incremented before next
			// retrival
			// TODO check if #tuples < index
			indexInTupleArray = 0;
			if (matchingTuples == null) {
				System.out.println("No Matching keys found for "
						+ fieldVal.toString());
				return null;
			}
			Tuple joinedTuple = Tuple.join(leftTuple,
					matchingTuples[indexInTupleArray], schema);
			return joinedTuple;

		}
		// No matching tuple found
		return null;

	}

	private void recreateHashDupForNewHash() {
		hashTableDup = new HashTableDup();
		boolean trap = false;
		//TODO need to reset iter ehre?
		while (rightBucketScan.hasNext()) {
			
			rightHashKey = rightBucketScan.getNextHash();
			rightTupleToMatch = rightBucketScan.getNext();

			if (leftHashKey == rightHashKey) {
				// Take all rows with this hash key and save in Tuple[]
				// read all right tuples hash and save in hashtable
				Tuple rightTuple = null;
				/*switch (rightScanType) {
				case FILESCAN:
					rightTuple = new Tuple(right.schema, ((FileScan) right)
							.getHeapFile().selectRecord(rightRIDToMatch));
					break;
				case KEYSCAN:
					rightTuple = new Tuple(right.schema, ((KeyScan) right)
							.getHeapFile().selectRecord(rightRIDToMatch));
					break;
				case INDEXSCAN:
					rightTuple = new Tuple(right.schema, ((IndexScan) right)
							.getHeapFile().selectRecord(rightRIDToMatch));
					break;
				case HASHJOIN:
					rightTuple = new Tuple(right.schema, ((HashJoin) right)
							.getHeapFile().selectRecord(rightRIDToMatch));
					break;

				}*/

				Object fieldVal = rightTuple.getField(rcol);
				SearchKey sk = new SearchKey(fieldVal);
				hashTableDup.add(sk, rightTuple);
				trap = true;
			} else {
				if (trap)
					break;
			}

		}
	}

	public Tuple getNextTupleFromIter(Iterator it, ScanType scanType) {
		switch (scanType) {
		case FILESCAN:
			return ((FileScan) it).getNext();
		case KEYSCAN:
			return ((KeyScan) it).getNext();
		case INDEXSCAN:
			((KeyScan) it).getNext();

		}

		return null;
	}

	public void setHeapFile(HeapFile heap) {
		this.file = heap;
	}

	public HeapFile getHeapFile() {
		return file;
	}
} // public class HashJoin extends Iterator
