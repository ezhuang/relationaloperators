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
	private static int globalfnamecounter = 0;
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

	private Tuple rightTupleToMatch = null;
	private Tuple leftTupleToMatch = null;

	private HashTableDup hashTableDup = null;

	// TODO : Assume join is always equi
	private boolean isOpen = false;

	private static int fileNameCtr = 0;
	private ScanType leftScanType = null;;
	private ScanType rightScanType = null;

	private Tuple nextTuple = null;

	/**
	 * Constructs a hash join, given the left and right iterators and which
	 * columns to match (relative to their individual schemas).
	 */
	public HashJoin(Iterator left, Iterator right, Integer lcol, Integer rcol) {
		this.lcol = lcol; // = new Integer(lcol);
		this.rcol = rcol; // = new Integer(rcol);
		left.restart();
		right.restart();
		leftScanType = ScanType.getScanType(left);
		rightScanType = ScanType.getScanType(right);
		this.schema = Schema.join(left.schema, right.schema);

		createBucketScan(left, right, lcol, rcol);

		// TODO just assign ref or create new obj
		this.left = left;
		this.right = right;
		left.restart();
		right.restart();

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
			hashAndSave(iter, col, hashIdx, null);
			return new IndexScan(iter.schema, hashIdx, ((FileScan) iter).getHeapFile());
			// break; 
		case KEYSCAN:
			return new IndexScan(iter.schema,((KeyScan) iter).getHashIndex(),((KeyScan) iter).getHeapFile()); 
		case INDEXSCAN:
			return ((IndexScan) iter);
		default:
//			HashIndex joinHashIdx = new HashIndex("HashFileName"
//					+ fileNameCtr++);
			HashIndex joinHashIdx = new HashIndex(null);
			HeapFile heapFile = new HeapFile(null);
			hashAndSave(iter, col, joinHashIdx, heapFile);
			return new IndexScan(iter.schema, joinHashIdx, heapFile);

		}
//		return null;
	}

	public void hashAndSave(Iterator input, int colNo, HashIndex hd, HeapFile hf) {

		if (input instanceof FileScan) {
			FileScan fs = (FileScan) input;
			while (fs.hasNext()) {
				Tuple t = input.getNext();
				Object colVal = t.getField(colNo);
				hd.insertEntry(new SearchKey(colVal), fs.getLastRID());
			}
		} else if (input instanceof HashJoin) {
			HashJoin hj = (HashJoin) input;
			int debugCounter = 0;
			while (hj.hasNext()) {
				Tuple t = input.getNext();
				Object colVal = t.getField(colNo);
				RID rid = hf.insertRecord(t.getData());
				hd.insertEntry(new SearchKey(colVal), rid);
				debugCounter++;
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
	    System.out.println("HASH JOIN");
		left.explain(depth + 1);
		right.explain(depth + 1);
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		left.restart();
		right.restart();
		isOpen = true;
		leftBucketScan.restart();
		rightBucketScan.restart();
		leftHashKey = -1;
		rightHashKey = -2;

		matchingTuples = null;
		indexInTupleArray = 0;

		rightTupleToMatch = null;
		leftTupleToMatch = null;

		hashTableDup = null;

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
		leftBucketScan.close();
		right.close();
		rightBucketScan.close();
		isOpen = false;
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
				Tuple joinedTuple = Tuple.join(leftTupleToMatch,
						matchingTuples[indexInTupleArray], schema);
				return joinedTuple;
			} else {
				// This means all values for the current outer row is done
				// processing
				 
			}

		}
		while (leftBucketScan.hasNext()) {

			int prevLeftHash = leftHashKey;

			leftHashKey = leftBucketScan.getNextHash();
			leftTupleToMatch = leftBucketScan.getNext();
			boolean newLeftHashId = true;
			if (prevLeftHash == leftHashKey) {
				newLeftHashId = false;
			}
			boolean foundMatchingBucket = false;
			if (newLeftHashId) {
				foundMatchingBucket = recreateHashDupForNewHash();
				
			} else {
				foundMatchingBucket = true;
			}

			if (!foundMatchingBucket) {
				// get next row in left iter
				continue;
			}
			Object fieldVal = leftTupleToMatch.getField(lcol);
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
			Tuple joinedTuple = Tuple.join(leftTupleToMatch,
					matchingTuples[indexInTupleArray], schema);
			return joinedTuple;

		}
		// No matching tuple found
		return null;

	}

	private boolean recreateHashDupForNewHash() {
		hashTableDup = new HashTableDup();
		boolean trap = false;
		//TODO replace this 
//		rightBucketScan.restart();
		//TODO need to reset iter ehre?
		if (leftHashKey == rightHashKey && rightTupleToMatch != null) {
			Object fieldVal = rightTupleToMatch.getField(rcol);
			SearchKey sk = new SearchKey(fieldVal);
			hashTableDup.add(sk, rightTupleToMatch);
			trap = true;
		}
			
			while (rightBucketScan.hasNext()) {
			
			rightHashKey = rightBucketScan.getNextHash();
			rightTupleToMatch = rightBucketScan.getNext();

			if (leftHashKey == rightHashKey) {
				// Take all rows with this hash key and save in Tuple[]
				// read all right tuples hash and save in hashtable

				Object fieldVal = rightTupleToMatch.getField(rcol);
				SearchKey sk = new SearchKey(fieldVal);
				hashTableDup.add(sk, rightTupleToMatch);
				trap = true;
			} else {
				if (trap)
					break;
			}

		}
//		if (!trap) {
//			//no tuple matched condition
//			return false;
//		} else {
//			return true;
//		}
		return trap;
		
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
