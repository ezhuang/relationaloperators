package relop;

import global.SearchKey;
import index.BucketScan;
import index.HashIndex;

/**
 * Implements the hash-based join algorithm described in section 14.4.3 of the
 * textbook (3rd edition; see pages 463 to 464). HashIndex is used to partition
 * the tuples into buckets, and HashTableDup is used to store a partition in
 * memory during the matching phase.
 */
public class HashJoin extends Iterator {

	private Iterator left = null;
	private Iterator right = null;
	private Integer lcol = null;
	private Integer rcol = null;

	private BucketScanWrapper leftBucketScan = null;
	private BucketScanWrapper rightBucketScan = null;

	private HashTableDup hashTabDupLeft = new HashTableDup();
	private HashTableDup hashTabDupright = new HashTableDup();

	// TODO : Assume join is always equi
	private boolean foundNext = false;
	private Tuple next = null;
	private boolean isOpen = false;

	private int fileNameCtr = 0;

	/**
	 * Constructs a hash join, given the left and right iterators and which
	 * columns to match (relative to their individual schemas).
	 */
	public HashJoin(Iterator left, Iterator right, Integer lcol, Integer rcol) {
		// this.left = left;
		// this.right = right;
		this.lcol = lcol; // = new Integer(lcol);
		this.rcol = rcol; // = new Integer(rcol);
		createIndex(left, right, lcol, rcol);
		
		//TODO just assign ref or create new obj
		this.left = left;
		this.right = right;
		
	}

	public void createIndex(Iterator left, Iterator right, int lcol, int rcol) {
		this.leftBucketScan = createBucketScan(left, lcol);
		this.rightBucketScan = createBucketScan(right, rcol);

	}

	private BucketScanWrapper createBucketScan(Iterator iter, int col) {
		if (iter instanceof FileScan) {
			HashIndex hashIdx = new HashIndex("HashFileName" + fileNameCtr++);
			hashAndSave(iter, col, hashIdx);
			return new BucketScanWrapper(hashIdx);// hashIdx);
		} else if (iter instanceof KeyScan) {
			// TODO what if the KeyScan is on a diff column?
			return new BucketScanWrapper(((KeyScan) iter).getHashIndex());
		} else if (iter instanceof IndexScan) {
			return new BucketScanWrapper(((IndexScan) iter).getHashIndex());

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

		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		throw new UnsupportedOperationException("Not implemented");
	}

} // public class HashJoin extends Iterator
