package relop;

import global.SearchKey;
import global.RID;
import heap.HeapFile;
import index.HashIndex;
import index.BucketScan;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {

  private HashIndex index;
  private HeapFile file;
  private BucketScan scan;

  private boolean isOpen;

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
    this.schema = schema;
    this.index = index;
    this.file = file;
    init();
  }

  private void init() {
    scan = index.openScan();
    isOpen = true;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    scan.close();
    init();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return isOpen;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    scan.close();
    isOpen = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return isOpen ? scan.hasNext() : false;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    byte[] record = file.selectRecord(scan.getNext());
    if (null == record) throw new IllegalStateException();
    return new Tuple(schema, record);
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
    return scan.getLastKey();
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
    return scan.getNextHash();
  }

} // public class IndexScan extends Iterator
