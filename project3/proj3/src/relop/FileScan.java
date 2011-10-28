package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

  private HeapScan scan;
  private HeapFile file;
  private Schema schema;

  private boolean isOpen;
  private RID lastRID;

  /**
   * Constructs a file scan, given the schema and heap file.
   */
  public FileScan(Schema schema, HeapFile file) {
    this.schema = schema;
    this.file = file;
    init();
  }

  private void init() {
    scan = file.openScan();
    isOpen = true;
    lastRID = null;
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
    isOpen = false;
    scan.close();
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
    if (false == hasNext()) throw new IllegalStateException();
    return new Tuple(schema, scan.getNext(lastRID));
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
    return lastRID;
  }

} // public class FileScan extends Iterator
