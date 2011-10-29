package relop;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends Iterator {

  private Iterator left = null,
                   right = null;
  private Predicate[] preds = null;
  private boolean isOpen = false;
  private boolean foundNext = false;
  private Tuple next = null;
  /**
   * Constructs a join, given the left and right iterators and join predicates
   * (relative to the combined schema).
   */
  public SimpleJoin(Iterator left, Iterator right, Predicate... preds) {
    this.left = left;
    this.right = right;
    this.preds = preds;
    init(); 
  }

  private void init() {
    left.restart();
    right.restart();
    isOpen = true;
    next = null;
    foundNext = false;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    indent(depth);
    System.out.println("SIMPLE JOIN");
    left.explain(depth+1);
    right.explain(depth+1);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
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
    left.close();
    right.close();
    isOpen = false;
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
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    throw new UnsupportedOperationException("Not implemented");
  }

  private void findNext() {

  }

} // public class SimpleJoin extends Iterator
