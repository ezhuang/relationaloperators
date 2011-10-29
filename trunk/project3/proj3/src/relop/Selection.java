package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {

  private Iterator iter = null;
  private Predicate[] preds = null;
  private boolean foundNext = false;
  private Tuple next = null;
  private boolean isOpen = false;

  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator iter, Predicate... preds) {
    this.iter = iter;
    this.preds = preds;
    this.schema = iter.schema;
    iter.restart();
    isOpen = true;
    foundNext = false;
    next = null;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    indent(depth);
    System.out.print("SELECT");
    iter.explain(depth+1);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    iter.restart();
    isOpen = true;
    next = null;
    foundNext = false;
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
    iter.close();
    next = null;
    isOpen = false;
    foundNext = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    if (false == foundNext) findNext();
    return foundNext;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    if (false == foundNext) { 
      findNext();
    }
    if (false == foundNext) throw new IllegalStateException();
    foundNext = false;
    return next;
  }

  private void findNext() {
    foundNext = false;
    while (!foundNext && iter.hasNext()) {
      Tuple t = iter.getNext();
      if (true == qualify(t)) {
        next = t;
        foundNext = true;
      }
    }
  }

  private boolean qualify(Tuple t) {
    boolean retVal = false;
    for (Predicate p : preds) {
      if(true == p.evaluate(t)) {
        retVal = true;
        break;
      }
    }
    return retVal;
  }

} // public class Selection extends Iterator
