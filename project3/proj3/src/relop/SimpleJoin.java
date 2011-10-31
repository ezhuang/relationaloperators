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
  private Tuple lTuple = null;
  /**
   * Constructs a join, given the left and right iterators and join predicates
   * (relative to the combined schema).
   */
  public SimpleJoin(Iterator left, Iterator right, Predicate... preds) {
    this.left = left;
    this.right = right;
    this.preds = preds;
    this.schema = Schema.join(left.schema, right.schema);
    init(); 
  }

  private void init() {
    left.restart();
    right.restart();
    isOpen = true;
    next = null;
    lTuple = null;
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
    Tuple rTuple = null;
    
    if (null == lTuple) {
      if (true == left.hasNext()) 
        lTuple = left.getNext();
      else 
        return;
    }

    if (right.hasNext()) {
      rTuple = right.getNext();
    }
    else {
      right.restart();
      
      if (right.hasNext()) 
        rTuple = right.getNext();
      else 
        findNext();
      
      if (left.hasNext()) 
        lTuple = left.getNext();
      else
        return;
    }

    Tuple t = Tuple.join(lTuple, rTuple, schema);
    if (true == qualify(t)) {
      next = t;
      foundNext = true;
    }
    else
      findNext();
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

} // public class SimpleJoin extends Iterator
