package relop;

import java.util.ArrayList;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {

  private Iterator iter;
  private ArrayList<Predicate> preds = new ArrayList<Predicate>();
  private boolean foundNext = false;
  private Tuple next = null;

  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator iter, Predicate... preds) {
    this.iter = iter;
    for ( Predicate pred : preds) {
      this.preds.add(pred);
    }
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    System.out.print("SELECT * WHERE ");
    java.util.Iterator<Predicate> itr = preds.iterator();
    while (itr.hasNext()) {
      System.out.print(((Predicate)itr.next()).toString());
    }
    System.out.println("");
    iter.explain(depth+1);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    iter.restart();
    foundNext = false;
    next = null;
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return iter.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    iter.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    if (false == iter.hasNext()) return false;
    if (false == foundNext) findNext();
    return foundNext;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    if (false == iter.hasNext()) throw new IllegalStateException();
    if (false == foundNext) findNext();
    if (false == foundNext) throw new IllegalStateException();
    return next;
  }

  private void findNext() {
    foundNext = false;
    while (!foundNext && iter.hasNext()) {
      Tuple t = iter.getNext();
      if (true == qualify(t)) {
        next = t;
        foundNext = true;
        return;
      }
    }
  }

  private boolean qualify(Tuple t) {
    boolean retVal = false;
    java.util.Iterator<Predicate> itr = preds.iterator();
    while (itr.hasNext()) {
      Predicate p = itr.next();
      retVal |= p.evaluate(t);
    }
    return retVal;
  }

} // public class Selection extends Iterator
