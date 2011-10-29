package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

  private Iterator iter = null;
  private Integer[] fields = null;
  private boolean isOpen = false;

  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
    this.iter = iter;
    this.fields = fields;
    this.schema = new Schema(fields.length);
    int ind = 0;
    for (Integer i : fields) {
      schema.initField(ind++, iter.schema, i);
    }
    iter.restart();
    isOpen = true;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    indent(depth);
    System.out.print("PROJECT");
    iter.explain(depth+1);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    iter.restart();
    isOpen = true;
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
    isOpen = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return iter.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    if (false == hasNext()) throw new IllegalStateException();

    Tuple t = iter.getNext();
    Tuple retVal = new Tuple(schema);
    int ind = 0;
    for (Integer i : fields) {
      retVal.setField(ind++, t.getField(i));
    }

    return retVal;
  }

} // public class Projection extends Iterator
