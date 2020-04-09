package simpledb;

import java.io.IOException;
import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {
    private Predicate p;
    private OpIterator child;
    private List<OpIterator> opIterators;
    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.p = p;
        this.child = child;
        opIterators = new LinkedList<>();
        opIterators.add(this.child);
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException, NoSuchFieldException, IOException {
        // some code goes here
        super.open();
        this.child.open();
    }

    public void close() {
        // some code goes here
        this.child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException, NoSuchFieldException, IOException {
        // some code goes here
        close();
        open();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException, NoSuchFieldException, IOException {
        // some code goes here
        while (this.child.hasNext()) {
            Tuple tuple = this.child.next();
            if (this.p.filter(tuple)) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] children = new OpIterator[opIterators.size()];
        for (int i = 0; i < children.length; i++) {
            children[i] = opIterators.get(i);
        }
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        for (int i = 0; i < children.length; i++) {
            opIterators.add(i,children[i]);
        }

    }

}
