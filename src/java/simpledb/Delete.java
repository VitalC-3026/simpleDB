package simpledb;

import java.io.IOException;
import java.util.LinkedList;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator child;
    private LinkedList<OpIterator> children = new LinkedList<>();
    private Tuple reTuple;
    private int count = 0;
    private int childPosition = 0;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        children.add(child);
        Type type = Type.INT_TYPE;
        TupleDesc tupleDesc = new TupleDesc(new Type[]{type}, new String[]{null});
        reTuple = new Tuple(tupleDesc);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return reTuple.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException, NoSuchFieldException, IOException {
        // some code goes here
        super.open();
        child.open();
        count = 0;
        childPosition = 0;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException, NoSuchFieldException, IOException {
        // some code goes here
        close();
        open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException, NoSuchFieldException, IOException {
        // some code goes here
        if (childPosition < children.size()) {
            child = children.get(childPosition);
            childPosition++;
            while (child.hasNext()) {
                Tuple tuple = child.next();
                Database.getBufferPool().deleteTuple(tid, tuple);
                count++;
            }
            IntField intField = new IntField(count);
            reTuple.setField(0, intField);
            return reTuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] newChildren = new OpIterator[children.size()];
        for (int i = 0; i < children.size(); i++) {
            newChildren[i] = children.get(i);
        }
        return newChildren;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        for (int i = 0; i < children.length; i++) {
            this.children.add(i, children[i]);
        }
    }

}
