package simpledb;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private LinkedList<OpIterator> children = new LinkedList<>();
    private Tuple reTuple;
    private int count = 0;
    private int childPosition = 0;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        children.add(child);
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableId);
        if (!tupleDesc.equals(child.getTupleDesc())) {
            throw new DbException("not match TupleDesc");
        }
        Type type = Type.INT_TYPE;
        reTuple = new Tuple(new TupleDesc(new Type[]{type}, new String[]{null}));
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return reTuple.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException, NoSuchFieldException, IOException, InterruptedException {
        // some code goes here
        super.open();
        child.open();
        count = 0;
        childPosition = 0;
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException, NoSuchFieldException, IOException, InterruptedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException, NoSuchFieldException, IOException, InterruptedException {
        // some code goes here
        if (childPosition < children.size()) {
            child = children.get(childPosition);
            childPosition++;
            while (child.hasNext()) {
                Tuple tuple = child.next();
                try {
                    Database.getBufferPool().insertTuple(tid, tableId, tuple);
                } catch (Exception e) {
                    e.printStackTrace();
                }
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
