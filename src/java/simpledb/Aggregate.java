package simpledb;

import java.io.IOException;
import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private List<OpIterator> children;
    private Tuple outputTuple = null;
    private boolean agg = false;
    private IntegerAggregator integerAggregator = null;
    private StringAggregator stringAggregator = null;
    private OpIterator result;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        children = new LinkedList<>();
        children.add(child);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
        if (gfield == -1) {
            return Aggregator.NO_GROUPING;
        }
	    return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() throws NoSuchFieldException {
	// some code goes here
        if (gfield == -1) {
            return null;
        }
        // what if the child hasn't opened yet
	    // return this.child.next().getField(gfield).toString();
        return outputTuple.getField(gfield).toString();
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	    return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() throws NoSuchFieldException {
	// some code goes here
        // return this.child.next().getField(afield).toString();
        return outputTuple.getField(afield).toString();
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	    return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException, NoSuchFieldException, IOException {
	// some code goes here
        this.child.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException, NoSuchFieldException, IOException {
	// some code goes here
        if (!agg) {
            agg = true;
            while (child.hasNext()) {
                outputTuple = child.next();
                if(outputTuple.getField(afield).getType().equals(Type.INT_TYPE)) {
                    if(integerAggregator == null){
                        Type gType;
                        if(gfield == Aggregator.NO_GROUPING) {
                            gType = Type.INT_TYPE;
                        } else {
                            gType = outputTuple.getField(gfield).getType();
                        }
                        integerAggregator = new IntegerAggregator(gfield, gType, afield, aop);
                        integerAggregator.mergeTupleIntoGroup(outputTuple);
                    } else {
                        integerAggregator.mergeTupleIntoGroup(outputTuple);
                    }
                }
                if(outputTuple.getField(afield).getType().equals(Type.STRING_TYPE)) {
                    if(stringAggregator == null) {
                        Type gType;
                        if(gfield == Aggregator.NO_GROUPING) {
                            gType = Type.INT_TYPE;
                        } else {
                            gType = outputTuple.getField(gfield).getType();
                        }
                        stringAggregator = new StringAggregator(gfield, gType, afield, aop);
                        stringAggregator.mergeTupleIntoGroup(outputTuple);
                    } else {
                        stringAggregator.mergeTupleIntoGroup(outputTuple);
                    }
                }
            }
            if(integerAggregator != null) {
                result = integerAggregator.iterator();
                result.open();
            }
            if(stringAggregator != null) {
                result = stringAggregator.iterator();
                result.open();
            }
        }
        if(result.hasNext()) {
            return result.next();
        }
	    return null;
    }

    public void rewind() throws NoSuchElementException, DbException,
            TransactionAbortedException, NoSuchFieldException, IOException {
	// some code goes here
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	    return child.getTupleDesc();
    }

    public void close() {
	// some code goes here
        outputTuple = null;
        result.close();
        child.close();
        super.close();
        agg = false;
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
        OpIterator[] opIterators = new OpIterator[children.size()];
        for (int i = 0; i < children.size(); i++) {
            opIterators[i] = children.get(i);
        }
	    return opIterators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
	    // some code goes here
        for (int i = 0; i < children.length; i++) {
            this.children.add(i, children[i]);
        }
    }
    
}
