package simpledb;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;
    private Type gbFieldType;
    private int afield;
    private Op what;
    private HashMap<Op, List<Tuple>> Grouping = new HashMap<>();
    private HashMap<Op, Type> GroupTd;
    private List<Tuple> noGrouping;
    private Type noGroupAType;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.afield = afield;
        this.gbFieldType = gbfieldtype;
        this.what = what;
        if(gbfield != Aggregator.NO_GROUPING) {
            Grouping.put(what, new LinkedList<Tuple>());
            GroupTd = new HashMap<>();
        } else {
            noGrouping = new LinkedList<>();
        }

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) throws NoSuchFieldException {
        // some code goes here
        Type aFieldType = tup.getField(afield).getType();
        Type gFieldType = tup.getField(gbField).getType();
        if(gFieldType.equals(gbFieldType)) {
            if (gbField == Aggregator.NO_GROUPING && noGroupAType.equals(aFieldType)) {
                Type[] types = new Type[1];
                String[] names = new String[1];
                names[0] = aFieldType.name();
                types[0] = aFieldType;
                TupleDesc newTupleDesc = new TupleDesc(types, names);
                tup.resetTupleDesc(newTupleDesc);
                noGrouping.add(tup);
                return;
            }
            Type[] types = new Type[2];
            String[] names = new String[2];
            names[0] = gFieldType.name();
            types[0] = gFieldType;
            names[1] = aFieldType.name();
            types[1] = aFieldType;
            TupleDesc newTupleDesc = new TupleDesc(types, names);
            tup.resetTupleDesc(newTupleDesc);
            if(GroupTd.containsKey(what) && Grouping.containsKey(what)) {
                if (aFieldType.equals(GroupTd.get(what))) {
                    LinkedList tuples = (LinkedList) Grouping.get(what);
                    tuples.add(tup);
                    Grouping.put(what, tuples);
                }
            } else {
                GroupTd.put(what, aFieldType);
                LinkedList<Tuple> tuples = new LinkedList<>();
                tuples.add(tup);
                Grouping.put(what, tuples);
            }
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // throw new UnsupportedOperationException("please implement me for lab2");
        if (gbField == Aggregator.NO_GROUPING) {
            TupleDesc tupleDesc = noGrouping.get(0).getTupleDesc();
            return new TupleIterator(tupleDesc, noGrouping);
        }
        else {
            TupleDesc tupleDesc = Grouping.get(what).get(0).getTupleDesc();
            return new TupleIterator(tupleDesc, Grouping.get(what));
        }
    }

}
