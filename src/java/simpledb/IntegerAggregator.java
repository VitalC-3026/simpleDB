package simpledb;

import java.util.HashMap;
import java.util.Iterator;
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
    private List<Tuple> allTuples;
    private HashMap<Integer, Integer> aggregateIntegers = new HashMap<>();
    private HashMap<Integer, Tuple> aggregateTuples;
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
        aggregateTuples = new HashMap<>();
        if (what.toString().equals("avg") || gbfield == Aggregator.NO_GROUPING) {
            allTuples = new LinkedList<>();
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
        if(gFieldType.equals(gbFieldType) && aFieldType.equals(Type.INT_TYPE)) {
            if (gbField == Aggregator.NO_GROUPING) {
                Type[] types = new Type[1];
                String[] names = new String[1];
                names[0] = aFieldType.name();
                types[0] = aFieldType;
                // 传进来是一个包含了很多其他field的tuple，但我又无法设置一个新的tuple的值
                TupleDesc newTupleDesc = new TupleDesc(types, names);
                tup.resetTupleDesc(newTupleDesc);
                allTuples.add(tup);
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
            int gFieldInteger = ((IntField) tup.getField(gbField)).getValue();
            int aFieldInteger = ((IntField) tup.getField(afield)).getValue();
            if(what.toString().equals("avg")) {
                allTuples.add(tup);
            }
            if(aggregateTuples.size() == 0 || !aggregateIntegers.containsKey(gFieldInteger)) {
                aggregateIntegers.put(gFieldInteger, aFieldInteger);
                aggregateTuples.put(gFieldInteger, tup);
            } else {
                int newInteger = aggregateManipulation(what, tup);
                int oldInteger = aggregateIntegers.get(gFieldInteger);
                aggregateIntegers.replace(gFieldInteger, oldInteger, newInteger);
                if (newInteger != oldInteger) {
                    tup.setField(1, new IntField(newInteger));
                    aggregateTuples.put(gFieldInteger, tup);
                }
            }
        }
    }

    private int aggregateManipulation (Op what, Tuple tuple) throws NoSuchFieldException {
        int toAggregate = ((IntField)tuple.getField(afield)).getValue();
        int gFieldInteger = ((IntField) tuple.getField(gbField)).getValue();
        switch (what) {
            case MIN: return Math.min(aggregateIntegers.get(gFieldInteger), toAggregate);
            case MAX: return Math.max(aggregateIntegers.get(gFieldInteger), toAggregate);
            case COUNT: return aggregateIntegers.get(gFieldInteger) + 1;
            case SUM: return aggregateIntegers.get(gFieldInteger) + toAggregate;
            case AVG: {
                int sum = 0, count = 0;
                for (Tuple tup: allTuples) {
                    if (((IntField) tup.getField(gbField)).getValue() == gFieldInteger) {
                        sum += ((IntField) tup.getField(afield)).getValue();
                        count++;
                    }
                }
                count++;
                sum += toAggregate;
                return sum/count;
            }
        }
        throw new IllegalArgumentException("Impossible to get here in aggregateManipulation");
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
        Iterator<Integer> iterator = aggregateIntegers.keySet().iterator();
        LinkedList<Tuple> tuples = new LinkedList<>();
        while (iterator.hasNext()) {
            tuples.add(aggregateTuples.get(iterator.next()));
        }
        TupleDesc tupleDesc = tuples.get(0).getTupleDesc();
        return new TupleIterator(tupleDesc, tuples);
    }

}
