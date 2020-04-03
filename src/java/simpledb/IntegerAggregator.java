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
    private HashMap<Object, Integer> aggregateIntegers = new HashMap<>();
    private HashMap<Object, Tuple> aggregateTuples;
    private int noGroupingInteger = -65535;
    private Tuple noGroupTuple = null;
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
        if(aFieldType.equals(Type.INT_TYPE)) {
            if (gbField == Aggregator.NO_GROUPING) {
                allTuples.add(tup);
                Type[] types = new Type[1];
                String[] names = new String[1];
                names[0] = aFieldType.name();
                types[0] = aFieldType;
                // 传进来是一个包含了很多其他field的tuple，但我又无法设置一个新的tuple的值 √
                TupleDesc newTupleDesc = new TupleDesc(types, names);
                int value = aggregateManipulation(what, tup);
                noGroupTuple = new Tuple(newTupleDesc);
                IntField intField = new IntField(value);
                noGroupTuple.setField(0, intField);
                return;
            }
            Type[] types = new Type[2];
            String[] names = new String[2];
            names[0] = gbFieldType.name();
            types[0] = gbFieldType;
            names[1] = aFieldType.name();
            types[1] = aFieldType;
            TupleDesc newTupleDesc = new TupleDesc(types, names);
            // tup.resetTupleDesc(newTupleDesc);
            Object gFieldValue = null;
            if (gbFieldType.equals(Type.STRING_TYPE)){
                gFieldValue = ((StringField) tup.getField(gbField)).getValue();
            }
            if (gbFieldType.equals(Type.INT_TYPE)){
                gFieldValue = ((IntField) tup.getField(gbField)).getValue();
            }
            int aFieldInteger = ((IntField) tup.getField(afield)).getValue();
            if(what.toString().equals("avg")) {
                allTuples.add(tup);
            }
            Tuple tuple = new Tuple(newTupleDesc);
            tuple.setField(gbField, tup.getField(gbField));
            if(aggregateTuples.size() == 0 || !aggregateIntegers.containsKey(gFieldValue)) {
                if(what.toString().equals("count")) {
                    aggregateIntegers.put(gFieldValue, 1);
                    tuple.setField(afield, new IntField(1));
                    aggregateTuples.put(gFieldValue, tuple);
                } else {
                    aggregateIntegers.put(gFieldValue, aFieldInteger);
                    tuple.setField(afield, new IntField(aFieldInteger));
                    aggregateTuples.put(gFieldValue, tuple);
                }
            } else {
                int newInteger = aggregateManipulation(what, tup);
                int oldInteger = aggregateIntegers.get(gFieldValue);
                aggregateIntegers.replace(gFieldValue, oldInteger, newInteger);
                if (newInteger != oldInteger) {
                    tuple.setField(1, new IntField(newInteger));
                    aggregateTuples.put(gFieldValue, tuple);
                }
            }
        }
    }

    private int aggregateManipulation (Op what, Tuple tuple) throws NoSuchFieldException {
        int toAggregate = ((IntField)tuple.getField(afield)).getValue();
        if (gbField == Aggregator.NO_GROUPING) {
            if (noGroupingInteger == -65535){
                noGroupingInteger = toAggregate;
                return noGroupingInteger;
            }
            switch (what) {
                case MIN: return Math.min(noGroupingInteger, toAggregate);
                case MAX: return Math.max(noGroupingInteger, toAggregate);
                case COUNT: return allTuples.size();
                case SUM: {
                    int sum = 0;
                    for (Tuple tup : allTuples) {
                        sum += ((IntField) tup.getField(afield)).getValue();
                    }
                    return sum;
                }
                case AVG: {
                    int sum = 0, count = allTuples.size();
                    for (Tuple tup: allTuples) {
                        sum += ((IntField) tup.getField(afield)).getValue();
                    }
                    return sum/count;
                }
            }
        }
        Object gFieldType = tuple.getField(gbField).getType();
        Object gFieldValue;
        if (gFieldType.equals(Type.INT_TYPE)) {
            gFieldValue = ((IntField) tuple.getField(gbField)).getValue();
        }
        else {
            gFieldValue = ((StringField) tuple.getField(gbField)).getValue();
        }
        switch (what) {
            case MIN: return Math.min(aggregateIntegers.get(gFieldValue), toAggregate);
            case MAX: return Math.max(aggregateIntegers.get(gFieldValue), toAggregate);
            case COUNT: return aggregateIntegers.get(gFieldValue) + 1;
            case SUM: return aggregateIntegers.get(gFieldValue) + toAggregate;
            case AVG: {
                int sum = 0, count = 0;
                for (Tuple tup: allTuples) {
                    Object tupGbFieldValue;
                    if (gFieldType.equals(Type.INT_TYPE)) {
                        tupGbFieldValue = ((IntField) tup.getField(gbField)).getValue();
                    } else {
                        tupGbFieldValue = ((StringField) tup.getField(gbField)).getValue();
                    }
                    if (tupGbFieldValue.equals(gFieldValue)) {
                        sum += ((IntField) tup.getField(afield)).getValue();
                        count++;
                    }
                }
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
        if (gbField==Aggregator.NO_GROUPING) {
            TupleDesc tupleDesc = noGroupTuple.getTupleDesc();
            LinkedList<Tuple> tuples = new LinkedList<>();
            tuples.add(noGroupTuple);
            return new TupleIterator(tupleDesc, tuples);
        }
        Iterator iterator = aggregateIntegers.keySet().iterator();
        LinkedList<Tuple> tuples = new LinkedList<>();
        while (iterator.hasNext()) {
            tuples.add(aggregateTuples.get(iterator.next()));
        }
        TupleDesc tupleDesc = tuples.get(0).getTupleDesc();
        return new TupleIterator(tupleDesc, tuples);
    }

}
