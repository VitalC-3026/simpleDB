package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;
    private Type gbFieldType;
    private int afield;
    private Op what;
    private int count = 0;
    private Type oldType = null;
    private HashMap<Integer, Integer> aggregateCounts;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.afield = afield;
        this.gbFieldType = gbfieldtype;
        this.what = what;
        aggregateCounts = new HashMap<>();
        if(!this.what.toString().equals("count")){
            throw new IllegalArgumentException();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) throws NoSuchFieldException {
        // some code goes here
        Type aFieldType = tup.getField(gbField).getType();
        Type gFieldType = tup.getField(gbField).getType();
        if (oldType == null) {
            oldType = aFieldType;
        } else if (!oldType.equals(aFieldType)) {
            return;
        }
        if(gFieldType.equals(gbFieldType)) {
            if (gbField == Aggregator.NO_GROUPING) {
                count++;
                return;
            }

            int gFieldInteger = ((IntField) tup.getField(gbField)).getValue();
            if(aggregateCounts.size() == 0 || !aggregateCounts.containsKey(gFieldInteger)) {
                aggregateCounts.put(gFieldInteger, 1);
            } else {
                int oldVal = aggregateCounts.get(gFieldInteger);
                aggregateCounts.replace(gFieldInteger, oldVal, oldVal + 1);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() throws NoSuchFieldException {
        // some code goes here
        // throw new UnsupportedOperationException("please implement me for lab2");
        if (gbField == Aggregator.NO_GROUPING) {
            IntField aField = new IntField(count);
            Type[] types = new Type[1];
            types[0] = oldType;
            String[] strings = new String[1];
            strings[0] = oldType.name();
            TupleDesc tupleDesc = new TupleDesc(types, strings);
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, aField);
            ArrayList<Tuple> tuples = new ArrayList<>();
            tuples.add(tuple);
            return new TupleIterator(tupleDesc, tuples);
        } else {
            Iterator<Integer> iterator = aggregateCounts.keySet().iterator();
            Type[] types = new Type[2];
            String[] strings = new String[2];
            types[0] = gbFieldType;
            types[1] = Type.INT_TYPE;
            strings[0] = gbFieldType.name();
            strings[1] = types[1].name();
            TupleDesc tupleDesc = new TupleDesc(types, strings);
            LinkedList<Tuple> tuples = new LinkedList<>();
            while (iterator.hasNext()) {
                Tuple tuple = new Tuple(tupleDesc);
                int count = aggregateCounts.get(iterator.next());
                IntField intField = new IntField(count);
                tuple.setField(1, intField);
                tuples.add(tuple);
            }
            return new TupleIterator(tupleDesc, tuples);
        }
    }
}
