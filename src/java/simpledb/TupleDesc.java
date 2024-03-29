package simpledb;

import org.omg.CORBA.IDLType;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private List<TDItem> items;

    public List<TDItem> getItems() {
        return items;
    }

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // create TDItem with two arrays named typeAr and fieldAr
        items = new LinkedList<>();
        for (int i = 0; i<typeAr.length; i++){
            TDItem item = new TDItem(typeAr[i], fieldAr[i]);
            items.add(item);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // anonymous fields -> null
        items = new ArrayList<>();
        for (int i = 0; i<typeAr.length; i++){
            TDItem item = new TDItem(typeAr[i], null);
            items.add(item);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= items.size() || i < 0) {
            throw new NoSuchElementException();
        }
        return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= items.size() || i < 0) {
            throw new NoSuchElementException();
        }
        return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // if name == null, there might be more than one fields.
        if (name != null) {
            for (int i = 0; i < items.size(); i++) {
                if (name.equals(items.get(i).fieldName)) {
                    return i;
                }
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int sum = 0;
        for (int i = 0; i< items.size(); i++) {
            sum += getFieldType(i).getLen();
        }
        return sum;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        List<TDItem> TDItems = new ArrayList<>();
        if(td1 != null) {
            TDItems.addAll(td1.items);
            if (td2 != null) {
                TDItems.addAll(td1.items.size(),td2.items);
            }
        } else {
            if (td2 != null) {
                TDItems.addAll(td2.items);
            }
        }


        Type[] typeAr = new Type[TDItems.size()];
        String[] fieldAr = new String[TDItems.size()];
        for (int i = 0; i < TDItems.size(); i++) {
            typeAr[i] = TDItems.get(i).fieldType;
            fieldAr[i] = TDItems.get(i).fieldName;
        }
        TupleDesc tupleDesc = new TupleDesc(typeAr, fieldAr);
        return tupleDesc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        // 判断两个对象的值是否相同
        // Type: 可以直接用equals()吗?
        if (o instanceof TupleDesc) {
             if (items.size() == ((TupleDesc) o).items.size()) {
                 for (int i = 0; i < items.size(); i++) {
                     if (!items.get(i).fieldType.equals(((TupleDesc) o).items.get(i).fieldType)) {
                         return false;
                     }
                     if (items.get(i).fieldName == null) {
                         if (((TupleDesc) o).items.get(i).fieldName != null) {
                             if (((TupleDesc) o).items.get(i).fieldName.contains(".")) {
                                 String name = ((TupleDesc) o).items.get(i).fieldName;
                                 int index = name.indexOf(".");
                                 name = name.substring(index + 1);
                                 if (!name.contains("null")) {
                                     return false;
                                 }
                             } else {
                                 return false;
                             }
                         }
                     } else if (!items.get(i).fieldName.equals(((TupleDesc) o).items.get(i).fieldName)) {
                        return false;
                     }
                 }
             } else {
                 return false;
             }
        } else {
            return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder output = new StringBuilder("");
        for (TDItem item: items){
            output.append(item.fieldType).append("(").append(item.fieldName).append(")");
        }
        return output.toString();
    }
}
