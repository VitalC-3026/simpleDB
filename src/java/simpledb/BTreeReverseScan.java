package simpledb;

import net.sf.antcontrib.logic.condition.IsPropertyFalse;

import java.io.IOException;
import java.util.*;

/**
 * BTreeReverseScan is an operator which reads tuples in sorted order
 * according to a predicate
 */
public class BTreeReverseScan implements OpIterator{

    private boolean isOpen = false;
    private TransactionId tid;
    private int tableId;
    private IndexPredicate indexPredicate;
    private String tableName;
    private String alias = "";
    private TupleDesc tupleDesc;
    // 不被序列化，也就是只存于内存中
    private transient DbFileIterator it;

    /**
     * Creates a B+ tree scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     * @param ipred
     * 			  The index predicate to match. If null, the scan will return all tuples
     */
    public BTreeReverseScan(TransactionId tid, int tableid, String tableAlias, IndexPredicate ipred)
            throws TransactionAbortedException, NoSuchFieldException, DbException, IOException {
        this.tid = tid;
        this.indexPredicate = ipred;
        this.tableId = tableid;
        reset(tableid,tableAlias);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return this.tableName;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        return this.alias;
    }

    /**
     * Returns the TupleDesc with field names from the underlying BTreeFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying BTreeFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) throws TransactionAbortedException, IOException, DbException, NoSuchFieldException {
        this.isOpen=false;
        this.alias = tableAlias;
        this.tableName = Database.getCatalog().getTableName(tableid);
        if(indexPredicate == null) {
            this.it = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
        }
        else {
            this.it = ((BTreeFile) Database.getCatalog().getDatabaseFile(tableid)).indexIterator(tid, indexPredicate);
        }
        tupleDesc = Database.getCatalog().getTupleDesc(tableid);
        String[] newNames = new String[tupleDesc.numFields()];
        Type[] newTypes = new Type[tupleDesc.numFields()];
        for (int i = 0; i < tupleDesc.numFields(); i++) {
            String name = tupleDesc.getFieldName(i);
            Type t = tupleDesc.getFieldType(i);

            newNames[i] = tableAlias + "." + name;
            newTypes[i] = t;
        }
        tupleDesc = new TupleDesc(newTypes, newNames);
    }

    @Override
    public void open() throws DbException, TransactionAbortedException, NoSuchFieldException, IOException {

    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException, NoSuchFieldException, IOException {
        return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException, NoSuchFieldException, IOException {
        return null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException, IOException, NoSuchFieldException {

    }

    @Override
    public void close() {

    }
}
