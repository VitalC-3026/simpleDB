package simpledb;

import java.io.IOException;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private int tableId;
    private String tableAlias;
    private Tuple next = null;
    private boolean open = false;
    // private HeapFile.HeapFileIterator heapFileIterator;
    private HeapFile heapFile;
    private TupleIterator tupleIterator;
    private DbFileIterator dbFileIterator;
    /**
     * Creates a sequential scan over the specified table as a part of the
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
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias() {
        // some code goes here
        return tableAlias;
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
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableAlias = tableAlias;
        this.tableId = tableid;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableId);
        List<TupleDesc.TDItem> OldTDItem = tupleDesc.getItems();
        Type[] types = new Type[OldTDItem.size()];
        String[] fieldNames = new String[OldTDItem.size()];
        for(int i = 0; i < OldTDItem.size(); i++) {
            Type type = OldTDItem.get(i).fieldType;
            String fieldName = tableAlias + "." + OldTDItem.get(i).fieldName;
            types[i] = type;
            fieldNames[i] = fieldName;
        }
        return new TupleDesc(types, fieldNames);
    }

    public void open() throws DbException, TransactionAbortedException, NoSuchFieldException, IOException {
        // some code goes here
        heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        dbFileIterator = heapFile.iterator(tid);
        dbFileIterator.open();
        this.open = true;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException, NoSuchFieldException, IOException {
        // some code goes here
        if (!this.open)
            throw new IllegalStateException("Operator not yet open");

        if (dbFileIterator == null)
            return false;

        return dbFileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException, NoSuchFieldException, IOException {
        // some code goes here
        if (!this.open)
            throw new IllegalStateException("Operator not yet open");

        if (hasNext()) {
            next = dbFileIterator.next();
            if (next == null)
                throw new NoSuchElementException();
        }

        Tuple result = next;
        next = null;
        return result;
    }

    public void close() {
        // some code goes here
        next = null;
        this.open = false;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        this.open = true;
        next = null;
    }

}
