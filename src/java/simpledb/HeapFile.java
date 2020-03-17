package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc td;
    private BufferPool bufferPool;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IOException, NoSuchFieldException {
        // some code goes here
        RandomAccessFile io = new RandomAccessFile(file, "rw");
        byte[] data = new byte[BufferPool.getPageSize()];
        io.read(data);
        return new HeapPage((HeapPageId) pid, data);
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException, NoSuchFieldException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) throws TransactionAbortedException, NoSuchFieldException, DbException, IOException {
        // some code goes here
        class HeapFileIterator implements DbFileIterator {
            private List<HeapPage> pages;
            private List<Iterator<Tuple>> tuples;
            private Iterator<Tuple> tupleIterator = null;
            private int i = 0;

            public HeapFileIterator(List<HeapPage> pages){
                this.pages = pages;
            }

            @Override
            public void open() throws DbException, TransactionAbortedException, IOException, NoSuchFieldException {
                tuples = new LinkedList<>();
                for(HeapPage page: pages){
                    tuples.add(page.iterator());
                }
                tupleIterator = tuples.get(0);
                i = 0;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException, NoSuchFieldException, IOException {
                if (tuples == null) return false;
                if (tupleIterator == null && i >= tuples.size()) return false;
                if (tupleIterator == null) {
                    tupleIterator = readNext();
                }
                return tupleIterator.hasNext();
            }

            private Iterator<Tuple> readNext() {
                if(i < tuples.size()){
                    tupleIterator = tuples.get(i);
                } else {
                    tupleIterator = null;
                }
                return tupleIterator;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException, NoSuchFieldException, IOException {
                if (tupleIterator == null && tuples == null) throw new NoSuchElementException();
                if (tupleIterator == null) {
                    tupleIterator = readNext();
                    i++;
                }

                Tuple result = tupleIterator.next();
                return result;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException, IOException, NoSuchFieldException {
                tupleIterator = null;
                i = 0;
            }

            @Override
            public void close() {
                tupleIterator = null;
                tuples = null;
                i = 0;
            }
        }

        List<HeapPage> pages = new LinkedList<>();
        for(int i = 0; i < numPages(); i++) {
            HeapPageId heapPageId = new HeapPageId(getId(), i);
            Page page = Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
            pages.add((HeapPage) page);
        }
        return new HeapFileIterator(pages);
    }

}

