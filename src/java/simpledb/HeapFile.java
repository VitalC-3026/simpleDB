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
        if (file == null) return null;
        Page page = null;
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(pid.getPageNumber()*BufferPool.getPageSize());
            randomAccessFile.read(data, 0, data.length);
            page = new HeapPage((HeapPageId) pid, data);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return page;
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
        // System.out.println(file.length());
        int i = (int)(file.length() / Database.getBufferPool().getPageSize());
        return i;
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
        List<HeapPage> pages = new LinkedList<>();
        for(int i = 0; i < numPages(); i++) {
            // System.out.println(numPages());
            HeapPageId heapPageId = new HeapPageId(getId(), i);
            Page page = Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
            pages.add((HeapPage) page);
        }
        class HeapFileIterator implements DbFileIterator {
            private List<HeapPage> pages;
            // private List<Iterator<Tuple>> tuples;
            private Iterator<Tuple> tupleIterator = null;
            private int pagePosition = 0;

            private HeapFileIterator(List<HeapPage> pages){
                this.pages = pages;
            }

            @Override
            public void open() throws DbException, TransactionAbortedException, IOException, NoSuchFieldException {
                /*tuples = new LinkedList<>();
                for(HeapPage page: pages){
                    tuples.add(page.iterator());
                }*/
                tupleIterator = pages.get(0).iterator();
                pagePosition = 0;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException, NoSuchFieldException, IOException {
                if (pages == null) return false;
                if (tupleIterator == null) return false;
                if (!tupleIterator.hasNext() && pagePosition >= pages.size() - 1) return false;
                if (!tupleIterator.hasNext()) {
                    tupleIterator = pages.get(++pagePosition).iterator();
                }
                return tupleIterator.hasNext();

            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException, NoSuchFieldException, IOException {
                if (!hasNext()) throw new NoSuchElementException();
                return tupleIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException, IOException, NoSuchFieldException {
                close();
                open();
            }

            @Override
            public void close() {
                tupleIterator = null;
                // tuples = null;
                pagePosition = 0;
            }
        }
        return new HeapFileIterator(pages);
    }
    /*private class HeapFileIterator implements DbFileIterator {
         private int pagePosition = 0;
         private Iterator<Tuple> tupleIterator;
         private TransactionId tid;

         HeapFileIterator(TransactionId tid) {
             this.tid = tid;
         }

         @Override
         public void open() throws DbException, TransactionAbortedException, IOException, NoSuchFieldException {
             HeapPageId heapPageId = new HeapPageId(getId(), pagePosition);
             HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
             tupleIterator = heapPage.iterator();
         }

         @Override
         public boolean hasNext() throws DbException, TransactionAbortedException, NoSuchFieldException, IOException {
             if (tupleIterator == null) {
                 return false;
             }
             if (tupleIterator.hasNext()) {
                 return true;
             }
             if(pagePosition < numPages() - 1) {
                 HeapPageId heapPageId = new HeapPageId(getId(), ++pagePosition);
                 HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
                 tupleIterator = heapPage.iterator();
                 return tupleIterator.hasNext();
             } else {
                 return false;
             }
         }

         @Override
         public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException, NoSuchFieldException, IOException {
             if (!hasNext()) throw new NoSuchElementException();
             return tupleIterator.next();
         }

         @Override
         public void rewind() throws DbException, TransactionAbortedException, IOException, NoSuchFieldException {
            close();
            open();
         }

         @Override
         public void close() {
            tupleIterator = null;
            pagePosition = 0;
         }
     }*/
}

