package simpledb;

import org.omg.Messaging.SYNC_WITH_TRANSPORT;

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
    // private ArrayList<Page> pages = new ArrayList<>();
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
    public void writePage(Page page) throws IOException, NoSuchFieldException {
        // some code goes here
        // not necessary for lab1
        if (page == null) return;
        byte[] data = page.getPageData();
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            randomAccessFile.seek(page.getId().getPageNumber()*BufferPool.getPageSize());
            randomAccessFile.write(data, 0, data.length);
        } catch (Exception e) {
            e.printStackTrace();
        }

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
            throws DbException, IOException, TransactionAbortedException, NoSuchFieldException, InterruptedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pages = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId heapPageId = new HeapPageId(getId(), i);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            System.out.println("insert: " + t.toString() + " " +  ((IntField) t.getField(0)).getValue());
            if (heapPage.getNumEmptySlots() > 0) {
                heapPage.insertTuple(t);
                heapPage.markDirty(true, tid);
                pages.add(heapPage);
                break;
            }
        }
        // 创建新的blank paper的时候要先写入文件，再从buffer pool取出来，再insert tuple
        if (pages.size() == 0) {
            HeapPageId newPageId = new HeapPageId(getId(), numPages());
            HeapPage newPage = new HeapPage(newPageId, HeapPage.createEmptyPageData());
            writePage(newPage);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, newPageId, Permissions.READ_WRITE);
            // heapPage.markDirty(true, tid);
            heapPage.insertTuple(t);
            pages.add(heapPage);
        }
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException, IOException, NoSuchFieldException, InterruptedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pages = new ArrayList<>();
        HeapPageId heapPageId = new HeapPageId(getId(), t.getRecordId().getPageId().getPageNumber());
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        pages.add(heapPage);
        heapPage.markDirty(true, tid);
        if (pages.size() == 0) {
            throw new DbException("tuple " + t + " is not in this table");
        }
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) throws TransactionAbortedException, NoSuchFieldException, DbException, IOException {
        // some code goes here
        return new HeapFileIterator(tid);
    }
    private class HeapFileIterator implements DbFileIterator {
         private int pagePosition = 0;
         private Iterator<Tuple> tupleIterator;
         private TransactionId tid;

         HeapFileIterator(TransactionId tid) {
             this.tid = tid;
         }

         @Override
         public void open() throws DbException, TransactionAbortedException, IOException, NoSuchFieldException, InterruptedException {
             HeapPageId heapPageId = new HeapPageId(getId(), pagePosition);
             HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
             tupleIterator = heapPage.iterator();
         }

         @Override
         public boolean hasNext() throws DbException, TransactionAbortedException, NoSuchFieldException, IOException, InterruptedException {
             if (tupleIterator == null) {
                 return false;
             }
             if (tupleIterator.hasNext()) {
                 return true;
             }
             if(pagePosition < numPages() - 1) {
                 HeapPageId heapPageId = new HeapPageId(getId(), ++pagePosition);
                 HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
                 System.out.println(heapPageId);
                 tupleIterator = heapPage.iterator();
                 return tupleIterator.hasNext();
             } else {
                 return false;
             }
         }

         @Override
         public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException, NoSuchFieldException, IOException, InterruptedException {
             if (!hasNext()) throw new NoSuchElementException();
             return tupleIterator.next();
         }

         @Override
         public void rewind() throws DbException, TransactionAbortedException, IOException, NoSuchFieldException, InterruptedException {
            close();
            open();
         }

         @Override
         public void close() {
            tupleIterator = null;
            pagePosition = 0;
         }
     }
}

