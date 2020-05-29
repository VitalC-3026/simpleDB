package simpledb;

import com.sun.corba.se.impl.orbutil.concurrent.Sync;

import java.io.*;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages = DEFAULT_PAGES;

    private HashMap<PageId, Page> bufferPoolEdit;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private LockRepository lockRepository = new LockRepository();

    private final int blockTime;

    public int getBufferPool() {
        return bufferPoolEdit.size();
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.bufferPoolEdit = new HashMap<>();
        this.blockTime = 500;
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws IOException, NoSuchFieldException, DbException, InterruptedException, TransactionAbortedException {
        // some code goes here
        long start = System.currentTimeMillis();
        LockRepository.LockType result = null;
        if(perm.toString().equals(Permissions.READ_ONLY.toString())) {
            result = lockRepository.requireShareLock(tid, pid, perm);
        } else {
            result = lockRepository.requireExclusiveLock(tid, pid, perm);
        }
        System.out.println(tid.toString()+" "+pid.toString()+" "+perm.toString()+" "+result);
        while(result.equals(LockRepository.LockType.Block)) {
            Thread.sleep(blockTime);
            if(perm.toString().equals(Permissions.READ_ONLY.toString())){
                result = lockRepository.requireShareLock(tid, pid, perm);
            } else {
                result = lockRepository.requireExclusiveLock(tid, pid, perm);
            }
            long end = System.currentTimeMillis();
            if (end - start > blockTime * 20) {
                throw new TransactionAbortedException();
            }
            System.out.println("another try: "+tid.toString()+" "+pid.toString()+" "+perm.toString()+" "+result);
        }
        // System.out.println(bufferPoolEdit.values());
        if (bufferPoolEdit.containsKey(pid)) {
            return bufferPoolEdit.get(pid);
        }
        if (bufferPoolEdit.size() < numPages) {
            if (bufferPoolEdit.containsKey(pid)) {
                return bufferPoolEdit.get(pid);
            } else {
                Page newPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                bufferPoolEdit.put(pid, newPage);
                return newPage;
            }
        } else {
            evictPage();
            Page newPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            bufferPoolEdit.put(pid, newPage);
            return newPage;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) throws DbException {
        // some code goes here
        // not necessary for lab1|lab2
        if (!lockRepository.unlock(tid, pid)) {
            throw new DbException("release a non-existent lock");
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public synchronized void transactionComplete(TransactionId tid) throws IOException, NoSuchFieldException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        LockRepository.LockType res = lockRepository.isHoldingLock(tid, p);
        return !res.equals(LockRepository.LockType.None);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit)
            throws IOException, NoSuchFieldException {
        // some code goes here
        // not necessary for lab1|lab2
        // 将当前tid下所有的锁去掉
        ArrayList<PageId> pages = lockRepository.releaseAllLock(tid);
        if (commit) {
            // 将当前tid下所有的page写入磁盘
            flushPages(tid);
            /*for (PageId pageId: pages) {
                if (bufferPoolEdit.containsKey(pageId)) {
                    DbFile file = Database.getCatalog().getDatabaseFile(pageId.getTableId());
                    Page page = bufferPoolEdit.get(pageId);
                    page.markDirty(false, tid);
                    file.writePage(page);
                } else {
                    throw new IllegalArgumentException("Dirty page is not in the buffer pool");
                }
            }*/

        } else {
            // 将当前tid下所有的page都从磁盘中重新获取
            for (PageId pageId: pages) {
                if (bufferPoolEdit.containsKey(pageId)) {
                    DbFile file = Database.getCatalog().getDatabaseFile(pageId.getTableId());
                    Page page = file.readPage(pageId);
                    bufferPoolEdit.replace(pageId, page);
                }
            }
        }

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException, NoSuchFieldException, InterruptedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = file.insertTuple(tid, t);
        // 重写了insertTuple说明markDirty还需要在buffer pool进行
        for (int i = 0; i < pages.size(); i++) {
            pages.get(i).markDirty(true, tid);
            bufferPoolEdit.put(pages.get(i).getId(), pages.get(i));
            // flushPage(pages.get(i).getId());
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException, NoSuchFieldException, InterruptedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> pages = file.deleteTuple(tid, t);
        for (int i = 0; i < pages.size(); i++) {
            pages.get(i).markDirty(true, tid);
            bufferPoolEdit.put(pages.get(i).getId(), pages.get(i));
            // flushPage(pages.get(i).getId());
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException, NoSuchFieldException {
        // some code goes here
        // not necessary for lab1
        for (PageId pageId : bufferPoolEdit.keySet()) {
            flushPage(pageId);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        bufferPoolEdit.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException, NoSuchFieldException {
        // some code goes here
        // not necessary for lab1
        if (bufferPoolEdit.containsKey(pid)) {
            Page page = bufferPoolEdit.get(pid);
            TransactionId tid = page.isDirty();
            if (tid != null) {
                DbFile file = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                page.markDirty(false, null);
                file.writePage(page);
            }
        } else {
            throw new NullPointerException("page not in buffer pool");
        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException, NoSuchFieldException {
        // some code goes here
        // not necessary for lab1|lab2
        Iterator<PageId> idIterator = bufferPoolEdit.keySet().iterator();
        while (idIterator.hasNext()) {
            PageId pageId = idIterator.next();
            Page page = bufferPoolEdit.get(pageId);
            if (tid.equals(page.isDirty())) {
                flushPage(pageId);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException, IOException, NoSuchFieldException {
        // some code goes here
        // not necessary for lab1
        Iterator<PageId> it = bufferPoolEdit.keySet().iterator();
        while(it.hasNext()) {
            PageId pageId = it.next();
            Page page = bufferPoolEdit.get(pageId);
            if (page.isDirty() == null) {
                flushPage(pageId);
                // System.out.println("evict:"+pageId);
                it.remove();
                return;
            }
        }
        throw new DbException("all pages in the buffer pool are dirty");
    }
}
