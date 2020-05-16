package simpledb;

import java.util.concurrent.ConcurrentHashMap;

public class LockRepository {
    private ConcurrentHashMap<TransactionId, PageId> readLocks = new ConcurrentHashMap<>();
    private ConcurrentHashMap<PageId, TransactionId> writeLocks = new ConcurrentHashMap<>();

    public static enum LockType {ExclusiveLock, ShareLock, Block};

    public LockType requireShareLock(TransactionId tid, PageId pid){
        if (writeLocks.containsKey(pid)) {
            return LockType.Block;
        }
        if (readLocks.containsKey(tid) && readLocks.size() == 1) {
            writeLocks.put(pid, tid);
            readLocks.remove(tid, pid);
            return requireExclusiveLock(tid, pid);
        } else if (readLocks.containsKey(tid))
        readLocks.put(tid, pid);
        return LockType.ShareLock;
    }

    public LockType requireExclusiveLock(TransactionId tid, PageId pid){
        if (readLocks.containsKey(tid)){
            return LockType.Block;
        }
        if (writeLocks.containsKey(pid)){
            return LockType.Block;
        }
        writeLocks.put(pid, tid);
        return LockType.ExclusiveLock;
    }

}

