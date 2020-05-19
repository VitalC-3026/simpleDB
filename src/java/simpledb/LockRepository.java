package simpledb;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LockRepository {
    private ConcurrentHashMap<PageId, List<LockState>> waitingList = new ConcurrentHashMap<>();
    private ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, LockType>> locksList = new ConcurrentHashMap<>();

    public enum LockType {ExclusiveLock, ShareLock, Block, None};

    public class LockState {
        TransactionId tid;
        LockType lockType;

        public LockState(TransactionId tid, LockType lockType) {
            this.tid = tid;
            this.lockType = lockType;
        }
    }

    public synchronized LockType requireShareLock(TransactionId tid, PageId pid){
        switch (isHoldingLock(tid, pid)) {
            case ShareLock: return LockType.ShareLock;
            case ExclusiveLock: {
                if (isWaiting(tid, pid).equals(LockType.None)) {
                    if(waitingList.get(pid) != null){
                        List<LockState> states = waitingList.get(pid);
                        states.add(new LockState(tid, LockType.ShareLock));
                        waitingList.put(pid, states);
                    } else {
                        List<LockState> states = new LinkedList<>();
                        states.add(new LockState(tid, LockType.ShareLock));
                        waitingList.put(pid, states);
                    }
                }
                return LockType.Block;
            }
            case None: {
                if (isWaiting(tid, pid).equals(LockType.ExclusiveLock)) {
                    List<LockState> states = waitingList.get(pid);
                    states.add(new LockState(tid, LockType.ExclusiveLock));
                    waitingList.put(pid, states);
                    return LockType.None;
                } else {
                    return LockType.ShareLock;
                }
            }
        }
        return LockType.ShareLock;
    }

    public synchronized LockType requireExclusiveLock(TransactionId tid, PageId pid){
        switch (isHoldingLock(tid, pid)) {
            case ExclusiveLock: return LockType.ExclusiveLock;
            case ShareLock: {
                if (isWaiting(tid, pid).equals(LockType.None)) {
                    if(waitingList.get(pid) != null){
                        List<LockState> states = waitingList.get(pid);
                        states.add(new LockState(tid, LockType.ShareLock));
                        waitingList.put(pid, states);
                    } else {
                        List<LockState> states = new LinkedList<>();
                        states.add(new LockState(tid, LockType.ShareLock));
                        waitingList.put(pid, states);
                    }
                }
                return LockType.Block;
            }
            case None: {
                if (isWaiting(tid, pid).equals(LockType.ShareLock)) {
                    List<LockState> states = waitingList.get(pid);
                    states.add(new LockState(tid, LockType.ExclusiveLock));
                    waitingList.put(pid, states);
                    return LockType.None;
                } else {
                    return LockType.ExclusiveLock;
                }
            }
        }
        return LockType.ExclusiveLock;
    }

    public synchronized LockType isHoldingLock(TransactionId tid, PageId pid){
        if (locksList.containsKey(pid)) {
            ConcurrentHashMap<TransactionId, LockType> state = locksList.get(pid);
            if (state.containsKey(tid)) {
                return state.get(tid);
            }
        }
        return LockType.None;
    }

    public synchronized LockType isWaiting(TransactionId tid, PageId pid) {
        if (waitingList.containsKey(pid)){
            List<LockState> lockState = waitingList.get(pid);
            for (LockState state: lockState) {
                if (state.tid.equals(tid)) {
                    return state.lockType;
                }
            }
            return LockType.None;
        }
        return LockType.None;
    }

}

