package simpledb;

import java.beans.Transient;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LockRepository {
    private ConcurrentHashMap<PageId, List<LockState>> waitingList = new ConcurrentHashMap<>();
    private ConcurrentHashMap<PageId, List<LockState>> locksList = new ConcurrentHashMap<>();

    public enum LockType {ExclusiveLock, ShareLock, Block, None};

    public class LockState {
        TransactionId tid;
        LockType lockType;

        public LockState(TransactionId tid, LockType lockType) {
            this.tid = tid;
            this.lockType = lockType;
        }

        public boolean equals(TransactionId tid) {
            return tid.equals(this.tid);
        }

        public boolean equals(TransactionId tid, LockType lockType){
            return tid.equals(this.tid) && lockType.equals(this.lockType);
        }
    }

    public synchronized LockType requireShareLock(TransactionId tid, PageId pid) throws DbException {
        switch (isHoldingLock(tid, pid)) {
            case ShareLock: {
                List<LockState> lockStateList = locksList.get(pid);
                if (lockStateList.size() == 1) {
                    LockState state = lockStateList.iterator().next();
                    state.lockType = LockType.ExclusiveLock;
                    List<LockState> stateList = new ArrayList<>();
                    stateList.add(state);
                    locksList.replace(pid, stateList);
                    return LockType.ExclusiveLock;
                }
                return LockType.ShareLock;
            }
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
                    states.add(new LockState(tid, LockType.ShareLock));
                    waitingList.put(pid, states);
                    // 是否可以加入这个排它锁到locksList？
                    return LockType.Block;
                } else if ((isWaiting(tid, pid).equals(LockType.None))){
                    if (waitingList.containsKey(pid) && (waitingList.get(pid) != null || waitingList.get(pid).size() != 0)) {
                        List<LockState> states = waitingList.get(pid);
                        for (LockState state: states) {
                            if (!state.tid.equals(tid) && state.lockType.equals(LockType.ExclusiveLock)) {
                                // 是否可以加入这个排它锁到locksList里？
                                states.add(new LockState(tid, LockType.ShareLock));
                                waitingList.replace(pid, states);
                                return LockType.Block;
                            }
                        }
                    }
                    lock(tid, pid, LockType.ShareLock);
                } else {
                    List<LockState> states = waitingList.get(pid);
                    states.remove(new LockState(tid, LockType.ShareLock));
                    waitingList.put(pid, states);
                    lock(tid, pid, LockType.ShareLock);
                }
                return LockType.ShareLock;
            }
        }
        throw new DbException("Impossible to reach here");
    }

    public synchronized LockType requireExclusiveLock(TransactionId tid, PageId pid) throws DbException {
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
                    return LockType.Block;
                } else if ((isWaiting(tid, pid).equals(LockType.None))){
                    if (waitingList.containsKey(pid) && (waitingList.get(pid) != null || waitingList.get(pid).size() != 0)){
                        List<LockState> states = waitingList.get(pid);
                        for(LockState state: states) {
                            if (!state.tid.equals(tid) && state.lockType.equals(LockType.ShareLock)) {
                                states.add(new LockState(tid, LockType.ExclusiveLock));
                                waitingList.replace(pid, states);
                                return LockType.Block;
                            }
                        }
                    }
                    lock(tid, pid, LockType.ExclusiveLock);
                    return LockType.ExclusiveLock;
                } else {
                    lock(tid, pid, LockType.ExclusiveLock);
                    return LockType.ExclusiveLock;
                }
            }
        }
        throw new DbException("Impossible to reach here");
    }

    public synchronized LockType isHoldingLock(TransactionId tid, PageId pid){
        if (locksList.containsKey(pid)) {
            List<LockState> state = locksList.get(pid);
            if (state != null && state.size() != 0) {
                for (LockState lockState : state) {
                    if (lockState.equals(tid)) {
                        return lockState.lockType;
                    }
                }
            }
        }
        return LockType.None;
    }

    public synchronized LockType isWaiting(TransactionId tid, PageId pid) {
        if (waitingList.containsKey(pid)){
            List<LockState> lockState = waitingList.get(pid);
            if (lockState != null && lockState.size() != 0) {
                for (LockState state: lockState) {
                    if (state.tid.equals(tid)) {
                        return state.lockType;
                    }
                }
            }
            return LockType.None;
        }
        return LockType.None;
    }

    public synchronized void unlock(TransactionId tid, PageId pid) throws DbException {
        if (isHoldingLock(tid, pid).equals(LockType.None)) {
            throw new DbException("release a none-existent lock");
        }
        List<LockState> states = locksList.get(pid);
        for (int i = 0; i < states.size(); i++) {
            if (states.get(i).tid.equals(tid)) {
                LockState state = new LockState(tid, states.get(i).lockType);
                states.remove(state);
                break;
            }
        }

    }

    public synchronized void lock(TransactionId tid, PageId pid, LockType type) {
        if (locksList.containsKey(pid)) {
            List<LockState> states = locksList.get(pid);
            if (states == null) {
                states = new ArrayList<>();
            }
            states.add(new LockState(tid, type));
            locksList.replace(pid, states);
        } else {
            List<LockState> states = new ArrayList<>();
            states.add(new LockState(tid, type));
            locksList.put(pid, states);
        }
    }

    /*public synchronized void wait(TransactionId tid, PageId pid, LockType type) {
        if (waitingList.containsKey(pid)) {
            List<LockState> lockStates = waitingList.get(pid);
            if (lockStates == null) {
                List<LockState> stateList = new ArrayList<>();
                stateList.add(new LockState(tid, type));
                waitingList.put(pid, stateList);
            } else {
                lockStates.add(new LockState(tid, type));
                waitingList.replace(pid, lockStates);
            }
        } else {
            List<LockState> stateList = new ArrayList<>();
            stateList.add(new LockState(tid, type));
            waitingList.put(pid, stateList);
        }
    }*/

}

