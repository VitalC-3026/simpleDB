package simpledb;

import java.beans.Transient;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LockRepository {
    private ConcurrentHashMap<TransactionId, PageId> waitingList = new ConcurrentHashMap<>();
    private ConcurrentHashMap<PageId, List<LockState>> locksList = new ConcurrentHashMap<>();

    public enum LockType {
        ExclusiveLock, ShareLock, Block, None
    };

    public class LockState {
        TransactionId tid;
        LockType lockType;
        Permissions permissions;

        public LockState(TransactionId tid, LockType lockType, Permissions permissions) {
            this.tid = tid;
            this.lockType = lockType;
            this.permissions = permissions;
        }

        public boolean equals(TransactionId tid) {
            return tid.equals(this.tid);
        }

        public boolean equals(TransactionId tid, LockType lockType){
            return tid.equals(this.tid) && lockType.equals(this.lockType);
        }
    }

    public synchronized LockType requireShareLock(TransactionId tid, PageId pid, Permissions permissions) throws DbException {
        System.out.println("requireExclusiveLock");
        switch (isHoldingLock(tid, pid)) {
            case ShareLock: {
                System.out.println("ShareLock HOLDING");
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
                System.out.println("ExclusiveLock HOLDING");
                List<LockState> lockStateList = locksList.get(pid);
                lockStateList.add(new LockState(tid, LockType.ShareLock, permissions));
                return LockType.ExclusiveLock;
            }
            case None: {
                /*System.out.println("None");
                if (isWaiting(tid, pid).equals(LockType.ExclusiveLock)) {
                    System.out.println("Block");
                    List<LockState> states = waitingList.get(pid);
                    states.add(new LockState(tid, LockType.ShareLock));
                    waitingList.put(pid, states);
                    // 是否可以加入这个排它锁到locksList？
                    return LockType.Block;
                } else if ((isWaiting(tid, pid).equals(LockType.None))){
                    System.out.println("None");
                    if (waitingList.containsKey(pid) && (waitingList.get(pid) != null && waitingList.get(pid).size() != 0)) {
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
                    return lock(tid, pid, LockType.ShareLock);
                } else {
                    List<LockState> states = waitingList.get(pid);
                    states.remove(new LockState(tid, LockType.ShareLock));
                    waitingList.put(pid, states);
                    return lock(tid, pid, LockType.ShareLock);
                }*/
                System.out.println("NoLock HOLDING");
                List<LockState> lockStateList = locksList.get(pid);
                if (lockStateList == null || lockStateList.size() == 0){
                    return lock(tid, pid, LockType.ShareLock, permissions);
                }
                if (lockStateList.size() == 1) {
                    LockState lockState = lockStateList.iterator().next();
                    if (lockState.permissions.equals(Permissions.READ_ONLY)) {
                        lockState.lockType = LockType.ShareLock;
                        lockStateList.add(lockState);
                        lockStateList.add(new LockState(tid, LockType.ShareLock, permissions));
                    } else {
                        waitingList.put(tid, pid);
                        return LockType.Block;
                    }

                }
                for (LockState state: lockStateList) {
                    if (!state.tid.equals(tid) && state.lockType.equals(LockType.ExclusiveLock)) {
                        waitingList.put(tid, pid);
                        return LockType.Block;
                    }
                }
                lock(tid, pid, LockType.ShareLock, permissions);
                return LockType.ShareLock;
            }
        }
        throw new DbException("Impossible to reach here");
    }

    public synchronized LockType requireExclusiveLock(TransactionId tid, PageId pid, Permissions permissions) throws DbException {
        System.out.println("requireExclusiveLock");
        switch (isHoldingLock(tid, pid)) {
            case ExclusiveLock: {
                System.out.println("ExclusiveLock HOLDING");
                return LockType.ExclusiveLock;}
            case ShareLock: {
                System.out.println("ShareLock HOLDING");
                List<LockState> states = locksList.get(pid);
                states.add(new LockState(tid, LockType.ExclusiveLock, permissions));
                return LockType.ExclusiveLock;
            }
            case None: {
                System.out.println("NoLock HOLDING");
                List<LockState> states = locksList.get(pid);
                if (states == null || states.size() == 0){
                    return lock(tid, pid, LockType.ExclusiveLock, permissions);
                }
                for (LockState state: states) {
                    if (!state.tid.equals(tid) && (state.lockType.equals(LockType.ShareLock) || state.permissions.equals(Permissions.READ_ONLY))) {
                        waitingList.put(tid, pid);
                        return LockType.Block;
                    }
                }
                lock(tid, pid, LockType.ExclusiveLock, permissions);
                return LockType.ExclusiveLock;
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

    /*public synchronized LockType isWaiting(TransactionId tid, PageId pid) {
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
    }*/

    public synchronized void unlock(TransactionId tid, PageId pid) throws DbException {
        if (isHoldingLock(tid, pid).equals(LockType.None)) {
            throw new DbException("release a none-existent lock");
        }
        List<LockState> states = locksList.get(pid);
        for (int i = 0; i < states.size(); i++) {
            if (states.get(i).tid.equals(tid)) {
                states.remove(i);
                break;
            }
        }

    }

    public synchronized LockType lock(TransactionId tid, PageId pid, LockType type, Permissions permissions) {
        if (locksList.containsKey(pid)) {
            List<LockState> states = locksList.get(pid);
            if (states == null || states.size() == 0) {
                states = new ArrayList<>();
                states.add(new LockState(tid, LockType.ExclusiveLock, permissions));
                locksList.replace(pid, states);
                System.out.println("Exclusive");
                return LockType.ExclusiveLock;
            } else if (states.size() == 1 && states.iterator().next().tid.equals(tid)) {
                states.iterator().next().lockType = LockType.ExclusiveLock;
                locksList.replace(pid, states);
                System.out.println("Exclusive");
                return LockType.ExclusiveLock;
            } else {
                states.add(new LockState(tid, type, permissions));
                locksList.replace(pid, states);
                System.out.println(type);
                return type;
            }
        } else {
            List<LockState> states = new ArrayList<>();
            states.add(new LockState(tid, LockType.ExclusiveLock, permissions));
            locksList.put(pid, states);
            System.out.println("Exclusive");
            return LockType.ExclusiveLock;
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

