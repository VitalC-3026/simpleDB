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
        // System.out.println("requireExclusiveLock");
        switch (isHoldingLock(tid, pid)) {
            case ShareLock: {
                // 如果当前的事务tid已经获得了对pid的读锁，那么如果pid仅有一个这样的锁，那么就将这个共享锁变成排它锁
                // 如果pid不仅有当前tid的共享锁，那么就直接返回共享锁，说明可以获取到该锁
                // System.out.println("ShareLock HOLDING");
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
                // 如果当前的tid已经获得了对pid的排它锁，那也就是可以获得对pid的共享锁，不需要阻塞
                // System.out.println("ExclusiveLock HOLDING");
                List<LockState> lockStateList = locksList.get(pid);
                lockStateList.add(new LockState(tid, LockType.ShareLock, permissions));
                locksList.replace(pid, lockStateList);
                return LockType.ShareLock;
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
                // 如果当前tid没有对pid获取到任何锁，会有3种情况：
                // ①pid上有一个排它锁，但权限为READ_ONLY，则将这个锁改回共享锁，并允许tid获取到pid的共享锁
                // ②pid上只要有一个权限为READ_WRITE的排它锁，那么当前tid阻塞
                // ③pid上有不同tid上需要的共享锁，那么当前tid也可以获得pid的共享锁
                // System.out.println("NoLock HOLDING");
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
        // System.out.println("requireExclusiveLock");
        switch (isHoldingLock(tid, pid)) {
            case ExclusiveLock: {
                // 如果当前tid已经拥有了pid的排它锁，那么返回该锁即可
                // System.out.println("ExclusiveLock HOLDING");
                return LockType.ExclusiveLock;}
            case ShareLock: {
                // 如果当前tid拥有的是pid的共享锁，那么也允许当前tid获取pid的排它锁
                // System.out.println("ShareLock HOLDING");
                List<LockState> states = locksList.get(pid);
                states.add(new LockState(tid, LockType.ExclusiveLock, permissions));
                return LockType.ExclusiveLock;
            }
            case None: {
                // 如果当前tid没有拥有pid的锁，那会出现以下几种情况：
                // ①pid上没有任何锁，那就可以放心大胆地允许tid获取pid的排它锁
                // ②pid上有其他tid上的共享锁或者是有READ_ONLY权限的排它锁，此时tid只能阻塞
                // ③pid上有其他tid上获取的排它锁，阻塞？
                // System.out.println("NoLock HOLDING");
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

    public synchronized boolean unlock(TransactionId tid, PageId pid){
        if (isHoldingLock(tid, pid).equals(LockType.None)) {
            return false;
        }
        List<LockState> states = locksList.get(pid);
        for (int i = 0; i < states.size(); i++) {
            if (states.get(i).tid.equals(tid)) {
                states.remove(i);
                break;
            }
        }
        return true;
    }

    public synchronized LockType lock(TransactionId tid, PageId pid, LockType type, Permissions permissions) {
        if (locksList.containsKey(pid)) {
            List<LockState> states = locksList.get(pid);
            if (states == null || states.size() == 0) {
                states = new ArrayList<>();
                states.add(new LockState(tid, LockType.ExclusiveLock, permissions));
                locksList.replace(pid, states);
                // System.out.println("Exclusive");
                return LockType.ExclusiveLock;
            } else if (states.size() == 1 && states.iterator().next().tid.equals(tid)) {
                states.iterator().next().lockType = LockType.ExclusiveLock;
                locksList.replace(pid, states);
                // System.out.println("Exclusive");
                return LockType.ExclusiveLock;
            } else {
                states.add(new LockState(tid, type, permissions));
                locksList.replace(pid, states);
                // System.out.println(type);
                return type;
            }
        } else {
            List<LockState> states = new ArrayList<>();
            states.add(new LockState(tid, LockType.ExclusiveLock, permissions));
            locksList.put(pid, states);
            // System.out.println("Exclusive");
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

    public synchronized ArrayList<PageId> releaseAllLock(TransactionId tid) {
        ArrayList<PageId> pages = new ArrayList<>();
        for (PageId pageId : locksList.keySet()) {
            Iterator<LockState> states = locksList.get(pageId).iterator();
            while (states.hasNext()) {
                LockState state = states.next();
                if (state.tid.equals(tid)) {
                    states.remove();
                    pages.add(pageId);
                }
            }
        }
        return pages;
    }
}

