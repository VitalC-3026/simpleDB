package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockRepository {
    private ConcurrentHashMap<TransactionId, LockState> waitingList = new ConcurrentHashMap<>();
    private ConcurrentHashMap<PageId, List<LockState>> locksList = new ConcurrentHashMap<>();
    public Graph graph = new Graph();

    public enum LockType {
        ExclusiveLock, ShareLock, Block, None
    };

    public static class LockState {
        TransactionId tid;
        LockType lockType;
        Permissions permissions;
        PageId pid;

        public LockState(TransactionId tid, PageId pid, Permissions permissions) {
            this.tid = tid;
            this.pid = pid;
            this.permissions = permissions;
        }

        public LockState(TransactionId tid, PageId pid, LockType lockType, Permissions permissions) {
            this.tid = tid;
            this.pid = pid;
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
        switch (isHoldingLock(tid, pid)) {
            case ShareLock: {
                // 如果当前的事务tid已经获得了对pid的读锁，那么如果pid仅有一个这样的锁，那么就将这个共享锁变成排它锁
                // 如果pid不仅有当前tid的共享锁，那么就直接返回共享锁，说明可以获取到该锁
                System.out.println("Require SLock: ShareLock HOLDING");
                /*List<LockState> lockStateList = locksList.get(pid);
                for (LockState state: lockStateList) {
                    if (!state.tid.equals(tid) && state.lockType.equals(LockType.ExclusiveLock)) {

                    }
                }*/
                return LockType.ShareLock;
            }
            case ExclusiveLock: {
                // 如果当前的tid已经获得了对pid的排它锁，那也就是可以获得对pid的共享锁，不需要阻塞
                System.out.println("Require SLock: ExclusiveLock HOLDING");
                List<LockState> lockStateList = locksList.get(pid);
                // 如果当前pid获得了其他tid的排它锁，阻塞
                for (LockState state: lockStateList) {
                    if (!state.tid.equals(tid) && state.permissions.equals(Permissions.READ_WRITE)) {
                        waitingList.put(tid, new LockState(tid, pid, permissions));
                        graph.addSchedule(new LockState(tid, pid, permissions));
                        return LockType.Block;
                    }
                }
                /*lockStateList.add(new LockState(tid, pid, LockType.ShareLock, permissions));
                locksList.replace(pid, lockStateList);
                Iterator<TransactionId> it = waitingList.keySet().iterator();
                while(it.hasNext()) {
                    LockState state = waitingList.get(it.next());
                    if (state.tid.equals(tid) && state.pid.equals(pid)) {
                        it.remove();
                        graph.deleteSchedule(state);
                    }
                }*/
                lock(tid, pid, LockType.ShareLock, permissions);
                return LockType.ShareLock;
            }
            case None: {
                // 如果当前tid没有对pid获取到任何锁，会有3种情况：
                // ①pid上没有任何锁，放心大胆获取，但升级为排它锁
                // ②pid上只要有一个权限为READ_WRITE的排它锁，那么当前tid阻塞
                // ③pid上有不同tid上需要的共享锁，那么当前tid也可以获得pid的共享锁
                System.out.println("Require SLock: NoLock HOLDING");
                List<LockState> lockStateList = locksList.get(pid);
                if (lockStateList == null || lockStateList.size() == 0){
                    return lock(tid, pid, LockType.ShareLock, permissions);
                }
                /*if (lockStateList.size() == 1 && lockStateList.iterator().next().permissions.equals(Permissions.READ_ONLY)) {
                    return lock(tid, pid, LockType.ShareLock, permissions);
                }*/
                for (LockState state: lockStateList) {
                    if (!state.tid.equals(tid) && state.lockType.equals(LockType.ExclusiveLock)) {
                        waitingList.put(tid, new LockState(tid, pid, permissions));
                        graph.addSchedule(new LockState(tid, pid, permissions));
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
                System.out.println("Require XLock: ExclusiveLock HOLDING");
                /*List<LockState> states = locksList.get(pid);
                if (states.size() == 1) {
                    LockState state = states.iterator().next();
                    if (state.permissions.equals(Permissions.READ_ONLY)) {
                        states.iterator().next().lockType = LockType.ShareLock;
                        locksList.replace(pid, states);
                        lock(tid, pid, LockType.ExclusiveLock, permissions);
                    }
                }*/
                return LockType.ExclusiveLock;
            }
            case ShareLock: {
                // 如果当前tid拥有的是pid的共享锁，那么也允许当前tid获取pid的排它锁
                System.out.println("Require XLock: ShareLock HOLDING");
                List<LockState> states = locksList.get(pid);
                // 如果该pid拥有了其他事务的排它锁，阻塞
                for (LockState state: states) {
                    /*if (state.lockType.equals(LockType.ExclusiveLock) && !state.tid.equals(tid))*/
                    if (!state.tid.equals(tid)) {
                        waitingList.put(tid, new LockState(tid, pid, permissions));
                        graph.addSchedule(new LockState(tid, pid, permissions));
                        return LockType.Block;
                    }
                }
                /*states.add(new LockState(tid, pid, LockType.ExclusiveLock, permissions));
                locksList.replace(pid, states);
                Iterator<TransactionId> it = waitingList.keySet().iterator();
                while(it.hasNext()) {
                    LockState state = waitingList.get(it.next());
                    if (state.tid.equals(tid) && state.pid.equals(pid)) {
                        it.remove();
                        graph.deleteSchedule(state);
                    }
                }*/
                lock(tid, pid, LockType.ExclusiveLock, permissions);
                return LockType.ExclusiveLock;
            }
            case None: {
                // 如果当前tid没有拥有pid的锁，那会出现以下几种情况：
                // ①pid上没有任何锁，那就可以放心大胆地允许tid获取pid的排它锁
                // ②pid上有其他tid上的共享锁或者是有READ_ONLY权限的排它锁，此时tid只能阻塞
                // ③pid上有其他tid上获取的排它锁，阻塞
                System.out.println("Require XLock: NoLock HOLDING");
                List<LockState> states = locksList.get(pid);
                if (states == null || states.size() == 0){
                    return lock(tid, pid, LockType.ExclusiveLock, permissions);
                }
                for (LockState state: states) {
                    if (!state.tid.equals(tid)) {
                        waitingList.put(tid, new LockState(tid, pid, permissions));
                        graph.addSchedule(new LockState(tid, pid, permissions));
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
        LockType resultType;
        if (locksList.containsKey(pid)) {
            List<LockState> states = locksList.get(pid);
            /*if (states == null || states.size() == 0) {
                states = new ArrayList<>();
                states.add(new LockState(tid, pid, LockType.ExclusiveLock, permissions));
                locksList.replace(pid, states);
                resultType = LockType.ExclusiveLock;
            } else if (states.size() == 1 && states.iterator().next().tid.equals(tid)) {
                states.iterator().next().lockType = LockType.ExclusiveLock;
                locksList.replace(pid, states);
                resultType =  LockType.ExclusiveLock;
            } else {
                states.add(new LockState(tid, pid, type, permissions));
                locksList.replace(pid, states);
                resultType = type;
            }*/
            if (states == null || states.size() == 0) {
                states = new ArrayList<>();
            }
            states.add(new LockState(tid, pid, type, permissions));
            locksList.replace(pid, states);
            resultType = type;
        } else {
            /*List<LockState> states = new ArrayList<>();
            states.add(new LockState(tid, pid, LockType.ExclusiveLock, permissions));
            locksList.put(pid, states);
            resultType = LockType.ExclusiveLock;*/
            List<LockState> states = new ArrayList<>();
            states.add(new LockState(tid, pid, type, permissions));
            locksList.put(pid, states);
            resultType = type;
        }
        Iterator<TransactionId> it = waitingList.keySet().iterator();
        while(it.hasNext()) {
            LockState state = waitingList.get(it.next());
            if (state.tid.equals(tid) && state.pid.equals(pid)) {
                it.remove();
                graph.deleteSchedule(state);
            }
        }
        return resultType;
    }

    public synchronized ArrayList<PageId> releaseAllLock(TransactionId tid) {
        ArrayList<PageId> pages = new ArrayList<>();
        for (PageId pageId : locksList.keySet()) {
            List<LockState> states = locksList.get(pageId);
            Iterator<LockState> it = states.iterator();
            while (it.hasNext()) {
                LockState state = it.next();
                if (state.tid.equals(tid)) {
                    it.remove();
                    pages.add(pageId);
                }
            }
            locksList.replace(pageId, states);
        }
        return pages;
    }

    public synchronized boolean hasDeadlock() {
        graph.buildPrecedenceGraph();
        return graph.isCyclic();
    }

}

class Graph {
    ConcurrentHashMap<TransactionId, LinkedList<TransactionId>> edges;
    LinkedList<LockRepository.LockState> schedule = new LinkedList<>();

    void addSchedule(LockRepository.LockState lockState) {
        schedule.add(lockState);
    }

    void deleteSchedule(LockRepository.LockState lockState) {
        schedule.remove(lockState);
        /*Iterator<TransactionId> it = edges.keySet().iterator();
        while(it.hasNext()) {
            TransactionId tid = it.next();
            if (tid.equals(lockState.tid)) {
                it.remove();
            } else {
                LinkedList<TransactionId> t = edges.get(tid);
                for(TransactionId transactionId: t) {
                    if (transactionId.equals(lockState.tid)) {
                        t.remove(lockState.tid);
                    }
                }
                edges.replace(tid, t);
            }
        }
        for (TransactionId tid: edges.keySet()) {
            if (edges.get(tid).size() == 0) {
                edges.remove(tid);
            }
        }*/
    }

    synchronized void buildPrecedenceGraph() {
        edges = new ConcurrentHashMap<>();
        ConcurrentHashMap<PageId, LinkedList<Integer>> transactionsByPage = new ConcurrentHashMap<>();
        for(int i = 0; i < schedule.size(); i++) {
            LockRepository.LockState state = schedule.get(i);
            if (transactionsByPage.containsKey(state.pid)) {
                LinkedList<Integer> transactions = transactionsByPage.get(state.pid);
                transactions.add(i);
                transactionsByPage.replace(state.pid, transactions);
            } else {
                LinkedList<Integer> transactions = new LinkedList<>();
                transactions.add(i);
                transactionsByPage.put(state.pid, transactions);
            }
        }
        Iterator<PageId> it = transactionsByPage.keySet().iterator();
        LinkedList<TransactionId> STransactions = new LinkedList<>();
        while(it.hasNext()) {
            LinkedList<Integer> transactions = transactionsByPage.get(it.next());
            LockRepository.LockState newLockState, oldLockState = null;
            for(Integer integer: transactions) {
               newLockState = schedule.get(integer);
               if (newLockState.permissions.equals(Permissions.READ_WRITE)) {
                   if (STransactions.size() != 0) {
                       for (TransactionId tid: STransactions) {
                           if (!edges.containsKey(tid)) {
                               edges.put(tid, new LinkedList<>());
                           }
                           LinkedList<TransactionId> t = edges.get(tid);
                           t.add(newLockState.tid);
                           edges.put(tid, t);
                           // schedule.remove(newLockState);
                       }
                       STransactions = new LinkedList<TransactionId>();
                   }
                   if (oldLockState != null) {
                       LinkedList<TransactionId> t = edges.get(newLockState.tid);
                       if (t == null) {
                           t = new LinkedList<>();
                       }
                       t.add(oldLockState.tid);
                       edges.put(newLockState.tid, t);
                       // schedule.remove(oldLockState);
                   }
                   oldLockState = newLockState;
               } else {
                   if (oldLockState != null) {
                       LinkedList<TransactionId> t = edges.get(newLockState.tid);
                       if (t == null) {
                           t = new LinkedList<>();
                       }
                       t.add(oldLockState.tid);
                       edges.put(newLockState.tid, t);
                       // schedule.remove(oldLockState);
                   }
                   STransactions.add(newLockState.tid);
               }
            }
        }
    }

    synchronized boolean isCyclic() {
        int count = 0;
        Stack<TransactionId> stack = new Stack<>();
        ConcurrentHashMap<TransactionId, Integer> inDegree = findInDegree();
        for(TransactionId transactionId: inDegree.keySet()) {
            if(inDegree.get(transactionId) == 0) {
                stack.push(transactionId);
            }
        }
        while (!stack.empty()) {
            TransactionId tid = stack.pop();
            LinkedList<TransactionId> transactionIds = edges.get(tid);
            count++;
            for(TransactionId transactionId: transactionIds) {
                int oldValue = inDegree.get(transactionId);
                int newValue = --oldValue;
                if (newValue == 0) {
                    stack.push(transactionId);
                }
                inDegree.replace(transactionId, oldValue, newValue);
            }
        }
        return count < edges.keySet().size();
    }

    synchronized ConcurrentHashMap<TransactionId, Integer> findInDegree(){
        ConcurrentHashMap<TransactionId, Integer> inDegree = new ConcurrentHashMap<>();
        for (TransactionId tid : edges.keySet()) {
            int degree = 0;
            for (TransactionId id : edges.keySet()) {
                LinkedList<TransactionId> transactionIds = edges.get(id);
                for (TransactionId transactionId : transactionIds) {
                    if (transactionId.equals(tid)) {
                        degree++;
                    }
                }
            }
            inDegree.put(tid, degree);
        }
        return inDegree;
    }
}
