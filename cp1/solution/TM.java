package cp1.solution;

import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

import cp1.base.ResourceOperation;
import cp1.base.TransactionManager;
import cp1.base.LocalTimeProvider;
import cp1.base.Resource;
import cp1.base.ResourceId;
import cp1.base.AnotherTransactionActiveException;
import cp1.base.ActiveTransactionAborted;
import cp1.base.ResourceOperationException;
import cp1.base.UnknownResourceIdException;
import cp1.base.NoActiveTransactionException;

public class TM implements TransactionManager {
    private static final Integer ACTIVE = 1;
    private static final Integer ABORTED = 2;

    private Long ThreadtoAbort;
    private static Semaphore mutex;
    private LocalTimeProvider timeProvider;

    // To save entry time of each transaction.
    private ConcurrentMap<Long, Long> timer;

    // Keeps info wheather transaction is active/aborted/inactive.
    private ConcurrentMap<Long, Integer> ActiveTransactions;

    // Semaphore for every Resource to know the next thread
    // that is going to acquire this resource.
    // Semaphore fairness is set to true.
    private ConcurrentMap<ResourceId, Semaphore> onResource;

    // Semaphore for every Resource to know the queue of threads
    // that are going to acquire resource.
    // Semaphore fairness is set to true.
    private ConcurrentMap<ResourceId, Semaphore> wait;

    // Keeps info about already taken resources.
    private ConcurrentMap<ResourceId, Long> takenResources;

    // Transactions that are first to take resource when it will be released.
    private ConcurrentMap<Long, ResourceId> waitingOnResource;

    // To store resources.
    private ArrayList<Resource> resources;

    // To store operations that have already been done in a current transactions.
    private ConcurrentMap<Long, List<Operation>> OperationLogs;

    // To store threads that are being considered in isDeadLock method.
    private Map<Long, Integer> threadsList;

    public TM(Collection<Resource> resources,
              LocalTimeProvider timeProvider) {
        this.timeProvider = timeProvider;
        this.resources = new ArrayList<>(resources);
        onResource = new ConcurrentHashMap<>();
        wait = new ConcurrentHashMap<>();
        for (Resource r : this.resources) {
            onResource.computeIfAbsent(r.getId(), (k) -> new Semaphore(1, true));
            wait.computeIfAbsent(r.getId(), (k) -> new Semaphore(1, true));
        }
        waitingOnResource = new ConcurrentHashMap<>();
        ActiveTransactions = new ConcurrentHashMap<>();
        takenResources = new ConcurrentHashMap<>();
        timer = new ConcurrentHashMap<>();
        OperationLogs = new ConcurrentHashMap<>();
        mutex = new Semaphore(1, true);
        threadsList = new HashMap<>();
        ThreadtoAbort = (long) 0;
    }

    private Resource getResource(ResourceId rid) {
        for (Resource res : resources) {
            if (rid == res.getId())
                return res;
        }
        return null;
    }

    private void setThreadtoAbort(long currentThread) {
        if (timer.get(currentThread) > timer.get(ThreadtoAbort)) {
            ThreadtoAbort = currentThread;

        } else if (timer.get(currentThread) == timer.get(ThreadtoAbort)
                && currentThread > ThreadtoAbort) {
            ThreadtoAbort = currentThread;
        }
    }

    private boolean checkForCycle(long currentThread) {
        if (threadsList.get(currentThread) == ACTIVE)
            return true;

        threadsList.computeIfAbsent(currentThread, k -> ACTIVE);
        setThreadtoAbort(currentThread);
        ResourceId resId = waitingOnResource.get(currentThread);
        if (resId == null)
            return false;

        Long newThread = takenResources.get(resId);
        return checkForCycle(newThread);
    }

    private boolean isDeadlock(long currentTransaction) {
        threadsList.clear();
        ThreadtoAbort = currentTransaction;
        return checkForCycle(currentTransaction);
    }

    public void startTransaction() throws AnotherTransactionActiveException {
        Long currentThreadId = Thread.currentThread().getId();
        if (!ActiveTransactions.containsKey(currentThreadId)) {
            ActiveTransactions.computeIfAbsent(currentThreadId, (k) -> ACTIVE);
            timer.computeIfAbsent(currentThreadId, (k) -> timeProvider.getTime());
        } else
            throw new AnotherTransactionActiveException();
    }

    public void operateOnResourceInCurrentTransaction(ResourceId rid, ResourceOperation operation)
            throws
            NoActiveTransactionException,
            UnknownResourceIdException,
            ActiveTransactionAborted,
            ResourceOperationException,
            InterruptedException {

        Long currentThread = Thread.currentThread().getId();
        if (!isTransactionActive())
            throw new NoActiveTransactionException();

        if (isTransactionAborted())
            throw new ActiveTransactionAborted();

        Resource res = getResource(rid);
        if (res == null)
            throw new UnknownResourceIdException(rid);

        if (Thread.currentThread().isInterrupted())
            throw new InterruptedException();

        mutex.acquireUninterruptibly();
        Long threadOnResource = takenResources.get(rid);
        if (threadOnResource == null) {  // Resource is not taken.
            takenResources.computeIfAbsent(rid, (k) -> currentThread);
            mutex.release();
            onResource.get(rid).acquireUninterruptibly();

        } else if (threadOnResource != currentThread) { // Resource is already taken and this thread has to wait.
            mutex.release();
            wait.get(rid).acquireUninterruptibly();
            mutex.acquireUninterruptibly();
            waitingOnResource.computeIfAbsent(currentThread, (k) -> rid);
            if (isDeadlock(currentThread)) {
                ResourceId resToRelease = waitingOnResource.remove(ThreadtoAbort);
                ActiveTransactions.computeIfPresent(ThreadtoAbort, (k, v) -> ABORTED);
                wait.get(resToRelease).release();
            }
            mutex.release();
            onResource.get(rid).acquireUninterruptibly();
            if (ActiveTransactions.get(currentThread) == ABORTED) {
                Thread.currentThread().interrupt();
                onResource.get(rid).release();
                throw new ActiveTransactionAborted();
            }
            mutex.acquireUninterruptibly();
            waitingOnResource.remove(currentThread);
            takenResources.computeIfAbsent(rid, k -> currentThread);
            mutex.release();
            wait.get(rid).release();

        } else {
            mutex.release();
        }

        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }

        try {
            res.apply(operation);
        } catch (ResourceOperationException e) {
            throw new ResourceOperationException(rid, operation);
        }

        if (Thread.currentThread().isInterrupted()) {
            res.unapply(operation);
            throw new InterruptedException();
        }

        OperationLogs.computeIfAbsent(currentThread, k -> new ArrayList<Operation>());
        OperationLogs.get(currentThread).add(new Operation(rid, operation));

    }

    public void commitCurrentTransaction()
            throws
            NoActiveTransactionException,
            ActiveTransactionAborted {
        Long currentThread = Thread.currentThread().getId();
        if (!isTransactionActive()) {
            throw new NoActiveTransactionException();
        }

        if (isTransactionAborted()) {
            throw new ActiveTransactionAborted();
        }

        OperationLogs.remove(currentThread);
        timer.remove(currentThread);

        mutex.acquireUninterruptibly();
        for (Resource res : resources) {
            ResourceId rid = res.getId();
            if (takenResources.get(rid) == currentThread) {
                takenResources.remove(rid);
                onResource.get(rid).release();
            }
        }
        mutex.release();
        ActiveTransactions.remove(currentThread);
    }

    public void rollbackCurrentTransaction() {
        Long currentThread = Thread.currentThread().getId();
        if (OperationLogs.get(currentThread) != null) {
            ArrayList<Operation> operations = new ArrayList<>(OperationLogs.remove(currentThread));
            Collections.reverse(operations);
            for (Operation o : operations) {
                getResource(o.getResourceId()).unapply(o.getResourceOperation());
            }
        }
        timer.remove(currentThread);
        mutex.acquireUninterruptibly();
        for (Resource res : resources) {
            ResourceId rid = res.getId();
            if (takenResources.get(rid) == currentThread) {
                takenResources.remove(rid);
                onResource.get(rid).release();
            }
        }
        mutex.release();
        ActiveTransactions.remove(currentThread);

    }

    public boolean isTransactionActive() {
        if (ActiveTransactions.get(Thread.currentThread().getId()) == ACTIVE)
            return true;

        return false;
    }

    public boolean isTransactionAborted() {
        if (ActiveTransactions.get(Thread.currentThread().getId()) == ABORTED)
            return true;

        return false;
    }
}