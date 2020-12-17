package cp1.solution;

import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
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
    private ConcurrentMap<Long, Long> timer;
    private ConcurrentMap<Long, Integer> ActiveTransactions;
    private ConcurrentMap<ResourceId, Semaphore> onResource;
    private ConcurrentMap<ResourceId, Semaphore> wait;
    private ConcurrentMap<ResourceId, Long> takenResources;
    private ConcurrentMap<Long, ResourceId> waitingOnResource;
    private ArrayList<Resource> resources;
    private ConcurrentMap<Long, List<Operation>> OperationLogs;
    private Map<Long, Integer> threadsList;

    public TM(Collection<Resource> resources,
              LocalTimeProvider timeProvider) {
        this.timeProvider = timeProvider;
        this.resources = new ArrayList<>(resources);
        onResource = new ConcurrentHashMap<>();
        wait = new ConcurrentHashMap<>();
        for (Resource r : this.resources) {
            onResource.computeIfAbsent(r.getId(), (k) -> new Semaphore (1, true));
            wait.computeIfAbsent(r.getId(), (k) -> new Semaphore (1, true));
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

    private void setThreadtoAbort (long currentThread) {
        if (timer.get(currentThread) > timer.get(ThreadtoAbort)) {
            ThreadtoAbort = currentThread;
        } else if (timer.get(currentThread) == timer.get(ThreadtoAbort)
                   && currentThread > ThreadtoAbort) {
                ThreadtoAbort = currentThread;
        }
    }

    private boolean checkForCycle(long currentThread) {
        setThreadtoAbort(currentThread);
        if (threadsList.get(currentThread) == ACTIVE)
            return true;

        threadsList.computeIfAbsent(currentThread, k -> ACTIVE);
        ResourceId resId = waitingOnResource.get(currentThread);
        if (resId == null)
            return false;

        long newThread = takenResources.get(resId);
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
        }
        else
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

        mutex.acquire();
        Long threadOnResource = takenResources.get(rid);
        if (threadOnResource == null) {
            takenResources.computeIfAbsent(rid, (k) -> currentThread);
            mutex.release();
            onResource.get(rid).acquire();
        }
        else if (threadOnResource != currentThread) {
            mutex.release();
            wait.get(rid).acquire();
            waitingOnResource.computeIfAbsent(currentThread, (k) -> rid);
            mutex.acquire();
            if (isDeadlock(currentThread)) {
                ResourceId resToRelease = waitingOnResource.remove(ThreadtoAbort);
                ActiveTransactions.computeIfPresent(ThreadtoAbort, (k, v) -> ABORTED);
                wait.get(resToRelease).release();
            }
            mutex.release();
            onResource.get(rid).acquire();
            if (ActiveTransactions.get(currentThread) == ABORTED) {
                Thread.currentThread().interrupt();
                onResource.get(rid).release();
                return;
            }
            waitingOnResource.remove(currentThread);
            takenResources.computeIfAbsent(rid, k -> currentThread);
            wait.get(rid).release();
        } else {
            mutex.release();
        }

        OperationLogs.computeIfAbsent(currentThread, k -> new ArrayList<Operation>());
        OperationLogs.get(currentThread).add(new Operation(rid, operation));
        res.apply(operation);
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
        for (Resource res : resources) {
            ResourceId rid = res.getId();
            if (takenResources.get(rid) == currentThread) {
                takenResources.remove(rid);
                onResource.get(rid).release();
            }
        }
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
        for (Resource res : resources) {
            ResourceId rid = res.getId();
            if (takenResources.get(rid) == currentThread) {
                takenResources.remove(rid);
                onResource.get(rid).release();
            }
        }
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