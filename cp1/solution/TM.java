package cp1.solution;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
    private static final int ACTIVE = 1;
    private static final int ABORTED = 2;
    private static Semaphore mutex = new Semaphore(1, true);
    private static int waiters = 0;

    private LocalTimeProvider timeProvider;
    private ConcurrentMap<Long, Long> timer;
    private ConcurrentMap<Long, Integer> ActiveTransactions;
    private ConcurrentMap<ResourceId, AtomicInteger> onResource;
    private ConcurrentMap<ResourceId, Long> takenResources;
    private ArrayList<Resource> resources;
    private ConcurrentMap<Long, List<ResourceId, ResourceOperation>>

    public TM(Collection<Resource> resources,
              LocalTimeProvider timeProvider) {
        this.timeProvider = timeProvider;
        this.resources = new ArrayList<>(resources);
        this.onResource = new ConcurrentHashMap<>();
        for (Resource r : this.resources) {
            this.onResource.computeIfAbsent(r.getId(), (k) -> new AtomicInteger());
        }
        ActiveTransactions = new ConcurrentHashMap<>();
        takenResources = new ConcurrentHashMap<>();
        timer = new ConcurrentHashMap<>();
    }


    private Resource getResource(ResourceId rid) {
        for (Resource res : resources) {
            if (rid == res.getId())
                return res;
        }
        return null;
    }

    private boolean isIdValid(ResourceId rid) {
        for (Resource res : resources)
            if (rid == res.getId())
                return true;

        return false;
    }

    public void startTransaction() throws AnotherTransactionActiveException {
        long currentThreadId = Thread.currentThread().getId();
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
        if (!isTransactionActive())
            throw new NoActiveTransactionException();

        if (isTransactionAborted())
            throw new ActiveTransactionAborted();

        Resource res = getResource(rid);
        if (res == null)
            throw new UnknownResourceIdException(rid);

        if (Thread.currentThread().interrupted())
            throw new InterruptedException();


    }

    public void commitCurrentTransaction(
    ) throws
            NoActiveTransactionException,
            ActiveTransactionAborted {
    }

    public void rollbackCurrentTransaction() {
    }

    public boolean isTransactionActive() {
        long currentThreadId = Thread.currentThread().getId();
        if (ActiveTransactions.containsKey(currentThreadId)) {
            if (ActiveTransactions.get(currentThreadId) == ACTIVE)
                return true;
        }

        return false;
    }

    public boolean isTransactionAborted() {
        long currentThreadId = Thread.currentThread().getId();
        if (ActiveTransactions.containsKey(currentThreadId)) {
            if (ActiveTransactions.get(currentThreadId) == ABORTED)
                return true;
        }

        return false;
    }
}