package simpledb;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
	
	private static final Byte FK = 0;
	
	private final Map<PageId, Map<TransactionId, Byte>> pageReadHolders;
	private final Map<PageId, TransactionId> pageWriteHolder;
	private final Map<TransactionId, Map<PageId, Byte>> waitList;
	
	public LockManager() {
		this.pageReadHolders = new ConcurrentHashMap<PageId, Map<TransactionId,Byte>>();
		this.pageWriteHolder = new ConcurrentHashMap<PageId, TransactionId>();
		this.waitList = new ConcurrentHashMap<TransactionId, Map<PageId, Byte>>();
	}
	
	public void acquire(TransactionId tid, PageId pid, Permissions p)
		throws TransactionAbortedException {
		waitList.computeIfAbsent(tid, key ->
			new ConcurrentHashMap<PageId, Byte>()).put(pid, FK);
		if (p == Permissions.READ_ONLY) {
			acquireReadLock(tid, pid);
		} else {
			acquireWriteLock(tid, pid);
		}
		waitList.get(tid).remove(pid);
		if (waitList.get(tid).isEmpty()) {
			waitList.remove(tid);
		}
	}
	
	private void acquireReadLock(TransactionId tid, PageId pid)
		throws TransactionAbortedException {
		if (!hold(tid, pid)) {
			synchronized (tid) {
				while (pageWriteHolder.containsKey(pid)) {
					deadLockDetect(tid, pid);
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				pageReadHolders.computeIfAbsent(pid, key ->
					new ConcurrentHashMap<TransactionId, Byte>()).put(tid, FK);
			}
		}
	}
	
	private void acquireWriteLock(TransactionId tid, PageId pid)
		throws TransactionAbortedException {
		if (!holdWriteLock(tid, pid)) {
			synchronized (tid) {
				while (pageWriteHolder.containsKey(pid) || 
					haveOtherReader(tid, pid)) {
					deadLockDetect(tid, pid);
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				pageWriteHolder.put(pid, tid);
			}
		}
	}
	
	private boolean haveOtherReader(TransactionId tid, PageId pid) {
		synchronized (tid) {
			if (pageReadHolders.containsKey(pid)) {
				for (TransactionId t : pageReadHolders.get(pid).keySet()) {
					if (!t.equals(tid)) {
						return true;
					}
				}
			}
			return false;
		}
	}
	
	public void release(TransactionId tid, PageId pid) {
		releaseWriteLock(tid, pid);
		releaseReadLock(tid, pid);
	}
	
	private void releaseReadLock(TransactionId tid, PageId pid) {
		if (holdReadLock(tid, pid)) {
			synchronized (tid) {
				pageReadHolders.get(pid).remove(tid);
				if (pageReadHolders.get(pid).isEmpty()) {
					pageReadHolders.remove(pid);
				}
			}
		}
	}
	
	private void releaseWriteLock(TransactionId tid, PageId pid) {
		if (holdWriteLock(tid, pid)) {
			synchronized (tid) {
				pageWriteHolder.remove(pid);
			}
		}
	}
	
	public boolean hold(TransactionId tid, PageId pid) {
		return holdReadLock(tid, pid) || holdWriteLock(tid, pid);
	}
	
	private boolean holdReadLock(TransactionId tid, PageId pid) {
		synchronized (tid) {
			return pageReadHolders.containsKey(pid) && 
					pageReadHolders.get(pid).containsKey(tid);
		}
	}
	
	private boolean holdWriteLock(TransactionId tid, PageId pid) {
		synchronized (tid) {
			return pageWriteHolder.containsKey(pid) && 
					pageWriteHolder.get(pid).equals(tid);
		}
	}
	
	public void releaseAll(TransactionId tid) {
		// release waitList first
		if (waitList.containsKey(tid)) {
			waitList.remove(tid);
		}
		for (PageId pid : pageWriteHolder.keySet()) {
			releaseWriteLock(tid, pid);
		}
		for (PageId pid : pageReadHolders.keySet()) {
			releaseReadLock(tid, pid);
		}
	}
	
	private void deadLockDetect(TransactionId tid, PageId pid) 
		throws TransactionAbortedException {
		deadLockDetect(new ConcurrentHashMap<TransactionId, Byte>(), tid, pid);
	}
	
	private void deadLockDetect(Map<TransactionId, Byte> tMap, TransactionId tid, PageId pid) 
		throws TransactionAbortedException {
		if (tMap.containsKey(tid)) {
            throw new TransactionAbortedException();
        }
        tMap.put(tid, FK);
        //System.out.println("tid " + tid + " pid " + pid);
        Map<TransactionId, Byte> tIds = pageReadHolders.getOrDefault(pid, null);
        //System.out.println("tSet " + tSet);
        //System.out.println("tIds " + tIds);
        if (tIds != null) {
        	for (TransactionId t : tIds.keySet()) {
        		if (t.equals(tid)) continue;
        		Map<PageId, Byte> pageIds = waitList.getOrDefault(t, null);
                //System.out.println("pages " + pageIds);
        		if (pageIds != null) {
        			for (PageId pageId : pageIds.keySet()) {
        				deadLockDetect(tMap, t, pageId);
        			}
        		}
        	}
        }
        TransactionId t = pageWriteHolder.getOrDefault(pid, null);
        //System.out.println("wt " + t);
        if (t != null && (tIds == null || !tIds.containsKey(t))) {
        	Map<PageId, Byte> pageIds = waitList.getOrDefault(t, null);
    		if (pageIds != null) {
    			for (PageId pageId : pageIds.keySet()) {
    				deadLockDetect(tMap, t, pageId);
    			}
    		}
        }
	}
	
}
