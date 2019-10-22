package simpledb;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
	
	private final Map<PageId, Set<TransactionId>> pageReadHolders;
	private final Map<PageId, TransactionId> pageWriteHolder;
	
	public LockManager() {
		this.pageReadHolders = new ConcurrentHashMap<PageId, Set<TransactionId>>();
		this.pageWriteHolder = new ConcurrentHashMap<PageId, TransactionId>();
	}
	
	public void acquire(TransactionId tid, PageId pid, Permissions p) {
		if (p == Permissions.READ_ONLY) {
			acquireReadLock(tid, pid);
		} else {
			acquireWriteLock(tid, pid);
		}
	}
	
	private void acquireReadLock(TransactionId tid, PageId pid) {
		if (!hold(tid, pid)) {
			synchronized (pid) {
				while (pageWriteHolder.containsKey(pid)) {
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				pageReadHolders.computeIfAbsent(
					pid, key->new HashSet<TransactionId>()).add(tid);
			}
		}
	}
	
	private void acquireWriteLock(TransactionId tid, PageId pid) {
		if (!holdWriteLock(tid, pid)) {
			synchronized (pid) {
				while (pageWriteHolder.containsKey(pid) || haveOtherReader(tid, pid)) {
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
		synchronized (pid) {
			if (pageReadHolders.containsKey(pid)) {
				for (TransactionId t : pageReadHolders.get(pid)) {
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
			synchronized (pid) {
				pageReadHolders.get(pid).remove(tid);
				if (pageReadHolders.get(pid).isEmpty()) {
					pageReadHolders.remove(pid);
				}
			}
		}
	}
	
	private void releaseWriteLock(TransactionId tid, PageId pid) {
		if (holdWriteLock(tid, pid)) {
			synchronized (pid) {
				pageWriteHolder.remove(pid);
			}
		}
	}
	
	public boolean hold(TransactionId tid, PageId pid) {
		return holdReadLock(tid, pid) || holdWriteLock(tid, pid);
	}
	
	private boolean holdReadLock(TransactionId tid, PageId pid) {
		synchronized (pid) {
			return pageReadHolders.containsKey(pid) && 
					pageReadHolders.get(pid).contains(tid);
		}
	}
	
	private boolean holdWriteLock(TransactionId tid, PageId pid) {
		synchronized (pid) {
			return pageWriteHolder.containsKey(pid) && 
					pageWriteHolder.get(pid).equals(tid);
		}
	}
	
	public void releaseAll(TransactionId tid) {
		for (PageId pid : pageWriteHolder.keySet()) {
			releaseWriteLock(tid, pid);
		}
		for (PageId pid : pageReadHolders.keySet()) {
			releaseReadLock(tid, pid);
		}
	}
	
}
