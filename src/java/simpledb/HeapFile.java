package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	private final File file;
	private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.file = f;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (pid.getTableId() != getId()) {
        	throw new IllegalArgumentException("page not in this table");
        }
        int pgNo = pid.getPageNumber();
        if (pgNo < 0 || pgNo >= numPages()) {
        	throw new IllegalArgumentException("page number out of range");
        }
        byte[] data = HeapPage.createEmptyPageData();
        try {
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			try {
				raf.seek(BufferPool.getPageSize() * pgNo);
				raf.read(data, 0, BufferPool.getPageSize());
				return new HeapPage(new HeapPageId(getId(), pgNo), data);
			} finally {
				raf.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
        	
        	private int pgNo = -1;
        	private Iterator<Tuple> tupleIter = null;
        	private final BufferPool pool = Database.getBufferPool();
        	private final int tableId = getId();
        	
        	@Override
        	public void open()
    	        throws DbException, TransactionAbortedException {
        		pgNo = 0;
        		tupleIter = ((HeapPage)pool.getPage(tid, new HeapPageId(tableId, pgNo++),
						Permissions.READ_ONLY)).iterator();
        	}
        	@Override
    	    public boolean hasNext()
    	        throws DbException, TransactionAbortedException {
        		if (tupleIter != null && tupleIter.hasNext()) {
        			return true;
        		} else if (pgNo < 0 || pgNo >= numPages()) {
        			return false;
        		} else {
    				tupleIter = ((HeapPage)pool.getPage(tid, new HeapPageId(tableId, pgNo++), 
						Permissions.READ_ONLY)).iterator();
    				return hasNext();
        		}
        	}
    	    @Override
    	    public Tuple next()
    	        throws DbException, TransactionAbortedException, NoSuchElementException {
    	    	if (hasNext()) {
    	    		return tupleIter.next();
    	    	}
    	    	throw new NoSuchElementException();
    	    }
    	    @Override
    	    public void rewind() throws DbException, TransactionAbortedException {
    	    	open();
    	    }
    	    @Override
    	    public void close() {
    	    	pgNo = -1;
    	    	tupleIter = null;
    	    }
        };
    }

}

