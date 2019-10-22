package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private final TupleDesc td;
    private final TransactionId tid;
    private OpIterator child;
    private final int tableid;
    private boolean state;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
    	this.td = new TupleDesc(new Type[] {Type.INT_TYPE});
    	this.tid = t;
    	this.child = child;
    	this.tableid = tableId;
    	this.state = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	child.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (state) {
        	return null;
        } else {
        	Tuple tuple = new Tuple(td);
        	int num = 0;
        	BufferPool pool = Database.getBufferPool();
        	while (child.hasNext()) {
        		try {
					pool.insertTuple(tid, tableid, child.next());
				} catch (IOException e) {
					throw new DbException("insert tuple failed");
				}
        		++num;
        	}
        	tuple.setField(0, new IntField(num));
        	state = true;
        	return tuple;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	assert children.length > 0;
    	child = children[0];
    	state = false;
    }
}
