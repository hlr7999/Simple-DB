package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private final TupleDesc td;
    private final TransactionId tid;
    private OpIterator child;
    private boolean state;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
    	this.child = child;
    	this.tid = t;
    	this.state = false;
    	this.td = new TupleDesc(new Type[] {Type.INT_TYPE});
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
					pool.deleteTuple(tid, child.next());
				} catch (IOException e) {
					throw new DbException("delete tuple failed");
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
