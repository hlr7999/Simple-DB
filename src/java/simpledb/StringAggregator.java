package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private Object groups;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	if (what != Aggregator.Op.COUNT) {
    		throw new IllegalArgumentException();
    	}
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	if (gbfield == Aggregator.NO_GROUPING) {
    		this.groups = new Integer(0);
    	} else {
        	this.groups = new HashMap<Field, Integer>();
    	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    @SuppressWarnings("unchecked")
	public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if (gbfield == Aggregator.NO_GROUPING) {
    		groups = (Integer)groups + 1;
    	} else {
    		HashMap<Field,Integer> groupsMap = (HashMap<Field, Integer>)groups;
    		Field key = tup.getField(gbfield);
    		if (groupsMap.containsKey(key)) {
    			groupsMap.put(key, groupsMap.get(key) + 1);
    		} else {
    			groupsMap.put(key, 1);
    		}
    	}
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
    	 class iter implements OpIterator {
 			
 			private ArrayList<Tuple> res = new ArrayList<Tuple>();
 			private Iterator<Tuple> iterator = null;
 			
 			@SuppressWarnings("unchecked")
 			public iter() {
 				if (gbfield == Aggregator.NO_GROUPING) {
 					Tuple tuple = new Tuple(getTupleDesc());
 					tuple.setField(0, new IntField((Integer)groups));
 					res.add(tuple);
 				} else {
 					HashMap<Field, Integer> groupsMap = (HashMap<Field, Integer>)groups;
 					for (Map.Entry<Field, Integer> entry : groupsMap.entrySet()) {
 						Tuple t = new Tuple(getTupleDesc());
 	                    Field groupField = entry.getKey();
 	                    Field aField = new IntField(entry.getValue());
 	                    t.setField(0, groupField);
 	                    t.setField(1, aField);
 	                    res.add(t);
 					}
 				}
 			}
 			
 			@Override
 			public void open() throws DbException, TransactionAbortedException {
 				iterator = res.iterator();
 			}
 			
 			@Override
 			public boolean hasNext() throws DbException, TransactionAbortedException {
 				if (iterator == null) {
 					throw new IllegalStateException();
 				}
 				return iterator.hasNext();
 			}
 			
 			@Override
 			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
 				if (iterator == null) {
 					throw new IllegalStateException();
 				}
 				return iterator.next();
 			}
 			
 			@Override
 			public void rewind() throws DbException, TransactionAbortedException {
 				iterator = res.iterator();
 			}
 			
 			@Override
 			public TupleDesc getTupleDesc() {
 				if (gbfield == Aggregator.NO_GROUPING) {
 	                return new TupleDesc(new Type[]{Type.INT_TYPE});
 	            } else {
 	                return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
 	            }
 			}
 			
 			@Override
 			public void close() {
 				iterator = null;
 			}
 		}
        return new iter();
    }

}
