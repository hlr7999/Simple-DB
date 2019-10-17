package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final Object groups;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	if (gbfield == Aggregator.NO_GROUPING) {
    		this.groups = new ArrayList<Integer>();
    	} else {
        	this.groups = new HashMap<Field, ArrayList<Integer>>();
    	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    @SuppressWarnings("unchecked")
	public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if (gbfield == Aggregator.NO_GROUPING) {
    		((ArrayList<Integer>)groups).add(((IntField)tup.getField(afield)).getValue());
    	} else {
    		HashMap<Field, ArrayList<Integer>> groupsMap = (HashMap<Field, ArrayList<Integer>>)groups;
    		Integer val = ((IntField)tup.getField(afield)).getValue();
    		groupsMap.computeIfAbsent(tup.getField(gbfield), k -> new ArrayList<Integer>()).add(val);
    	}
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
					tuple.setField(0, new IntField(computeGroupVal((ArrayList<Integer>)groups)));
					res.add(tuple);
				} else {
					HashMap<Field, ArrayList<Integer>> groupsMap = (HashMap<Field, ArrayList<Integer>>)groups;
					for (Map.Entry<Field, ArrayList<Integer>> entry : groupsMap.entrySet()) {
						Tuple t = new Tuple(getTupleDesc());
	                    Field groupField = entry.getKey();
	                    Field aField = new IntField(computeGroupVal(entry.getValue()));
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
			
			private int computeGroupVal(List<Integer> l) {
		    	assert l.size() > 0;
		    	int res = 0;
		    	switch (what) {
		        case MIN:
		            res = l.get(0);
		            for (int v : l)
		                if (res > v) res = v;
		            break;
		        case MAX:
		            res = l.get(0);
		            for (int v : l)
		                if (res < v) res = v;
		            break;
		        case SUM:
		            for (int v : l)
		                res += v;
		            break;
		        case AVG:
		            for (int v : l)
		                res += v;
		            res = res / l.size();
		            break;
		        case COUNT:
		            res = l.size();
		            break;
		        default:
		        	break;
		    	}
		    	return res;
		    }
		}
        return new iter();
    }

}
