package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
    	class iter implements Iterator<TDItem> {
            private int cur= 0;
            
            @Override
            public boolean hasNext() {
                return cur != tdItems.length;
            }

            @Override
            public TDItem next() {
                TDItem item = tdItems[cur];
                ++cur;
                return item;
            }
        }
        return new iter();
    }

    private static final long serialVersionUID = 1L;
    private TDItem[] tdItems;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) throws IllegalArgumentException {
        // some code goes here
    	if (typeAr == null || typeAr.length == 0) {
    		throw new IllegalArgumentException("typeAr must contain as least one entry");
    	}
    	this.tdItems = new TDItem[typeAr.length];
    	for (int i = 0; i < typeAr.length; ++i) {
    		this.tdItems[i] = new TDItem(typeAr[i], fieldAr==null?"":fieldAr[i]);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	this(typeAr, null);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the i'th field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the i'th field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	if (i < 0 || i >= this.tdItems.length) {
    		throw new NoSuchElementException();
    	}
        return this.tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	if (i < 0 || i >= this.tdItems.length) {
    		throw new NoSuchElementException();
    	}
        return this.tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
    	for (int i = 0; i < this.tdItems.length; ++i) {
    		if (tdItems[i].fieldName.equals(name)) {
    			return i;
    		}
    	}
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int ans = 0;
    	for (TDItem item : this.tdItems) {
    		ans += item.fieldType.getLen();
    	}
        return ans;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
    	TupleDesc res = new TupleDesc(new Type[1]);
    	int size = td1.numFields() + td2.numFields();
    	res.tdItems = new TDItem[size];
    	for (int i = 0; i < td1.numFields(); ++i) {
    		res.tdItems[i] = td1.tdItems[i];
    	}
    	for (int i = td1.numFields(); i < size; ++i) {
    		res.tdItems[i] = td2.tdItems[i - td1.numFields()];
    	}
        return res;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
    	if (o == null) return false;
    	TupleDesc desc = null;
    	try {
    		desc = (TupleDesc)o;
    	} catch (ClassCastException e) {
			return false;
		}
    	if (this.tdItems.length != desc.numFields()) {
    		return false;
    	}
    	for (int i = 0; i < this.tdItems.length; ++i) {
    		if (!this.tdItems[i].fieldType.equals(desc.getFieldType(i)) ||
    			!this.tdItems[i].fieldName.equals(desc.getFieldName(i))) {
    			return false;
    		}
    	}
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return this.toString().hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	StringBuilder res = new StringBuilder();
    	for (int i = 0; i < this.tdItems.length - 1; ++i) {
    		res.append(this.tdItems[i]);
    		res.append(", ");
    	}
    	res.append(this.tdItems[this.tdItems.length - 1]);
        return res.toString();
    }
}
