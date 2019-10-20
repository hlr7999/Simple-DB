package simpledb;

import simpledb.Predicate.Op;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	
	private final int[] buckets;
	private final int max;
	private final int min;
	private int ntups;
	private final int width;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.buckets = new int[buckets];
    	this.max = max;
    	this.min = min;
    	this.ntups = 0;
    	this.width = (max-min)/buckets + 1;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	if (v < min || v > max) {
    		throw new IllegalArgumentException("value out of range");
    	}
    	++buckets[(v-min)/width];
    	++ntups;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        switch (op) {
		case EQUALS:
			if (v < min || v > max) {
				return 0.0;
			} else {
				double num = buckets[(v-min)/width] / (double)width;
				return num / ntups;
			}
		case LESS_THAN:
			if (v <= min) {
                return 0.0;
            } else if (v > max) {
                return 1.0;
            } else {
                int index = (v-min)/width;
                double count = 0;
                for (int i = 0; i < index; ++i) {
                    count += buckets[i];
                }
                count += buckets[index]/(double)width*(v-index*width-min);
                return count/ntups;
            }
		case LESS_THAN_OR_EQ:
			return estimateSelectivity(Op.LESS_THAN, v+1);
		case GREATER_THAN:
			return 1 - estimateSelectivity(Op.LESS_THAN_OR_EQ, v);
		case GREATER_THAN_OR_EQ:
			return estimateSelectivity(Op.GREATER_THAN, v-1);
		case NOT_EQUALS:
			return 1- estimateSelectivity(Op.EQUALS, v);
		default:
			throw new IllegalArgumentException("illegal op");
		}
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
    	return String.format("IntHistgram(buckets=%d, min=%d, max=%d",
            buckets.length, min, max);
    }
}
