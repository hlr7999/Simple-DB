  package simpledb;

/*
 *  IntHistogram and StringHistogram should implement this interface
 */
public interface Histogram {
    public double estimateSelectivity(Predicate.Op op, Field field);
    public double avgSelectivity();
    public void addValue(Field field);
}
