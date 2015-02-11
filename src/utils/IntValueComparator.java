package utils;


public class IntValueComparator implements java.util.Comparator<Integer> {

    java.util.Map<Integer, Integer> base;
    public IntValueComparator(java.util.Map<Integer, Integer> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals. 
    //sorts descending - from the largest to the smallest
    public int compare(Integer a, Integer b) {
        if (base.get(b) <= base.get(a)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}