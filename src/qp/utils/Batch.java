/**
 * Batch represents a page
 **/

package qp.utils;

import java.util.ArrayList;
import java.io.Serializable;

public class Batch implements Serializable {

    int MAX_SIZE;             // Number of tuples per page
    static int PageSize;      // Number of bytes per page
    ArrayList<Tuple> tuples;  // The tuples in the page

    /** Set number of bytes per page **/
    public static void setPageSize(int size) {
        PageSize = size;
    }

    /** Get number of bytes per page **/
    public static int getPageSize() {
        return PageSize;
    }

    /** Number of tuples per page **/
    public Batch(int numtuple) {
        MAX_SIZE = numtuple;
        tuples = new ArrayList<>(MAX_SIZE);
    }

    /** Insert the record in page at next free location **/
    public void add(Tuple t) {
        tuples.add(t);
    }

    public int capacity() {
        return MAX_SIZE;
    }

    public void clear() {
        tuples.clear();
    }

    public boolean contains(Tuple t) {
        return tuples.contains(t);
    }

    public Tuple get(int i) {
        return tuples.get(i);
    }

    public int indexOf(Tuple t) {
        return tuples.indexOf(t);
    }

    public void add(Tuple t, int i) {
        tuples.add(i, t);
    }

    public boolean isEmpty() {
        return tuples.isEmpty();
    }

    public void remove(int i) {
        tuples.remove(i);
    }

    public void set(Tuple t, int i) {
        tuples.set(i, t);
    }

    public int size() {
        return tuples.size();
    }

    public boolean isFull() {
        if (size() == capacity())
            return true;
        else
            return false;
    }
}