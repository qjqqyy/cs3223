/**
 * Select Operation
 **/

package qp.operators;

import qp.utils.*;

import java.util.ArrayList;

public class Distinct extends ExternalSort {

    Operator base;
    int batchsize;  // Number of tuples per outbatch
    ArrayList<Integer> indexes;
    ArrayList<Attribute> attrList;

    /**
     * The following fields are required during
     * * execution of the select operator
     **/
    boolean eos;     // Indicate whether end of stream is reached or not
    Batch inbatch;   // This is the current input buffer
    Batch outbatch;  // This is the current output buffer
    int start;       // Cursor position in the input buffer
    Tuple prevTuple; //stores the last seen tuple

    /**
     * constructor
     **/
    public Distinct(Operator base, ArrayList<Attribute> attrList, int type) {
        super(base, attrList, type);
        this.attrList = attrList;
        this.base = base;
    }

    /**
     * Opens the connection to the base operator
     **/
    public boolean open() {

        eos = false;  // Since the stream is just opened
        start = 0;    // Set the cursor to starting position in input buffer
        prevTuple = null;

        /** Set number of tuples per page**/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        indexes = new ArrayList<>(attrList.size());
        Schema baseSchema = base.getSchema();
        for (int i = 0; i < attrList.size(); i++) {
            Attribute a = attrList.get(i);
            indexes.add(i, baseSchema.indexOf(a));
        }

        if (super.open())
            return true;
        else
            return false;
    }

    /**
     * returns a batch of tuples that satisfies the
     * * condition specified on the tuples coming from base operator
     * * NOTE: This operation is performed on the fly
     **/
    public Batch next() {
        int i = 0;
        if (eos) {
            close();
            return null;
        }

        /** An output buffer is initiated **/
        outbatch = new Batch(batchsize);

        /** keep on checking the incoming pages until
         ** the output buffer is full
         **/
        while (!outbatch.isFull()) {
            if (start == 0) {
                inbatch = super.next();
                /** There is no more incoming pages from base operator **/
                if (inbatch == null) {
                    eos = true;
                    return outbatch;
                }
            }

            /** Continue this for loop until this page is fully observed
             ** or the output buffer is full
             **/
            for (i = start; i < inbatch.size() && (!outbatch.isFull()); ++i) {
                Tuple present = inbatch.get(i);
                /** If this tuple is distinct then
                 ** this tuple is added tot he output buffer
                 **/
                if (prevTuple == null || isDistinct(prevTuple, present)) {
                    outbatch.add(present);
                    prevTuple = present;
                }
            }

            /** Modify the cursor to the position requierd
             ** when the base operator is called next time;
             **/
            if (i == inbatch.size())
                start = 0;
            else
                start = i;
        }
        return outbatch;
    }


    private boolean isDistinct(Tuple t1, Tuple t2) {
        return (Tuple.compareTuples(t1,t2, indexes) != 0);
    }

    /**
     * closes the output connection
     * * i.e., no more pages to output
     **/
    public boolean close() {
        base.close();    // Added base.close
        super.close();
        return true;
    }

    @Override
    public Object clone() {
        Operator newBase = (Operator) base.clone();
        ArrayList<Attribute> newAttrList = (ArrayList<Attribute>) attrList.clone();
        Distinct newDistinct = new Distinct(newBase, newAttrList, optype);
        newDistinct.setSchema(newBase.getSchema());
        return newDistinct;
    }

}
