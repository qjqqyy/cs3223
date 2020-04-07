/**
 * To projec out the required attributes from the result
 **/

package qp.operators;

import qp.utils.*;

import java.util.ArrayList;
import java.util.HashMap;


public class Project extends Operator {

    Operator base;                                             // Base table to project
    ArrayList<Attribute> attrList;                              // Set of attributes to project
    int batchsize;                                             // Number of tuples per outbatch
    boolean isOutput = false;                                  // To check if final value has been output
    boolean hasAggregation;                                    // Boolean if any of the projection attributes
                                                               // are aggregated values

    /**
     * The following fields are requied during execution
     * * of the Project Operator
     **/
    Batch inbatch;
    Batch outbatch;

    /**
     * index of the attributes in the base operator
     * * that are to be projected
     **/
    int[] attrIndex;
    int[] tempStorageIndex;
    ArrayList<Pair> storeOrder;

    public Project(Operator base, ArrayList<Attribute> as, int type) {
        super(type);
        this.base = base;
        this.attrList = as;
        this.storeOrder = new ArrayList<>();
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getProjAttr() {
        return attrList;
    }


    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;

        /** The following loop finds the index of the columns that
         ** are required from the base operator
         **/
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrList.size()];
        boolean checked = false;

        for (int i = 0; i < attrList.size(); ++i) {
            Attribute attr = attrList.get(i);

            // check if any tuples has aggregation
            if (!checked && attr.getAggType() != Attribute.NONE) {
                hasAggregation = true;
                checked = true;
            }

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex[i] = index;

        }
        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);
        /** all the tuples in the inbuffer goes to the output buffer **/
        inbatch = base.next();

        if (!hasAggregation) {
            if (inbatch == null) {
                return null;
            }
            if (inbatch.size() == 0) {
                return outbatch;
            }
            for (int i = 0; i < inbatch.size(); i++) {
                Tuple basetuple = inbatch.get(i);
                ArrayList<Object> present = new ArrayList<>();
                for (int j = 0; j < attrList.size(); j++) {
                    Object data = basetuple.dataAt(attrIndex[j]);
                    present.add(data);
                }
                Tuple outtuple = new Tuple(present);
                outbatch.add(outtuple);
            }
            return outbatch;
        } else if (!isOutput && inbatch != null && inbatch.size() > 0) {
            // doing aggregation, might as well eat everything since we're not implementing groupby
            AggrProps ap = new AggrProps();
            for (int j = 0; j < attrList.size(); j++) {
                // build storeOrder exactly once
                int aggType = attrList.get(j).getAggType();
                if (aggType == Attribute.NONE) {
                    storeOrder.add(new Pair(aggType, inbatch.get(0).dataAt(attrIndex[j])));
                } else {
                    storeOrder.add(new Pair(aggType, attrIndex[j]));
                    ap.markAggregate(attrIndex[j], aggType);
                }
            }
            while (inbatch != null && inbatch.size() > 0) {
                // update intermediates
                ap.consume(inbatch);
                inbatch = base.next();
            }
            ArrayList<Object> present = new ArrayList<>();
            for (int i = 0; i < storeOrder.size(); i++) {
                Pair pair = storeOrder.get(i);
                int aggFunc = pair.getFirst();
                if (aggFunc == Attribute.NONE) {
                    present.add(pair.getSecond());
                } else {
                    present.add(ap.collect((Integer) pair.getSecond(), aggFunc));
                }
            }
            if (present.size() != 0) {
                Tuple outtuple = new Tuple(present);
                outbatch.add(outtuple);
            }
            isOutput = true;
            return outbatch;
        }
        return null;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrList.size(); ++i)
            newattr.add((Attribute) attrList.get(i).clone());
        Project newproj = new Project(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }

}
