/**
 * Select Operation
 **/

package qp.operators;

import qp.utils.*;

public class Select extends Operator {

    Operator base;  // Base operator
    Condition con;  // Select condition
    int batchsize;  // Number of tuples per outbatch

    /**
     * The following fields are required during
     * * execution of the select operator
     **/
    boolean eos;     // Indicate whether end of stream is reached or not
    Batch inbatch;   // This is the current input buffer
    Batch outbatch;  // This is the current output buffer
    int start;       // Cursor position in the input buffer

    /**
     * constructor
     **/
    public Select(Operator base, Condition con, int type) {
        super(type);
        this.base = base;
        this.con = con;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Condition getCondition() {
        return con;
    }

    public void setCondition(Condition cn) {
        this.con = cn;
    }

    /**
     * Opens the connection to the base operator
     **/
    public boolean open() {
        eos = false;  // Since the stream is just opened
        start = 0;    // Set the cursor to starting position in input buffer

        /** Set number of tuples per page**/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (base.open())
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
                inbatch = base.next();
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
                /** If the condition is satisfied then
                 ** this tuple is added tot he output buffer
                 **/
                if (checkCondition(present))
                    outbatch.add(present);
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

    /**
     * closes the output connection
     * * i.e., no more pages to output
     **/
    public boolean close() {
        base.close();    // Added base.close
        return true;
    }

    /**
     * To check whether the selection condition is satisfied for
     * * the present tuple
     **/
    protected boolean checkCondition(Tuple tuple) {
        Attribute attr = con.getLhs();
        int index = schema.indexOf(attr);
        int datatype = schema.typeOf(attr);
        Object srcValue = tuple.dataAt(index);
        int exprtype = con.getExprType();

        if (datatype == Attribute.INT) {
            int srcVal = ((Integer) srcValue).intValue();
            int checkVal = 0;
            if (con.getRhs() instanceof String) {
                checkVal = Integer.parseInt((String) con.getRhs());
            } else if (con.getRhs() instanceof Attribute) {
                checkVal = ((Integer) tuple.dataAt(schema.indexOf((Attribute) con.getRhs()))).intValue();
            } else {
                System.out.println("Select: Malformed condition");
            }
            if (exprtype == Condition.LESSTHAN) {
                if (srcVal < checkVal)
                    return true;
            } else if (exprtype == Condition.GREATERTHAN) {
                if (srcVal > checkVal)
                    return true;
            } else if (exprtype == Condition.LTOE) {
                if (srcVal <= checkVal)
                    return true;
            } else if (exprtype == Condition.GTOE) {
                if (srcVal >= checkVal)
                    return true;
            } else if (exprtype == Condition.EQUAL) {
                if (srcVal == checkVal)
                    return true;
            } else if (exprtype == Condition.NOTEQUAL) {
                if (srcVal != checkVal)
                    return true;
            } else {
                System.out.println("Select: Incorrect condition operator");
            }
        } else if (datatype == Attribute.STRING) {
            String srcVal = (String) srcValue;
            int flag = 0;
            if (con.getRhs() instanceof String) {
                flag = srcVal.compareTo((String) con.getRhs());
            } else if (con.getRhs() instanceof Attribute) {
                flag = srcVal.compareTo(((String) tuple.dataAt(schema.indexOf((Attribute) con.getRhs()))));
            } else {
                System.out.println("Select: Malformed condition");
            }
            if (exprtype == Condition.LESSTHAN) {
                if (flag < 0) return true;
            } else if (exprtype == Condition.GREATERTHAN) {
                if (flag > 0) return true;
            } else if (exprtype == Condition.LTOE) {
                if (flag <= 0) return true;
            } else if (exprtype == Condition.GTOE) {
                if (flag >= 0) return true;
            } else if (exprtype == Condition.EQUAL) {
                if (flag == 0) return true;
            } else if (exprtype == Condition.NOTEQUAL) {
                if (flag != 0) return true;
            } else {
                System.out.println("Select: Incorrect condition operator");
            }
        } else if (datatype == Attribute.REAL) {
            float srcVal = ((Float) srcValue).floatValue();
            float checkVal = 0;
            if (con.getRhs() instanceof String) {
                checkVal = Float.parseFloat((String) con.getRhs());
            } else if (con.getRhs() instanceof Attribute) {
                checkVal = ((Float) tuple.dataAt(schema.indexOf((Attribute) con.getRhs()))).floatValue();
            } else {
                System.out.println("Select: Malformed condition");
            }
            if (exprtype == Condition.LESSTHAN) {
                if (srcVal < checkVal) return true;
            } else if (exprtype == Condition.GREATERTHAN) {
                if (srcVal > checkVal) return true;
            } else if (exprtype == Condition.LTOE) {
                if (srcVal <= checkVal) return true;
            } else if (exprtype == Condition.GTOE) {
                if (srcVal >= checkVal) return true;
            } else if (exprtype == Condition.EQUAL) {
                if (srcVal == checkVal) return true;
            } else if (exprtype == Condition.NOTEQUAL) {
                if (srcVal != checkVal) return true;
            } else {
                System.out.println("Select: Incorrect condition operator");
            }
        }
        return false;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Condition newcon = (Condition) con.clone();
        Select newsel = new Select(newbase, newcon, optype);
        newsel.setSchema((Schema) newbase.getSchema().clone());
        return newsel;
    }

}
