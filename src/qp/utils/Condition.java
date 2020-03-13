/**
 * represents join/select condition
 **/

package qp.utils;

public class Condition {

    public static final int LESSTHAN = 1;
    public static final int GREATERTHAN = 2;
    public static final int LTOE = 3;
    public static final int GTOE = 4;
    public static final int EQUAL = 5;
    public static final int NOTEQUAL = 6;

    public static final int SELECT = 1;
    public static final int JOIN = 2;

    Attribute lhs; // Left hand side of the condition
    int optype;    // Operation type
    int exprtype;  // Comparision type
    Object rhs;    // Attribute for Join condition and String or Attribute for Select Condition

    public Condition(Attribute attr, int type, Object value) {
        lhs = attr;
        exprtype = type;
        this.rhs = value;
    }

    public Condition(int type) {
        exprtype = type;
    }

    public Condition getFlippedCondition() {
        if (!(rhs instanceof Attribute)) {
            System.out.println("Condition: Error in flipping condition");
            System.exit(1);
        }
        Attribute r = (Attribute) rhs;
        int op = getExprType();
        if (op == LESSTHAN) op = GREATERTHAN;
        else if (op == GREATERTHAN) op = LESSTHAN;
        else if (op == LTOE) op = GTOE;
        else if (op == GTOE) op = LTOE;

        Condition flippedCondition = new Condition(r, op, lhs);
        flippedCondition.setOpType(getOpType());
        return flippedCondition;
    }

    public Attribute getLhs() {
        return lhs;
    }

    public void setLhs(Attribute attr) {
        lhs = attr;
    }

    public void setOpType(int num) {
        optype = num;
    }

    public int getOpType() {
        return optype;
    }

    public void setExprType(int num) {
        exprtype = num;
    }

    public int getExprType() {
        return exprtype;
    }

    public Object getRhs() {
        return rhs;
    }

    public void setRhs(Object value) {
        rhs = value;
    }

    public void flip() {
        if (optype == JOIN) {
            Object temp = lhs;
            lhs = (Attribute) rhs;
            rhs = temp;
        }
    }

    public Object clone() {
        Attribute newlhs = (Attribute) lhs.clone();
        Object newrhs;
        if (optype == SELECT)
            newrhs = (String) rhs;
        else
            newrhs = (Attribute) ((Attribute) rhs).clone();

        Condition newcn = new Condition(newlhs, exprtype, newrhs);
        newcn.setOpType(optype);
        return newcn;
    }
}
