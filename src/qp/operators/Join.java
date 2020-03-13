/**
 * This is base class for all the join operators
 **/

package qp.operators;

import qp.utils.Condition;
import qp.utils.Schema;

import java.util.ArrayList;

public class Join extends Operator {

    Operator left;                       // Left child
    Operator right;                      // Right child
    ArrayList<Condition> conditionList;  // List of join conditions
    int numBuff;                         // Number of buffers available
    int jointype;                        // JoinType.NestedJoin/SortMerge/HashJoin
    int nodeIndex;                       // Each join node is given a number

    public Join(Operator left, Operator right, int type) {
        super(type);
        this.left = left;
        this.right = right;
        conditionList = new ArrayList<>();
    }

    public Join(Operator left, Operator right, Condition condition, int type) {
        super(type);
        this.left = left;
        this.right = right;
        conditionList = new ArrayList<>();
        conditionList.add(condition);
    }

    public Join(Operator left, Operator right, ArrayList<Condition> conditionList, int type) {
        super(type);
        this.left = left;
        this.right = right;
        this.conditionList = conditionList;
    }

    public int getNumBuff() {
        return numBuff;
    }

    public void setNumBuff(int num) {
        this.numBuff = num;
    }

    public int getNodeIndex() {
        return nodeIndex;
    }

    public void setNodeIndex(int num) {
        this.nodeIndex = num;
    }

    public int getJoinType() {
        return jointype;
    }

    public void setJoinType(int type) {
        this.jointype = type;
    }

    public Operator getLeft() {
        return left;
    }

    public void setLeft(Operator left) {
        this.left = left;
    }

    public Operator getRight() {
        return right;
    }

    public void setRight(Operator right) {
        this.right = right;
    }

    public Condition getCondition() {
        assert (conditionList.size() > 0);
        return conditionList.get(0);
    }

    public void setCondition(Condition condition) {
        conditionList = new ArrayList<>();
        conditionList.add(condition);
    }

    public ArrayList<Condition> getConditionList() {
        return conditionList;
    }

    public void setConditionList(ArrayList<Condition> conditionList) {
        this.conditionList = conditionList;
    }

    public void addCondition(Condition condition) {
        conditionList.add(condition);
    }

    public Object clone() {
        Operator newleft = (Operator) left.clone();
        Operator newright = (Operator) right.clone();
        ArrayList<Condition> newcondlist = new ArrayList<>();
        for (Condition cond : conditionList) {
            newcondlist.add((Condition) cond.clone());
        }
        Join jn = new Join(newleft, newright, newcondlist, optype);
        Schema newsche = newleft.getSchema().joinWith(newright.getSchema());
        jn.setSchema(newsche);
        jn.setJoinType(jointype);
        jn.setNodeIndex(nodeIndex);
        jn.setNumBuff(numBuff);
        return jn;
    }

}
