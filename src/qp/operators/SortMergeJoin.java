/**
 * Sort Merge Join algorithm
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.util.ArrayList;

public class SortMergeJoin extends Join {

    int batchSize;                   // Number of tuples per out batch
    ArrayList<Integer> leftIndexes;   // Indices of the join attributes in left table
    ArrayList<Integer> rightIndexes;  // Indices of the join attributes in right table

    ExternalSort sortedLeft;        // sorted relations after external sort
    ExternalSort sortedRight;       // sorted relations after external sort

    Batch sortedLeftBatch;          // Buffer page for left input stream
    Batch sortedRightBatch;         // Buffer page for right input stream
    Batch outBatch;                 // Buffer page for output

    Tuple leftTuple;                // current tuple for left side buffer
    Tuple rightTuple;               // current tuple for right side buffer
    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer

    Tuple matchingTuple;            // stores the right Tuple to be matched with the corresponding left tuples
    ArrayList<Tuple> matchingTuples; // stores the right tuples of the same values to be matched with corresponing left tuples
    int mcurs;                      // cursor for matching tuple list

    boolean isFirstNext;
    boolean endOfLeftRelation;
    boolean endOfRightRelation;


    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tuplesize;

        if (batchSize == 0) {
            System.out.println("batch size is 0");
            return false;
        }

        /** find indices attributes of join conditions **/
        leftIndexes = new ArrayList<>();
        rightIndexes = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftIndexes.add(left.getSchema().indexOf(leftattr));
            rightIndexes.add(right.getSchema().indexOf(rightattr));
        }

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;

        isFirstNext = true;
        endOfLeftRelation = false;
        endOfRightRelation = false;

        // Call External sort on both left and right table to sort them
        sortedLeft = new ExternalSort(left, leftIndexes, numBuff, "leftRelation");
        sortedRight = new ExternalSort(right, rightIndexes, numBuff, "rightRelation");

//        System.out.println("FINISH SETTING EXTERNAL SORT");

        if (sortedLeft.open() == false || sortedRight.open() == false) {
            System.out.println("Failed to open external sort");
            return false;
        }

        //prepare the first pages of both relations
        sortedLeftBatch = sortedLeft.next();
        sortedRightBatch = sortedRight.next();

        matchingTuples = new ArrayList<>();
        mcurs = 0;

//        System.out.println("finish open smj");

        return true;
    }

    /**
     * Advance both pointers of left and right, store the right tuples of same value in an arrayList called matchingTuples
     * If left tuple matches the current matching tuple, perform the join on all matching tuples in the list (which has the same value)
     * Stop when reaches the end of one of the list
     * * And returns a page of output tuples
     **/
    public Batch next() {

        if (matchingTuples.isEmpty() && (sortedLeftBatch == null || sortedRightBatch == null || sortedLeftBatch.size() == 0 || sortedRightBatch.size() == 0)) {
            //no more batch to be read
            close();
            return null;
        }

        //is is the very first next, get the matching tuple and fill up the matchingTuples
        if (isFirstNext) {

            matchingTuple = sortedRightBatch.get(0);
//            System.out.println("MATCHING TUPLE ");
//            Debug.PPrint(matchingTuple);

            // store the matching right tuples with the same values into the arraylist for comparison later
            while (sortedRightBatch != null || sortedRightBatch.size() == 0) {
                rightTuple = sortedRightBatch.get(rcurs);

                if (Tuple.compareTuples(matchingTuple, rightTuple, rightIndexes) != 0) {
                    break;
                } else {
                    matchingTuples.add(rightTuple);
//                    Debug.PPrint(rightTuple);
                    if (!updateRightPointer()) {
                        break;
                    }
                }
            }
//            System.out.println("IS FIRST NEXT");
            isFirstNext = false;
        }

        outBatch = new Batch(batchSize);

        while (!outBatch.isFull()) {
//            System.out.println("NOT FULL");

            //Not the end of relation yet
            if (!endOfLeftRelation && !endOfRightRelation) {

                leftTuple = sortedLeftBatch.get(lcurs);
//                System.out.println("---------------");
//                Debug.PPrint(leftTuple);
//                Debug.PPrint(matchingTuple);

                //same so do the match
                if (Tuple.compareTuples(leftTuple, matchingTuple, leftIndexes, rightIndexes) == 0) {
//                    System.out.println("MATCH ");

                    if (!joinSameTuples()) {
                        return outBatch;
                    }

                    if (mcurs >= matchingTuples.size()) {
                        mcurs = 0;
                        if (!updateLeftPointer()) {
                            return outBatch;
                        }
                    }

                // right is greater
                } else if (Tuple.compareTuples(leftTuple, matchingTuple, leftIndexes, rightIndexes) < 0) {
//                    System.out.println("RIGHT GREATER");
                    if (!updateLeftPointer()) {
                        return outBatch;
                    }

                // left is greater
                } else {
//                    System.out.println("LEFT GREATER ");
                    matchingTuples.clear(); // clear the right tuples to add the new right tuple
                    mcurs = 0;

                    // Get the next right tuple since left tuple is larger now
                    while (Tuple.compareTuples(leftTuple, rightTuple, leftIndexes, rightIndexes) > 0) {
                        if (!updateRightPointer()) {
                            return outBatch;
                        } else {
                            //clear the old matching tuples since right tuple is updated
                            matchingTuples.clear();
                        }
                        rightTuple = sortedRightBatch.get(rcurs);
                    }

                    matchingTuple = sortedRightBatch.get(rcurs);

                    // store the matching right tuples with the same values into the arraylist for comparison later
                    while (sortedRightBatch != null || sortedRightBatch.size() != 0) {
                        rightTuple = sortedRightBatch.get(rcurs);
                        if (Tuple.compareTuples(matchingTuple, rightTuple, rightIndexes) != 0) {
                            break;
                        } else {
                            matchingTuples.add(rightTuple);
                            if (!updateRightPointer()) {
                                break;
                            }
                        }
                    }

                }

            } else {

                // end of relation
                // so join whatever remaining matching tuples if present and return
                if(matchingTuples == null || matchingTuples.isEmpty()) {
                    close();
                    return null;
                } else {

                    while (!outBatch.isFull()) {
                        leftTuple = sortedLeftBatch.get(lcurs);
//                        System.out.println("---------------");
//                        Debug.PPrint(leftTuple);
//                        Debug.PPrint(matchingTuple);

                        //same so do the match
                        if (Tuple.compareTuples(leftTuple, matchingTuple, leftIndexes, rightIndexes) == 0) {

//                            System.out.println("MATCH eof");
                            if (!joinSameTuples()) {
                                return outBatch;
                            }

                            if (mcurs >= matchingTuples.size()) {
                                mcurs = 0;
                                if (!updateLeftPointer()) {
                                    return outBatch;
                                }
                            }

                        //right is greater
                        } else if (Tuple.compareTuples(leftTuple, matchingTuple, leftIndexes, rightIndexes) < 0) {
//                            System.out.println("RIGHT GREATER eof");
                            if (!updateLeftPointer()) {
                                return outBatch;
                            }

                        // left is larger
                        } else {
//                            System.out.println("LEFT GREATER eof");
                            matchingTuples.clear();
                            mcurs = 0;
                            return outBatch;
                        }
                    }
                }

            }
        }
        return outBatch;
    }

    // Join the tuples with the same value in the matchingTuples with the current left tuple
    public boolean joinSameTuples() {
        while (mcurs < matchingTuples.size()) {
            Tuple t = leftTuple.joinWith(matchingTuples.get(mcurs));
            outBatch.add(t);
            mcurs++;
            if (outBatch.isFull()) {
                return false;
            }
        }
        return true;
    }

    // UPDATE LEFT POINTERS, return false if end of relation
    private boolean updateLeftPointer() {
        lcurs++;
        if (lcurs >= sortedLeftBatch.size()) {
            //end of 1 batch reached and so get the next page
            sortedLeftBatch = sortedLeft.next();
            lcurs = 0;

            //end of whole relation
            if (sortedLeftBatch == null || sortedLeftBatch.isEmpty()) {
                endOfLeftRelation = true;
//                System.out.println("EOL");
                matchingTuples.clear();
                return false;
            }
        }
        return true;
    }

    // UPDATE RIGHT POINTERS, return false if end of relation
    private boolean updateRightPointer() {
        rcurs++;
        if (rcurs >= sortedRightBatch.size()) {
            //end of 1 batch reached and so get the next page
            sortedRightBatch = sortedRight.next();
            rcurs = 0;

            //end of whole relation
            if (sortedRightBatch == null || sortedRightBatch.isEmpty()) {
                endOfRightRelation = true;
//                System.out.println("EOR");
                return false;
            }
        }

        return true;
    }


    /**
     * Close both left and right external sorted relations
     */
    public boolean close() {
        return (sortedLeft.close() && sortedRight.close());
    }

}
