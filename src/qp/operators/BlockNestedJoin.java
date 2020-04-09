/**
 * Algorithm for Block Nested Join.
 */

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.List;

public class BlockNestedJoin extends Join {

    Batch outputBuffer;              // Buffer page for output
    Batch innerBuffer;               // Buffer page for inner relation
    List<Batch> outerBuffer;         // Buffer pages for outer relation (Size is numBuff - 2)
    int batchSize;                   // Number of tuples per output batch
    String innerFileName;            // The file name where the inner table is materialized
    ObjectInputStream in;            // File pointer to the right hand materialized file
    ArrayList<Integer> outerIndex;   // Indices of the join attributes in left (outer) table
    ArrayList<Integer> innerIndex;   // Indices of the join attributes in right (inner) table
    static int fileNumber = 0;       // To get unique filenum for this operation


    int outCursor;                   // Cursor for outer buffer
    int inCursor;                    // Cursor for inner buffer
    int bufferCursor;                // Cursor for iterating num buffers
    boolean eosOuter;                // Whether end of stream (outer) is reached
    boolean eosInner;                // Whether end of stream (inner) is reached

    public BlockNestedJoin(Join join) {
        super(join.getLeft(), join.getRight(), join.getConditionList(), join.getOpType());
        schema = join.getSchema();
        jointype = join.getJoinType();
        numBuff = join.getNumBuff();
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

        /** find indices attributes of join conditions **/
        outerIndex = new ArrayList<>();
        innerIndex = new ArrayList<>();
        outerBuffer = new ArrayList<>();


        for (Condition con : conditionList) {
            Attribute leftAttr = con.getLhs();
            Attribute rightAttr = (Attribute) con.getRhs();
            outerIndex.add(left.getSchema().indexOf(leftAttr));
            innerIndex.add(right.getSchema().indexOf(rightAttr));
        }
        Batch innerPage;

        /** initialize the cursors of input buffers **/
        outCursor = 0;
        inCursor = 0;
        bufferCursor = 0;
        eosOuter = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosInner = true;

        /** Right hand side table is to be materialized
         ** for the Nested join to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            fileNumber++;
            innerFileName = "NJtemp-" + String.valueOf(fileNumber);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(innerFileName));
                while ((innerPage = right.next()) != null) {
                    out.writeObject(innerPage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("NestedJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
        }
        if (left.open())
            return true;
        else
            return false;
    }


    /**
     * Compares tuples from the outer (left) table and the inner (right) table and outputs a page of tuples that
     * satisfy the join condition.
     **/
    public Batch next() {
        if (eosOuter) {
            return null;
        }
        outputBuffer = new Batch(batchSize);
        while (!outputBuffer.isFull()) {

            /** new outer block needs to be fetched to fill outer buffers **/
            if (outCursor == 0 && eosInner) {
                outerBuffer = new ArrayList<>();
                for (int i = 0; i < numBuff - 2; i++) {

                    /**outer buffer may not be completely full **/
                    Batch tuples = (Batch) left.next();

                    if (tuples != null) {
                        outerBuffer.add(i, tuples);
                    } else {
                        break;
                    }
                }
                /** if outer buffer doesnt have any new entries **/
                if (outerBuffer.isEmpty()) {
                    eosOuter = true;
                    return outputBuffer;
                }
                /** Whenever a new outer block comes in, we have to start the
                 ** scanning of inner page
                 **/
                try {
                    in = new ObjectInputStream(new FileInputStream(innerFileName));
                    eosInner = false;

                } catch (IOException io) {
                    System.err.println("Block Nested Join: Error in reading the inner file");
                    System.exit(1);
                }
            }

           /** while haven't reach end of stream of inner table **/
           while(!eosInner) {
               try {
                   /** fetch new inner page only if all cursors are at 0. Otherwise it implies that not all outer
                    ** pages have been scanned against inner pages and so dont fetch a new inner page yet.
                    **/
                   if (inCursor == 0 && outCursor == 0 && bufferCursor == 0) {
                       innerBuffer = (Batch) in.readObject();
                   }

                   /** Loops through the outermost Arraylist of buffers and for each tuple in each outer page
                    ** the tuples in the inner page are checked against the join condition.
                    **/
                   for (int i = bufferCursor; i < outerBuffer.size(); i++) {
                       for (int j = outCursor; j < outerBuffer.get(i).size(); j++) {
                           for (int k = inCursor; k < innerBuffer.size(); k++) {
                               Tuple outerTuple = outerBuffer.get(i).get(j);
                               Tuple innerTuple = innerBuffer.get(k);

                               /** if join check passes, add to output tuple **/
                               if (outerTuple.checkJoin(innerTuple, outerIndex, innerIndex)) {
                                   Tuple outputTuple = outerTuple.joinWith(innerTuple);
                                   outputBuffer.add(outputTuple);

                                   /** manipulation of cursors if output buffer is full but haven't scanned everything,
                                    ** so that there is no duplication or missing tuples from final result
                                    **/
                                   if (outputBuffer.isFull()) {
                                       if (j == outerBuffer.get(i).size() - 1 && k == innerBuffer.size() - 1) {  //case 1
                                           bufferCursor = i + 1;
                                           outCursor = 0;
                                           inCursor = 0;
                                       } else if (j != outerBuffer.get(i).size() - 1 && k == innerBuffer.size() - 1) {  //case 2
                                           bufferCursor = i;
                                           outCursor = j + 1;
                                           inCursor = 0;
                                       } else if (j == outerBuffer.get(i).size() - 1 && k != innerBuffer.size() - 1) {  //case 3
                                           bufferCursor = i;
                                           outCursor = j;
                                           inCursor = k + 1;
                                       } else {

                                           bufferCursor = i;
                                           outCursor = j;
                                           inCursor = k + 1;
                                       }
                                       return outputBuffer;
                                   }
                               }
                           }
                           inCursor = 0;
                       }
                       outCursor = 0;
                   }
                   bufferCursor = 0;
                   innerBuffer.clear();
               } catch (EOFException e) {
                   try {
                       in.close();
                   } catch (IOException io) {
                       System.out.println("Block Nested Join: Errorrrr in reading temporary file");
                   }
                   eosInner = true;
               } catch (ClassNotFoundException c) {
                   System.out.println("Block Nested Join: Error in deserialising temporary file ");
                   System.exit(1);
               } catch (IOException io) {
                   System.out.println("Block Nested Join: Error in reading temporary file");
                   System.exit(1);
               }
           }

        }
        return outputBuffer;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File file = new File(innerFileName);
        file.delete();
        return true;
    }


}

