/**
 * External Sort algorithm: sorts the given relation
 **/

package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.*;
import java.util.*;

public class ExternalSort extends Operator {

    static int fileNum = 0;                 // To get unique filenum for this operation
    Operator base;
    ArrayList<Integer> indexes;             // The indexes needed for join condition (also the index to sort on)
    int numBuff;                            // Number of buffers available
    ArrayList<File> sortedRuns;             //stores the list of sorted runs
    ArrayList<File> resultSortedRuns;       //stores the temporary final result of sorted runs
    ObjectInputStream in;                   // for reading in files

    String fileName;                        // Corresponding file name
    int batchSize;                          // Number of tuples per out batch;
    int numOfBatch;                         // Number of batches this relation has

    int numOfRuns;                          // number of runs
    Batch batch;                            // current batch
    Batch newBatch;                         // separate current batch to be used within inner loop
    ArrayList<Tuple> tuplesInMainMem;       // stores the tuples read from disk into main memory

    int mergeRound;                         // indicates which merge round of the merge this is
    int round;                              // indicates which run its merging now (within a merge round)
    static int joinRound = 0;               // unique file num for indicating which join this is

    boolean isFirstNext = true;             // indicates whether Next() function has been called
    ArrayList<File> allFiles;               // keep track of all files for deletion later

    //for sortmerge
    public ExternalSort(Operator base, ArrayList<Integer> indexes, int numBuff, String fileName) {
        super(OpType.SORT);
        this.fileName = fileName;
        this.numBuff = numBuff;
        this.base = base;
        this.indexes = indexes;
    }

    //for distinct
    public ExternalSort(Operator base, ArrayList<Attribute> attrList, int type) {
        super(type);
        this.base = base;
        this.indexes = new ArrayList<>(attrList.size());
        Schema baseSchema = base.getSchema();
        for (int i = 0; i < attrList.size(); i++) {
            Attribute a = attrList.get(i);
            indexes.add(i, baseSchema.indexOf(a));
        }
        this.numBuff = BufferManager.getTotalBuffers();
        this.fileName = "distinct";
    }

    public boolean open() {
        if(!base.open()) {
            return false;
        }

        /** select number of tuples per batch **/
        int tuplesize = base.getSchema().getTupleSize();
        batchSize = Batch.getPageSize() / tuplesize;

        // Phase 1
        tuplesInMainMem = new ArrayList<>();
        allFiles = new ArrayList<>();
        sortedRuns = new ArrayList<>();
        generateSortedRuns();

        // Phase 2
        performMerge();

        return true;
    }

    /**
     * Generate the sorted runs for the relation and write them out using all numBuff pages
     */
    private void generateSortedRuns() {
        numOfRuns = 0;
        batch = base.next();
        numOfBatch = 0;
        ArrayList<Batch> oneRun = new ArrayList<>();
        List<Batch> oneSortedRun = new ArrayList<>();

        while (batch != null) {
            //read in as many pages to form one run
            for (int i = 0; (i < numBuff) && (batch != null); i++) {
                oneRun.add(batch);
                batch = base.next();
                numOfBatch++;
            }

            //store the tuples of one run in main mem for sorting
            for(Batch b: oneRun) {
                for (int j = 0; j < b.size(); j++) {
                    tuplesInMainMem.add(b.get(j));

                }
            }

            Collections.sort(tuplesInMainMem, ((t1, t2) -> Tuple.compareTuples(t1, t2, indexes)));

            //store the sorted results into pages
            newBatch = new Batch(batchSize);
            while (tuplesInMainMem.size() != 0) {
                Tuple t = tuplesInMainMem.remove(0);
                newBatch.add(t);
                if (newBatch.isFull()) {
                    oneSortedRun.add(newBatch);
                    newBatch = new Batch(batchSize);
                }
            }
            //take care of leftover tuples
            if (!newBatch.isFull()) {
                oneSortedRun.add(newBatch);
            }

            // write out the run to file
            File smTemp = new File(fileName + "-SMTemp-" + fileNum);
            smTemp = writeOneRun(oneSortedRun, smTemp);
            allFiles.add(smTemp);
            sortedRuns.add(smTemp);
            numOfRuns++;
            fileNum++;

            //reset for next run
            oneRun = new ArrayList<>();
            oneSortedRun = new ArrayList<>();
        }
    }


    /**
     * Merge the runs using numBuff - 1 buffers, keep 1 for writing output
     * Write out 1 resultant run
     */
    private void performMerge() {
        int numOfInputBuffers = numBuff - 1;
        File resultantRun;
        List<File> runsToMerge;
        mergeRound = 0;
        round = 0;
        joinRound++;

        //System.out.println("perform merge");

        // keep merging runs till we are left with 1 sorted run
        while (sortedRuns.size() > 1) {
            resultSortedRuns = new ArrayList<>();
//            System.out.println("sorted runs size > 1 "+ sortedRuns.size());
            int start = 0;
            int end = numOfInputBuffers;

            //read a window of numOfInputBuffers runs at once for merging
            while (end < sortedRuns.size()) {
//                System.out.println("sorted runs size BEFORE "+ sortedRuns.size());
                runsToMerge = sortedRuns.subList(start, end);
//                System.out.println("sorted runs size AFTER "+ sortedRuns.size());
//                System.out.println("start: "+ start + " end: "+ end);
                resultantRun = mergeSortedRuns(runsToMerge);
//                System.out.println("sorted runs size AFTER AFTER"+ sortedRuns.size());
                resultSortedRuns.add(resultantRun);
                //move the window down
                start = end;
                end += numOfInputBuffers;
//                System.out.println("SHIFT WINDOW");
//                System.out.println("new start " + start);
//                System.out.println("new end " + end);
                round++;
            }
//            System.out.println("sorted runs size "+ sortedRuns.size());

            //take care of the last window
            if (start < sortedRuns.size() && end >= sortedRuns.size()) {
//                System.out.println("ENTER LAST WINDOW");
                end = sortedRuns.size() ;
                runsToMerge = sortedRuns.subList(start, end);
//                System.out.println("start: "+ start + " end: "+ end);
                resultantRun = mergeSortedRuns(runsToMerge);
                allFiles.add(resultantRun);
                resultSortedRuns.add(resultantRun);
//                System.out.println("LAST WINDOW");
                round++;
            }
//            System.out.println("results runs "+resultSortedRuns.size());

            mergeRound++;
//            System.out.println("MERGE ROUND IS "+ mergeRound);

            //update sortedRuns for next round
            sortedRuns = resultSortedRuns;
//            System.out.println("after 1 round sorted runs size "+ sortedRuns.size());
        }

        //Checking
//        System.out.println();
//        System.out.println("CHECKING");
//        System.out.println("after 1 round sorted runs size "+ sortedRuns.size());
//
//        for (File f: sortedRuns) {
//            ObjectInputStream ois = initialiseInputStream(f);
//            Batch b = readPage(ois);
//            while (b!= null) {
//                Debug.PPrint(b);
//                b = readPage(ois);
//            }
//        }
//
//        System.out.println("-----------------------------------------");

    }

    /**
     * Use the 1 leftover buffer page to merge the numBuff-1 sorted runs and return it
     */
    private File mergeSortedRuns(List<File> runsToMerge) {
        Batch outputPage = new Batch(batchSize);
        Batch currPage;
        int pointerInCurrPage;
        int numOfRunsDoneMerging = 0;
        int numOfRunsToMerge = runsToMerge.size();
//        System.out.println("RUNS TO MERGE SIZE "+numOfRunsToMerge);

        ArrayList<Boolean> finishEachRun = new ArrayList<>(numOfRunsToMerge);
        ArrayList<Integer> pointersInEachPage = new ArrayList<>(numOfRunsToMerge);
        ArrayList<ObjectInputStream> inputStreamsOfRuns = new ArrayList<>(numOfRunsToMerge);
        ArrayList<Batch> pagesOfEachRun = new ArrayList<>(numOfRunsToMerge);

        File outputRun= new File(fileName + "-SMTempMergeStep-" + joinRound + "-" + round + "-" + mergeRound);
        ObjectOutputStream out = initialiseOutputStream(outputRun);
        allFiles.add(outputRun);

        // for each run, set up the input stream and the first page,
        // as well as the tuple pointers and boolean indicator of whether the run has been fully read
        for (int i = 0; i < numOfRunsToMerge; i++) {
            ObjectInputStream inForCurrRun = initialiseInputStream(runsToMerge.get(i));
//            System.out.println("read page from inForCurrRun");
            Batch pageForCurrRun = readPage(inForCurrRun);
            pagesOfEachRun.add(pageForCurrRun);
            inputStreamsOfRuns.add(inForCurrRun);
            pointersInEachPage.add(0);
            finishEachRun.add(false);
        }

        //perform the k way merge sort
        while (numOfRunsDoneMerging < numOfRunsToMerge) {
            int smallestPageIndex = 0;
            Tuple smallestTuple = null;
//            System.out.println("2 POINTERS PAGE SIZE "+ pointersInEachPage.size());

            //compare smallestTuple with the other tuples from each run
            for (int i = 0; i < numOfRunsToMerge ; i++) {
                //check if already finish reading this run
                if(finishEachRun.get(i)) {
                    continue;
                }

                currPage = pagesOfEachRun.get(i);
                pointerInCurrPage = pointersInEachPage.get(i);

                //already finish reading the batch for this run so load in new batch
                if ( currPage == null || pointerInCurrPage == currPage.size()) {
//                    System.out.println("pointer at the end of batch so load in new batch");
                    ObjectInputStream inForCurrRun = inputStreamsOfRuns.get(i);
                    currPage = readPage(inForCurrRun);
                    inputStreamsOfRuns.set(i,inForCurrRun);
                    pagesOfEachRun.set(i, currPage);

                    if (currPage == null || currPage.isEmpty()) {
                        closeInputStream(inForCurrRun);
                        finishEachRun.set(i,Boolean.TRUE);
                        numOfRunsDoneMerging++;
//                        System.out.println("num of runs done merging "+ numOfRunsDoneMerging);
                        continue;
                    }

                    pointerInCurrPage = 0;
                    pointersInEachPage.set(i, pointerInCurrPage);
//                    System.out.println("3 POINTERS PAGE SIZE "+ pointersInEachPage.size());
                }

                Tuple tuple = currPage.get(pointerInCurrPage);
//                if (smallestTuple!= null) {
//                    System.out.println("smallest tuple");
//                    Debug.PPrint(smallestTuple);
//                }
//                if (tuple!= null) {
//                    System.out.println(" tuple");
//                    Debug.PPrint(tuple);
//                }
                if (smallestTuple == null || Tuple.compareTuples(tuple, smallestTuple, indexes) < 0) {
                    smallestTuple = tuple;
                    smallestPageIndex = i;
                }

            }

            if (smallestTuple != null) {
//                System.out.println("smallest Tuple");
//                System.out.println(smallestPageIndex);
//                Debug.PPrint(smallestTuple);
                outputPage.add(smallestTuple);

                //increment the pointer in the page that was selected
                pointerInCurrPage = pointersInEachPage.get(smallestPageIndex);
                pointerInCurrPage++;
                pointersInEachPage.set(smallestPageIndex, pointerInCurrPage);
//                System.out.println("4 POINTERS PAGE SIZE "+ pointersInEachPage.size());
            }

            //write out the page if full
            if (outputPage.isFull()) {
                writePage(out, outputPage);
//                System.out.println("output page to write to object");
//                Debug.PPrint(outputPage);
//                System.out.println();
                outputPage = new Batch(batchSize);
            }
        }

        //leftover tuples in output page even if its not full
        if (!outputPage.isEmpty()) {
            writePage(out, outputPage);
        }

        closeOutputStream(out);
        for(ObjectInputStream i : inputStreamsOfRuns) {
            closeInputStream(i);
        }

//        System.out.println("FINISH ONE MERGE -------------------------------");
//        ObjectInputStream a = initialiseInputStream(outputRun);
//        Batch b = readPage(a);
//        while (b!= null) {
//            Debug.PPrint(b);
//            b = readPage(a);
//        }
//        closeInputStream(a);

        return outputRun;
    }

    /**
     * Initialise object input stream
     **/
    private ObjectInputStream initialiseInputStream(File file) {
        try {
            FileInputStream currRunFis = new FileInputStream(file);
            return new ObjectInputStream(currRunFis);
        } catch (IOException io) {
            System.out.println("External sort: fail to initialize input stream");
            return null;
        }
    }

    /**
     * Initialise object output stream
     **/
    private ObjectOutputStream initialiseOutputStream(File file) {
        try {
            FileOutputStream currRunFos = new FileOutputStream(file, true);
            return new ObjectOutputStream(currRunFos);
        } catch (IOException io) {
            System.out.println("External sort: fail to initialize output stream");
            return null;
        }
    }

    /**
     * Read in the page given the specified object output stream
     **/
    private Batch readPage(ObjectInputStream ois) {
        try {
            return (Batch) ois.readObject();
        } catch (ClassNotFoundException e) {
            System.out.println("ClassNotFoundException Thrown");
            System.exit(1);
        } catch (IOException io1) {
//            System.out.println("Error in External sort reading a page from a file");
            closeInputStream(ois);
        }
        return null;
    }

    /**
     * Write out the page given the specified object output stream
     **/
    private void writePage(ObjectOutputStream oos, Batch b) {
        try {
            oos.writeObject(b);
            oos.reset();
        } catch (IOException io1) {
//            System.out.println("Error in External sort writing out a page to a file");
            closeOutputStream(oos);
        }
    }

    /**
     * Write out the run into the specified file given the list of pages
     **/
    private File writeOneRun(List<Batch> batches, File file) {
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file));
            for (Batch b: batches) {
                out.writeObject(b);
            }
            out.close();
            return file;
        } catch (IOException e) {
//            System.out.println("ExternalSort: Error in writing out the pages to a file");
            return null;
        }
    }


    /**
     * Close the object input stream
     **/
    private void closeInputStream(ObjectInputStream ois) {
        try {
            ois.close();
        } catch (IOException io2) {
            System.out.println("Error in External sort closing input stream");
        }
    }

    /**
     * Close the object output stream
     **/
    private void closeOutputStream(ObjectOutputStream oos) {
        try {
            oos.close();
        } catch (IOException io2) {
            System.out.println("Error in External sort closing output stream");
        }
    }

    /**
     * Next operator - get a page from the file
     **/
    public Batch next() {
//        System.out.println("external sort batch called");
//        System.out.println("sorted pages list size "+ sortedRuns.size());

        if (isFirstNext) {
            if (sortedRuns.size() != 1) {
                System.out.println("ERROR: not 1 last sorted run");
                return null;
            }
            in = initialiseInputStream(sortedRuns.remove(0));
            isFirstNext = false;
        }

        batch = readPage(in);
        return batch;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }

    public void setNumBuff(int numBuff) {
        this.numBuff = numBuff;
    }

    public int getNumBuff() {
        return numBuff;
    }


    /**
     * Close the operator
     */
    public boolean close() {
        closeInputStream(in);
        sortedRuns.clear();
        for (File f: allFiles) {
            f.delete();
        }
        return true;
    }
}




