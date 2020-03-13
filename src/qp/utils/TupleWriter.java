/**
 * TupleWriter is a helper class that allows other operators to write tuples to a file in a Batch by Batch form
 */

package qp.utils;

import qp.utils.*;

import java.util.*;
import java.io.*;

public class TupleWriter {

    final String filename;    // Filename to write to
    final int batchsize;        // Number of tuples per out batch

    Batch outBatch;
    int numBatch = 0;                        // Number of batch written
    int numTuple = 0;                        // Number of tuples added
    ObjectOutputStream out;            // Output file stream

    // filename: Filename of the output file to write to
    // batchsize: Number of tuples per batch
    public TupleWriter(String filename, int batchsize) {
        this.filename = filename;
        this.batchsize = batchsize;
    }

    public int getNumBatch() {
        return numBatch;
    }

    public int getNumTuple() {
        return numTuple;
    }

    public int getBatchSize() {
        return batchsize;
    }

    public String getFileName() {
        return filename;
    }

    // Opens the file and initializes the class for writing
    public boolean open() {
        try {
            out = new ObjectOutputStream(new FileOutputStream(filename));
        } catch (IOException io) {
            System.out.printf("%s:writing the temporary file error", filename);
            return false;
        }
        outBatch = null;
        numBatch = 0;
        numTuple = 0;
        return true;
    }

    // Supplies a tuple to the tuple writer
    // Tuple writer will store the tuples and writes to the file on Batch at a time
    public boolean next(Tuple nextTuple) {
        if (outBatch == null) {
            outBatch = new Batch(batchsize);
        }
        outBatch.add(nextTuple);
        ++numTuple;
        if (outBatch.isFull())
            writeBatch();        // Immediately writes to file when we have enough tuples to fill a BAtch
        return true;
    }

    // Helper method to write a single batch to the output file
    private void writeBatch() {
        try {
            out.writeObject(outBatch);
            outBatch = null;        // deallocate output buffer --> saves memory
            ++numBatch;
        } catch (IOException io) {
            System.out.printf("%s:writing the temporary file error", filename);
            System.out.println(io);
            System.exit(1);
        }
    }

    // Signals the TupleWriter to finish writing all the tuples to the file
    public boolean close() {
        if (outBatch != null) writeBatch();  // Unfilled batch (if any) will be flushed to the file
        if (out != null) {
            try {
                out.close();
                out = null;
            } catch (IOException io) {
                System.out.printf("%s:writing the temporary file error", filename);
                System.out.println(io);
                System.exit(1);
            }
        }
        return true;
    }
}
