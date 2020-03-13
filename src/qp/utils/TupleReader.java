/**
 * TupleReader is a helper class that allows other operators to read tuples from a file in a Batch by Batch form
 */

package qp.utils;

import qp.utils.*;

import java.util.*;
import java.io.*;

public class TupleReader {

    final String filename;    // Filename to write to
    final int batchsize;        // Number of tuples per out batch

    Batch inBatch;                            // Currently buffered input
    int readCursor = 0;                    // Cursor within inBatch
    boolean completed = false;    // Whether EOF has been reached
    int numBatch = 0;                        // Number of batch read
    int numTuple = 0;                        // Number of tuples read
    Tuple peekTuple = null;            // The next tuple of the batch (if peeked)
    ObjectInputStream in;                // Input file stream

    // filename: Filename of the output file to read from
    // batchsize: Number of tuples per batch
    public TupleReader(String filename, int batchsize) {
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

    // Returns true if the end of the input file is encountered
    public boolean isEOF() {
        if (completed) return true;
        return (this.peek() == null);
    }

    // Opens the input file and initializes the class for reading
    public boolean open() {
        try {
            in = new ObjectInputStream(new FileInputStream(filename));
        } catch (IOException io) {
            System.out.printf("%s:reading the temporary file error", filename);
            return false;
        }
        inBatch = null;
        numBatch = 0;
        numTuple = 0;
        readCursor = 0;
        completed = false;
        return true;
    }

    // Allows the caller to 'peek' at the next tuple.
    // Returns the next tuple but does not actually removes it from the stream
    // This is lazily evaluated and will only read the next batch from the file if necessary
    public Tuple peek() {
        if (completed) return null;
        // Already know the next tuple
        if (peekTuple != null) return peekTuple;

        // If the next tuple is in the next batch, we have to read from the file
        if (inBatch == null) {
            try {
                while (true) {
                    inBatch = (Batch) in.readObject();
                    numBatch++;
                    if (inBatch.size() > 0) break;
                }
            } catch (EOFException e) {
                // No more batch in the file
                peekTuple = null;
                completed = true;
                this.close();
                return null;
            } catch (ClassNotFoundException c) {
                System.out.printf("%s:Some error in deserialization\n", filename);
                System.exit(1);
            } catch (IOException io) {
                System.out.printf("%s:temporary file reading error\n", filename);
                System.exit(1);
            }
        }

        // Read the next tuple from our already cached page
        peekTuple = inBatch.get(readCursor);
        readCursor++;

        // If reach end of the batch/page, reset the readCursor to prepare to read the next batch
        // However, do not actually read the next batch at this stage
        if (readCursor >= inBatch.size()) {
            readCursor = 0;
            inBatch = null; // deallocate buffer
        }
        return peekTuple;
    }

    // Returns the next tuple and advances the stream
    public Tuple next() {
        // Returns the next tuple, already computed by peek()
        Tuple curTuple = peek();
        // End of file encountered
        if (peek() == null) return null;
        // Advances the stream by setting peekTuple to null
        peekTuple = null;
        // Increases the number of tuples read by 1
        numTuple++;
        return curTuple;
    }

    // Signals the TupleReader to stop reading from the file
    // Buffers and the input file will be closed here
    public boolean close() {
        inBatch = null;        // deallocate buffer
        peekTuple = null;
        completed = true;
        if (in != null) {
            try {
                in.close();
                in = null;
            } catch (IOException io) {
                System.out.printf("%s:reading the temporary file error", filename);
                System.out.println(io);
                return false;
            }
        }
        return true;
    }
}
