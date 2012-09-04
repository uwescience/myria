package edu.washington.escience.myriad.parallel;

import java.io.*;
import java.util.*;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * Query is a wrapper class to manage the execution of queries. It takes a query
 * plan in the form of a high level DbIterator (built by initiating the
 * constructors of query plans) and runs it as a part of a specified
 * transaction.
 * 
 * @author Sam Madden
 */

public class Query implements Serializable {

    private static final long serialVersionUID = 1L;

    transient private Operator op;
    transient private boolean started = false;

    public void setPhysicalPlan(Operator pp) {
        this.op = pp;
    }

    public Operator getPhysicalPlan() {
        return this.op;
    }

    public Query() {
    }

    public Query(Operator root) {
        op = root;
    }

    public void start() throws IOException, DbException{
        op.open();

        started = true;
    }

    public Schema getOutputSchema() {
        return this.op.getSchema();
    }

    /** @return true if there are more tuples remaining. */
    public boolean hasNext() throws DbException{
        return op.hasNext();
    }

    /**
     * Returns the next tuple, or throws NoSuchElementException if the iterator
     * is closed.
     * 
     * @return The next tuple in the iterator
     * @throws DbException
     *             If there is an error in the database system
     * @throws NoSuchElementException
     *             If the iterator has finished iterating
     * @throws TransactionAbortedException
     *             If the transaction is aborted (e.g., due to a deadlock)
     */
    public _TupleBatch next() throws DbException, NoSuchElementException{
        if (!started)
            throw new DbException("Database not started.");

        return op.next();
    }

    /** Close the iterator */
    public void close() throws IOException {
        op.close();
        started = false;
    }

    public void execute() throws IOException, DbException{
        Schema td = this.getOutputSchema();

        String names = "";
        for (int i = 0; i < td.numFields(); i++) {
            names += td.getFieldName(i) + "\t";
        }
        System.out.println(names);
        for (int i = 0; i < names.length() + td.numFields() * 4; i++) {
            System.out.print("-");
        }
        System.out.println("");

        this.start();
        int cnt = 0;
        while (this.hasNext()) {
            _TupleBatch tup = this.next();
            System.out.println(tup);
            cnt+=tup.numOutputTuples();
        }
        System.out.println("\n " + cnt + " rows.");
        this.close();
    }
}
