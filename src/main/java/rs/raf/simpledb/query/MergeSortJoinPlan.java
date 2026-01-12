package rs.raf.simpledb.query;

import rs.raf.simpledb.query.operators.MergeSortJoinScan;
import rs.raf.simpledb.query.operators.Scan;
import rs.raf.simpledb.record.Schema;
import rs.raf.simpledb.tx.Transaction;

import java.util.Arrays;
import java.util.List;

public class MergeSortJoinPlan implements Plan {
    private Plan p1, p2;
    private String fldname1, fldname2;  // Join kolone
    private Transaction tx;

    public MergeSortJoinPlan(Plan p1, Plan p2, String fldname1, String fldname2, Transaction tx) {
        this.p1 = p1;
        this.p2 = p2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.tx = tx;
    }

    public Scan open() {
        // 1. Sortiraj obe strane po JOIN kolonama
        List<String> sortList1 = Arrays.asList(fldname1);
        List<String> sortList2 = Arrays.asList(fldname2);

        Plan sortedP1 = new SortPlan(p1, sortList1, tx);
        Plan sortedP2 = new SortPlan(p2, sortList2, tx);

        // 2. Otvori sortirane skenove
        Scan s1 = sortedP1.open();
        Scan s2 = sortedP2.open();

        // 3. Vrati MergeSortJoinScan (NE SortScan!)
        return new MergeSortJoinScan(s1, s2, fldname1, fldname2);
    }

    @Override
    public int blocksAccessed() {
        return 0;
    }

    @Override
    public int recordsOutput() {
        return 0;
    }

    @Override
    public int distinctValues(String fldname) {
        return 0;
    }

    @Override
    public Schema schema() {
        Schema schema = new Schema();
        schema.addAll(p1.schema());
        schema.addAll(p2.schema());
        return schema;
    }


    @Override
    public void printPlan(int indentLevel) {
        String indent = "  ".repeat(indentLevel);
        System.out.println(indent + "MergeSortJoin on " + fldname1 + "=" + fldname2);
        p1.printPlan(indentLevel + 1);
        p2.printPlan(indentLevel + 1);
    }
}

