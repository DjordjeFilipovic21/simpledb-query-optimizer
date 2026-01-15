package rs.raf.simpledb.query;

import rs.raf.simpledb.query.operators.MergeSortJoinScan;
import rs.raf.simpledb.query.operators.Scan;
import rs.raf.simpledb.query.operators.UpdateScan;
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
        List<String> sortList1 = Arrays.asList(fldname1);
        List<String> sortList2 = Arrays.asList(fldname2);

        Plan sortedP1 = new SortPlan(p1, sortList1, tx);
        Plan sortedP2 = new SortPlan(p2, sortList2, tx);

        Schema s2Schema = sortedP2.schema();
        TempTable tempTable = new TempTable(s2Schema, tx);
        UpdateScan s2Temp = (UpdateScan) tempTable.open();

        Scan s2Sorted = sortedP2.open(); // Nov sken
        while (s2Sorted.next()) {
            s2Temp.insert();
            for (String fldname : s2Schema.fields()) {
                s2Temp.setVal(fldname, s2Sorted.getVal(fldname));
            }
        }
        s2Sorted.close();
        s2Temp.beforeFirst();

        Scan s1 = sortedP1.open();

        return new MergeSortJoinScan(s1, s2Temp, fldname1, fldname2);
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

