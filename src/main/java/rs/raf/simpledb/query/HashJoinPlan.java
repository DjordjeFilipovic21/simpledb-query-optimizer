package rs.raf.simpledb.query;

import rs.raf.simpledb.query.operators.HashJoinScan;
import rs.raf.simpledb.query.operators.Scan;
import rs.raf.simpledb.record.Schema;

public class HashJoinPlan implements Plan {
    private Plan p1, p2;
    private String fldname1, fldname2;
    private Schema schema;

    public HashJoinPlan(Plan p1, Plan p2, String fldname1, String fldname2) {
        this.p1 = p1;
        this.p2 = p2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.schema = new Schema();
        schema.addAll(p1.schema());
        schema.addAll(p2.schema());
    }

    @Override
    public Scan open() {
        Scan s1 = p1.open();
        Scan s2 = p2.open();
        return new HashJoinScan(s1, s2, fldname1, fldname2, p1.schema());
    }

    @Override
    public int blocksAccessed() {
        return p1.blocksAccessed() + p2.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        int maxvals = Math.max(p1.distinctValues(fldname1), p2.distinctValues(fldname2));
        return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
    }

    @Override
    public int distinctValues(String fldname) {
        if (p1.schema().hasField(fldname))
            return p1.distinctValues(fldname);
        else
            return p2.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public void printPlan(int level) {
        String indent = " ".repeat(level * 2);
        System.out.println(indent + "HashJoin(" + fldname1 + "=" + fldname2 + ")");
        p1.printPlan(level + 1);
        p2.printPlan(level + 1);
    }
}
