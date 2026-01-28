package rs.raf.simpledb.query;


import rs.raf.simpledb.query.Plan;
import rs.raf.simpledb.query.operators.BlockNestedLoopJoinScan;
import rs.raf.simpledb.query.operators.Scan;
import rs.raf.simpledb.record.Schema;

public class BlockNestedLoopJoinPlan implements Plan {
    private Plan p1, p2;
    private String fld1, fld2;
    private int numBuffers;
    private Schema schema;

    public BlockNestedLoopJoinPlan(Plan p1, Plan p2, String fld1, String fld2, int numBuffers) {
        this.p1 = p1;
        this.p2 = p2;
        this.fld1 = fld1;
        this.fld2 = fld2;
        this.numBuffers = Math.max(3, numBuffers); // need at least 3 for BNL
        this.schema = new Schema();
        for (String f : p1.schema().fields())
            this.schema.add(f, p1.schema());
        for (String f : p2.schema().fields())
            this.schema.add(f, p2.schema());
    }

    @Override
    public Scan open() {
        return new BlockNestedLoopJoinScan(p1.open(), p2.open(), fld1, fld2, p1.schema(), p2.schema(), numBuffers);
    }

    @Override
    public int blocksAccessed() {
        int b1 = p1.blocksAccessed();
        int b2 = p2.blocksAccessed();
        int usable = Math.max(1, numBuffers - 2);
        int outerPasses = (int) Math.ceil((double) b1 / usable);
        return b1 + outerPasses * b2;
    }

    @Override
    public int recordsOutput() {
        int r1 = p1.recordsOutput();
        int r2 = p2.recordsOutput();
        int d1 = p1.distinctValues(fld1);
        int d2 = p2.distinctValues(fld2);
        int maxd = Math.max(1, Math.max(d1, d2));
        return (r1 * r2) / maxd;
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
    public void printPlan(int indentLevel) {
        System.out.println("-".repeat(indentLevel) + "-> BLOCK NESTED LOOP JOIN OF -> ");
        p1.printPlan(indentLevel + 3);
        p2.printPlan(indentLevel + 3);
    }
}
