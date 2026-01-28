package rs.raf.simpledb.query.operators;


import rs.raf.simpledb.query.Constant;
import rs.raf.simpledb.record.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BlockNestedLoopJoinScan implements Scan {
    private Scan s1, s2;
    private Schema sch1, sch2;
    private String fld1, fld2;
    private int blockSize;

    private List<Map<String, Constant>> outerBlock;
    private int outerIndex;
    private boolean innerValid;

    private Map<String, Constant> currentOuterSnapshot;
    private Map<String, Constant> currentInnerSnapshot;

    public BlockNestedLoopJoinScan(Scan s1, Scan s2, String fld1, String fld2,
                                   Schema sch1, Schema sch2, int numBuffers) {
        this.s1 = s1;
        this.s2 = s2;
        this.fld1 = fld1;
        this.fld2 = fld2;
        this.sch1 = sch1;
        this.sch2 = sch2;
        this.blockSize = Math.max(1, (numBuffers - 2) * 100); // heuristic: pages * avg tuples/page ~ 100
        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        s1.beforeFirst();
        s2.beforeFirst();
        outerBlock = new ArrayList<>();
        outerIndex = 0;
        currentOuterSnapshot = null;
        currentInnerSnapshot = null;
        innerValid = false;
        loadNextOuterBlock();
    }

    private void loadNextOuterBlock() {
        outerBlock.clear();
        int count = 0;
        while (count < blockSize && s1.next()) {
            Map<String, Constant> row = new HashMap<>();
            for (String f : sch1.fields()) {
                if (s1.hasField(f)) {
                    row.put(f, s1.getVal(f));
                }
            }
            outerBlock.add(row);
            count++;
        }
        outerIndex = 0;
        s2.beforeFirst();
        innerValid = s2.next();
    }

    private Map<String, Constant> snapshotInner() {
        Map<String, Constant> m = new HashMap<>();
        for (String f : sch2.fields()) {
            if (s2.hasField(f)) {
                m.put(f, s2.getVal(f));
            }
        }
        return m;
    }

    @Override
    public boolean next() {
        while (true) {
            if (outerIndex >= outerBlock.size()) {
                if (s1 == null || !s1.hasField(fld1)) { /* continue */ }
                loadNextOuterBlock();
                if (outerBlock.isEmpty()) {
                    return false;
                }
            }

            while (outerIndex < outerBlock.size()) {
                Map<String, Constant> outerRow = outerBlock.get(outerIndex);

                while (innerValid) {
                    Constant outerKey = outerRow.get(fld1);
                    Constant innerKey = s2.getVal(fld2);
                    if (outerKey != null && outerKey.equals(innerKey)) {
                        currentOuterSnapshot = outerRow;
                        currentInnerSnapshot = snapshotInner();
                        innerValid = s2.next();
                        return true;
                    }
                    innerValid = s2.next();
                }
                outerIndex++;
                s2.beforeFirst();
                innerValid = s2.next();
            }
        }
    }

    @Override
    public void close() {
        s1.close();
        s2.close();
    }

    @Override
    public Constant getVal(String fldname) {
        if (sch1.hasField(fldname)) {
            return currentOuterSnapshot.get(fldname);
        } else {
            return currentInnerSnapshot.get(fldname);
        }
    }

    @Override
    public int getInt(String fldname) {
        if (s1.hasField(fldname))
            return s1.getInt(fldname);
        else
            return s2.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        if (s1.hasField(fldname))
            return s1.getString(fldname);
        else
            return s2.getString(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return sch1.hasField(fldname) || sch2.hasField(fldname);
    }
}

