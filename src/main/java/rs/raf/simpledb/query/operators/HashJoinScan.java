package rs.raf.simpledb.query.operators;

import rs.raf.simpledb.query.Constant;
import rs.raf.simpledb.record.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashJoinScan implements Scan {
    private Scan s1, s2;
    private String fldname1, fldname2;
    private Map<Constant, List<SavedRecord>> hashTable;
    private List<SavedRecord> currentBucket;
    private int currentBucketIndex;
    private Schema schema1;

    public HashJoinScan(Scan s1, Scan s2, String fldname1, String fldname2, Schema schema1) {
        this.s1 = s1;
        this.s2 = s2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.schema1 = schema1;
        this.hashTable = new HashMap<>();
        this.currentBucket = null;
        this.currentBucketIndex = -1;
        buildHashTable();
    }

    private void buildHashTable() {
        while (s1.next()) {
            Constant joinVal = s1.getVal(fldname1);
            SavedRecord record = new SavedRecord(s1, (List<String>) schema1.fields());
            hashTable.computeIfAbsent(joinVal, k -> new ArrayList<>()).add(record);
        }
    }

    @Override
    public void beforeFirst() {
        s2.beforeFirst();
        currentBucket = null;
        currentBucketIndex = -1;
    }

    @Override
    public boolean next() {
        while (true) {
            if (currentBucket != null && currentBucketIndex < currentBucket.size() - 1) {
                currentBucketIndex++;
                return true;
            }

            if (!s2.next()) {
                return false;
            }

            Constant joinVal = s2.getVal(fldname2);
            currentBucket = hashTable.get(joinVal);

            if (currentBucket != null && !currentBucket.isEmpty()) {
                currentBucketIndex = 0;
                return true;
            }
        }
    }

    @Override
    public int getInt(String fldname) {
        if (s2.hasField(fldname))
            return s2.getInt(fldname);
        else
            return currentBucket.get(currentBucketIndex).getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        if (s2.hasField(fldname))
            return s2.getString(fldname);
        else
            return currentBucket.get(currentBucketIndex).getString(fldname);
    }

    @Override
    public Constant getVal(String fldname) {
        if (s2.hasField(fldname))
            return s2.getVal(fldname);
        else
            return currentBucket.get(currentBucketIndex).getVal(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return s1.hasField(fldname) || s2.hasField(fldname);
    }

    @Override
    public void close() {
        s1.close();
        s2.close();
    }


    private static class SavedRecord {
        private Map<String, Constant> vals = new HashMap<>();

        public SavedRecord(Scan s, List<String> fldnames) {
            for (String fldname : fldnames) {
                vals.put(fldname, s.getVal(fldname));
            }
        }

        public Constant getVal(String fldname) {
            return vals.get(fldname);
        }

        public int getInt(String fldname) {
            return (Integer) vals.get(fldname).asJavaVal();
        }

        public String getString(String fldname) {
            return (String) vals.get(fldname).asJavaVal();
        }
    }
}
