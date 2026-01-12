package rs.raf.simpledb.query.operators;

import rs.raf.simpledb.query.Constant;

public class MergeSortJoinScan implements Scan {
    private Scan s1, s2;
    private String fld1, fld2;
    private Constant joinval = null;
    private boolean s1Valid = false;
    private boolean s2Valid = false;
    private boolean hasMatch = false;

    public MergeSortJoinScan(Scan s1, Scan s2, String fld1, String fld2) {
        this.s1 = s2;
        this.s2 = s1;
        this.fld1 = fld2;
        this.fld2 = fld1;
        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        s1.beforeFirst();
        s2.beforeFirst();
        s1Valid = s1.next();
        s2Valid = s2.next();
        hasMatch = false;
    }

    @Override
    public boolean next() {
        // Ako imamo trenutni match, pokušaj naći sledeći na desnoj strani
        if (hasMatch) {
            s2Valid = s2.next();
            hasMatch = false;
        }

        while (s1Valid && s2Valid) {
            int cmp = s1.getVal(fld1).compareTo(s2.getVal(fld2));

            if (cmp == 0) {
                // Match pronađen!
                hasMatch = true;
                return true;
            } else if (cmp < 0) {
                // left.Key < right.Key
                s1Valid = s1.next();
            } else {
                // left.Key > right.Key
                s2Valid = s2.next();
            }
        }
        return false;
    }

    @Override
    public void close() {
        s1.close();
        s2.close();
    }

    @Override
    public Constant getVal(String fldname) {
        if (s1.hasField(fldname))
            return s1.getVal(fldname);
        else
            return s2.getVal(fldname);
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
        return s1.hasField(fldname) || s2.hasField(fldname);
    }

}

