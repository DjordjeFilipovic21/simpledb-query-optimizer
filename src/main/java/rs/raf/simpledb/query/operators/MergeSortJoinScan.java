package rs.raf.simpledb.query.operators;

import rs.raf.simpledb.query.Constant;
import rs.raf.simpledb.query.TempTable;
import rs.raf.simpledb.record.RID;
import rs.raf.simpledb.record.Schema;
import rs.raf.simpledb.tx.Transaction;

public class MergeSortJoinScan implements Scan {
    private Scan s1;
    private UpdateScan s2; // TempTable scan
    private String fld1, fld2;
    private TempTable tempTable;
    private Constant joinval = null;
    private RID markedRid = null;
    private boolean s1Valid = false;
    private boolean s2Valid = false;
    private boolean hasMatch = false;

    public MergeSortJoinScan(Scan s1, UpdateScan s2, String fld1, String fld2) {
        this.s1 = s1;
        this.s2 = s2;
        this.fld1 = fld1;
        this.fld2 = fld2;
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

    private boolean needsS2Advance = false;

    // Impl sa pseudo kodom iz pogledanog videa
    @Override
    public boolean next() {
        do {
            if (markedRid == null) {
                // while (r < s) { advance r }
                while (s1Valid && s2Valid && s1.getVal(fld1).compareTo(s2.getVal(fld2)) < 0) {
                    s1Valid = s1.next();
                }
                // while (r > s) { advance s }
                while (s1Valid && s2Valid && s1.getVal(fld1).compareTo(s2.getVal(fld2)) > 0) {
                    s2Valid = s2.next();
                }

                if (!s1Valid || !s2Valid) {
                    return false;
                }

                // mark start of "block" of s
                markedRid = s2.getRid();
            }

            // if (r == s)
            if (s1Valid && s2Valid && s1.getVal(fld1).equals(s2.getVal(fld2))) {
                // result = <r, s>
                // advance s
//                System.out.println("MATCH: " + s1.getVal(fld1) + " == " + s2.getVal(fld2));
                s2Valid = s2.next();
                // return result
                return true;
            } else {
                // reset s to mark
                if (markedRid != null) {
                    s2.moveToRid(markedRid);
                    s2Valid = true;
                }
                // advance r
                s1Valid = s1.next();
                // mark = NULL
                markedRid = null;
            }
        } while (true);
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

