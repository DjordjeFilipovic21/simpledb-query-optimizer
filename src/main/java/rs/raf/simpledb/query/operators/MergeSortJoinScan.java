
package rs.raf.simpledb.query.operators;
import rs.raf.simpledb.operators.SortScan;
import rs.raf.simpledb.query.Constant;
import rs.raf.simpledb.record.RID;


public class MergeSortJoinScan implements Scan {
    private SortScan s1, s2;
    private String fld1, fld2;
    private boolean s1Valid, s2Valid;
    private Boolean marked;

    public MergeSortJoinScan(SortScan s1, SortScan s2, String fld1, String fld2) {
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
        marked = false;
    }

    @Override
    public boolean next() {
        do {
            if (!marked) {
                // while (l < r) { advance l }
                while (s1Valid && s2Valid && s1.getVal(fld1).compareTo(s2.getVal(fld2)) < 0) {
                    s1Valid = s1.next();
                }
                // while (l > r) { advance r }
                while (s1Valid && s2Valid && s1.getVal(fld1).compareTo(s2.getVal(fld2)) > 0) {
                    s2Valid = s2.next();
                }

                if (!s1Valid || !s2Valid) {
                    return false;
                }
                // mark r
                s2.savePosition();
                marked = true;
            }

            // if (l == r)
            if (s1Valid && s2Valid && s1.getVal(fld1).equals(s2.getVal(fld2))) {
                // result = <l, r>
                // advance r
//                System.out.println("MATCH: " + s1.getVal(fld1) + " == " + s2.getVal(fld2));
                s2Valid = s2.next();
                return true;
            } else {
                // reset r to mark
                if (marked != null) {
                    s2.restorePosition();
                }
                // advance l
                s1Valid = s1.next();
                // mark = null
                marked = false;
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