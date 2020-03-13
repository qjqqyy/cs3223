/**
 * Attribute or column meta data
 **/

package qp.utils;

import java.io.Serializable;

public class Attribute implements Serializable {

    public static final int INT = 1;
    public static final int STRING = 2;
    public static final int REAL = 3;

    public static final int PK = 1;  // Primary key
    public static final int FK = 2;  // Foreign key

    public static final int NONE = 0;
    public static final int MAX = 1;
    public static final int MIN = 2;
    public static final int SUM = 3;
    public static final int COUNT = 4;
    public static final int AVG = 5;

    String tblname;   // Table to which this attribute belongs
    String colname;   // Name of the attribute
    int type;         // Type of attribute
    int aggtype = 0;  // Aggregate type of attribute
    int key = -1;     // Type of key
    int attrsize;     // Number of bytes for this attribute

    public Attribute(String tbl, String col) {
        tblname = tbl;
        colname = col;
    }

    public Attribute(String tbl, String col, int typ) {
        tblname = tbl;
        colname = col;
        type = typ;
    }

    public Attribute(String tbl, String col, int typ, int keytype) {
        tblname = tbl;
        colname = col;
        type = typ;
        key = keytype;
    }

    public Attribute(String tbl, String col, int typ, int keytype, int size) {
        tblname = tbl;
        colname = col;
        type = typ;
        key = keytype;
        attrsize = size;
    }

    public void setAttrSize(int size) {
        attrsize = size;
    }

    public int getAttrSize() {
        return attrsize;
    }

    public void setKeyType(int kt) {
        key = kt;
    }

    public int getKeyType() {
        return key;
    }

    public boolean isPrimaryKey() {
        if (key == PK)
            return true;
        else
            return false;
    }

    public boolean isForeignKey() {
        if (key == FK)
            return true;
        else
            return false;
    }


    public void setTabName(String tab) {
        tblname = tab;
    }

    public String getTabName() {
        return tblname;
    }

    public void setColName(String col) {
        colname = col;
    }

    public String getColName() {
        return colname;
    }

    public void setType(int typ) {
        type = typ;
    }

    public int getType() {
        return type;
    }

    public int getProjectedType() {
        if (aggtype == Attribute.COUNT) {
            return Attribute.INT;
        } else if (aggtype == Attribute.AVG) {
            return Attribute.REAL;
        } else {
            return type;
        }
    }

    public void setAggType(int at) {
        aggtype = at;
    }

    public Attribute getBaseAttribute() {
        Attribute base = (Attribute) clone();
        base.setAggType(Attribute.NONE);
        return base;
    }

    public int getAggType() {
        return aggtype;
    }

    public String toString() {
        String baseAttributeName = getTabName() + "." + getColName();
        if (getAggType() == Attribute.NONE) {
            return baseAttributeName;
        } else if (getAggType() == Attribute.MAX) {
            return "MAX(" + baseAttributeName + ")";
        } else if (getAggType() == Attribute.MIN) {
            return "MIN(" + baseAttributeName + ")";
        } else if (getAggType() == Attribute.SUM) {
            return "SUM(" + baseAttributeName + ")";
        } else if (getAggType() == Attribute.COUNT) {
            return "COUNT(" + baseAttributeName + ")";
        } else if (getAggType() == Attribute.AVG) {
            return "AVG(" + baseAttributeName + ")";
        } else return baseAttributeName;
    }

    public boolean equals(Attribute attr) {
        return this.toString().equals(attr.toString());
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Attribute) return this.equals((Attribute) other);
        else return false;
    }

    public Object clone() {
        String newtbl = tblname;
        String newcol = colname;
        Attribute newattr = new Attribute(newtbl, newcol);
        newattr.setType(type);
        newattr.setAggType(aggtype);
        newattr.setKeyType(key);
        newattr.setAttrSize(attrsize);
        return newattr;
    }

}
