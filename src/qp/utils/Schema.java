/**
 * schema of the table/result, and is attached to every operator
 **/

package qp.utils;

import java.io.Serializable;
import java.util.ArrayList;

public class Schema implements Serializable {
    ArrayList<Attribute> attset;  // The attributes belong to this schema
    int tupleSize;                // Number of bytes required for this tuple (size of record)

    public Schema(ArrayList<Attribute> colset) {
        attset = new ArrayList<>();
        for (Object o : colset) {
            attset.add((Attribute) o);
        }
    }

    public void setTupleSize(int size) {
        tupleSize = size;
    }

    public int getTupleSize() {
        return tupleSize;
    }

    public int getNumCols() {
        return attset.size();
    }

    public void add(Attribute attr) {
        attset.add(attr);
    }

    public ArrayList<Attribute> getAttList() {
        return attset;
    }

    public Attribute getAttribute(int i) {
        return attset.get(i);
    }

    public int indexOf(Attribute tarattr) {
        for (int i = 0; i < attset.size(); ++i) {
            Attribute attr = attset.get(i);
            if (attr.equals(tarattr)) {
                return i;
            }
        }
        return -1;
    }

    public int typeOf(Attribute tarattr) {
        for (int i = 0; i < attset.size(); ++i) {
            Attribute attr = attset.get(i);
            if (attr.equals(tarattr)) {
                return attr.getType();
            }
        }
        return -1;
    }

    public int typeOf(int attrAt) {
        Attribute attr = attset.get(attrAt);
        return attr.getType();
    }

    /** Checks whether given attribute is present in this Schema or not **/
    public boolean contains(Attribute tarattr) {
        for (int i = 0; i < attset.size(); ++i) {
            Attribute attr = attset.get(i);
            if (attr.equals(tarattr)) {
                return true;
            }
        }
        return false;
    }

    /** The schema of resultant join operation
     Not considered the elimination of duplicate column **/
    public Schema joinWith(Schema right) {
        ArrayList<Attribute> newVector = new ArrayList<>(this.attset);
        newVector.addAll(right.getAttList());
        int newTupleSize = this.getTupleSize() + right.getTupleSize();
        Schema newSchema = new Schema(newVector);
        newSchema.setTupleSize(newTupleSize);
        return newSchema;
    }

    /** To get schema due to result of project operation
     attrlist is the attirbuted that are projected **/
    public Schema subSchema(ArrayList<Attribute> attrlist) {
        ArrayList<Attribute> newVector = new ArrayList<>();
        int newTupleSize = 0;
        for (int i = 0; i < attrlist.size(); ++i) {
            Attribute resAttr = attrlist.get(i);
            int baseIndex = this.indexOf(resAttr.getBaseAttribute());
            Attribute baseAttr = (Attribute) this.getAttribute(baseIndex).clone();
            baseAttr.setAggType(resAttr.getAggType());
            newVector.add(baseAttr);
            if (baseAttr.getAggType() == Attribute.NONE) {
                newTupleSize = newTupleSize + baseAttr.getAttrSize();
            } else {
                newTupleSize = newTupleSize + 4;
            }
        }
        Schema newSchema = new Schema(newVector);
        newSchema.setTupleSize(newTupleSize);
        return newSchema;
    }

    /** Check compatibility for set operations **/
    public boolean checkCompat(Schema right) {
        ArrayList<Attribute> rightattrlist = right.getAttList();
        if (attset.size() != rightattrlist.size()) {
            return false;
        }
        for (int i = 0; i < attset.size(); ++i) {
            if (attset.get(i).getProjectedType() != rightattrlist.get(i).getProjectedType()) {
                return false;
            }
        }
        return true;
    }

    public Object clone() {
        ArrayList<Attribute> newVector = new ArrayList<>();
        for (int i = 0; i < attset.size(); ++i) {
            Attribute newAttribute = (Attribute) (attset.get(i)).clone();
            newVector.add(newAttribute);
        }
        Schema newSchema = new Schema(newVector);
        newSchema.setTupleSize(tupleSize);
        return newSchema;
    }

}
