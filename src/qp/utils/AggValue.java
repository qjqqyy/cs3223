/**
 * Interface: Union type of int and float, used for aggregations.
 * */

package qp.utils;

interface AggValue {
    static AggValue of(Object x) {
        if (x instanceof Integer)
            return new AggInt((Integer) x);
        else if (x instanceof Float)
            return new AggFloat((Float) x);
        return null;
    }
    boolean isInt();
    int intValue();
    float floatValue();

    AggValue minWith(AggValue o);
    AggValue maxWith(AggValue o);
    AggValue sumWith(AggValue o);
}
