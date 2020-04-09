/**
 * Class to facilitate computation of float aggregate values during Projection.
 **/

package qp.utils;

public class AggFloat implements AggValue {
    private final float x;
    public AggFloat(float x) {
        this.x = x;
    }
    public boolean isInt() {
        return false;
    }
    public int intValue() {
        throw new IllegalArgumentException("attempt to project intValue when union is inhibited by float");
    }
    public float floatValue() {
        return x;
    }

    public AggValue minWith(AggValue o) {
        if (o == null || o.isInt())
            return this;
        return new AggFloat(Math.min(floatValue(), o.floatValue()));
    }
    public AggValue maxWith(AggValue o) {
        if (o == null || o.isInt())
            return this;
        return new AggFloat(Math.max(floatValue(), o.floatValue()));
    }
    public AggValue sumWith(AggValue o) {
        if (o == null || o.isInt())
            return this;
        return new AggFloat(floatValue() + o.floatValue());
    }
}
