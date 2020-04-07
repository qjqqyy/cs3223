package qp.utils;

public class AggInt implements AggValue {
    private final int x;
    public AggInt(int x) {
        this.x = x;
    }
    public boolean isInt() {
        return true;
    }
    public int intValue() {
        return x;
    }
    public float floatValue() {
        throw new IllegalArgumentException("attempt to project floatValue when union is inhibited by int");
    }

    public AggValue minWith(AggValue o) {
        if (o == null || !o.isInt())
            return this;
        return new AggInt(Math.min(intValue(), o.intValue()));
    }
    public AggValue maxWith(AggValue o) {
        if (o == null || !o.isInt())
            return this;
        return new AggInt(Math.max(intValue(), o.intValue()));
    }
    public AggValue sumWith(AggValue o) {
        if (o == null || !o.isInt())
            return this;
        return new AggInt(intValue() + o.intValue());
    }
}
