/**
 * Class to facilitate computation of aggregates during Projection.
 **/


package qp.utils;

import java.util.HashMap;
import java.util.Map;

public class AggrProps {
    // what attributes to track
    private final Map<Integer, boolean[]> m = new HashMap<>();
    // intermediate values go here
    private final Map<Integer, AggValue[]> ivs = new HashMap<>();

    public void markAggregate(int index, int aggType) {
        if (!m.containsKey(index)) {
            m.put(index, new boolean[5]);
            ivs.put(index, new AggValue[5]);
        }
        if (aggType == Attribute.AVG) {
            m.get(index)[Attribute.SUM] = true;
            m.get(index)[Attribute.COUNT] = true;
        } else {
            m.get(index)[aggType] = true;
        }
    }

    public void consume(Batch inbatch) {
        for (Tuple tuple: inbatch.tuples) {
            for (int index: ivs.keySet()) {
                boolean[] aggregators = m.get(index);
                AggValue[] vs = ivs.get(index);
                Object v = tuple.dataAt(index);
                if (v == null)
                    continue;
                if (aggregators[Attribute.MAX])
                    vs[Attribute.MAX] = AggValue.of(v).maxWith(vs[Attribute.MAX]);
                if (aggregators[Attribute.MIN])
                    vs[Attribute.MIN] = AggValue.of(v).minWith(vs[Attribute.MIN]);
                if (aggregators[Attribute.SUM])
                    vs[Attribute.SUM] = AggValue.of(v).sumWith(vs[Attribute.SUM]);
                if (aggregators[Attribute.COUNT])
                    vs[Attribute.COUNT] = AggValue.of(1).sumWith(vs[Attribute.COUNT]);
            }
        }
    }

    public Object collect(int index, int aggType) {
        AggValue[] vs = ivs.get(index);
        if (aggType == Attribute.AVG) {
            return (vs[Attribute.SUM].isInt() ? (float)vs[Attribute.SUM].intValue() : vs[Attribute.SUM].floatValue())
                    / vs[Attribute.COUNT].intValue();
        }
        AggValue av = vs[aggType];
        if (av.isInt())
            return av.intValue();
        else
            return av.floatValue();
    }
}
