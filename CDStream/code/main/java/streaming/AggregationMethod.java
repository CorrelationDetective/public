package streaming;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class AggregationMethod {
    public final String methodName;
    public static AggregationMethod infer(String methodName){
        methodName = methodName.toLowerCase();
        switch (methodName){
            case "avg": return new Avg();
            case "sum": return new Sum();
            case "first": return new First();
            case "last": return new Last();
            case "max": return new Max();
            case "min": return new Min();
            case "count": return new Count();
            default:
                throw new IllegalArgumentException("Unknown aggregation method: " + methodName);
        }
    }
    public String toString(){
        return methodName;
    }

    public abstract double update(double agg, double val, int oldAggCount);
    public double reset(double lastAgg){return lastAgg;}

    private static class Avg extends AggregationMethod {
        public Avg() {
            super("avg");
        }

        @Override
        public double update(double agg, double val, int oldAggCount) {
            if (oldAggCount == 0) return val;
            return agg + (val - agg) / (oldAggCount + 1);
        }
    }

    private static class Sum extends AggregationMethod {
        public Sum() {
            super("sum");
        }
        @Override
        public double update(double agg, double val, int oldAggCount) {
            return agg + val;
        }

        @Override
        public double reset(double lastAgg) {
            return 0;
        }
    }

    private static class Count extends AggregationMethod {
        public Count() {
            super("count");
        }
        @Override
        public double update(double agg, double val, int oldAggCount) {
            return agg + 1;
        }

        @Override
        public double reset(double lastAgg) {
            return 0;
        }
    }

    private static class First extends AggregationMethod {
        public First() {
            super("first");
        }
        @Override
        public double update(double agg, double val, int oldAggCount) {
            if (oldAggCount == 0) return val;
            return agg;
        }
    }

    private static class Last extends AggregationMethod {
        public Last() {
            super("last");
        }
        @Override
        public double update(double agg, double val, int oldAggCount) {
            return val;
        }
    }

    private static class Min extends AggregationMethod {
        public Min() {
            super("min");
        }
        @Override
        public double update(double agg, double val, int oldAggCount) {
            if (oldAggCount == 0) return val;
            return Math.min(agg, val);
        }
    }

    private static class Max extends AggregationMethod {
        public Max() {
            super("max");
        }
        @Override
        public double update(double agg, double val, int oldAggCount) {
            if (oldAggCount == 0) return val;
            return Math.max(agg, val);
        }
    }
}


