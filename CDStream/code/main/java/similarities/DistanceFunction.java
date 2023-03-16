package similarities;

import streaming.TimeSeries;

public interface DistanceFunction {
    double dist(TimeSeries x, TimeSeries y);
}
