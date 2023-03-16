package similarities;

import _aux.lists.FastLinkedList;
import streaming.TimeSeries;

public interface NormalizationFunction {
    void normalize(TimeSeries ts);
}

