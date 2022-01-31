package streaming;
import _aux.Pair;
import _aux.Tuple3;
import _aux.lib;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.stream.IntStream;

public class StreamLib {

    public static double getDuration(long start, long stop){
        return (stop - start) / 1000d;
    }

    public static double getBoundFromBoundTuple(Tuple3<Integer, Integer, double[]> boundTuple){
        if (boundTuple.z == null || boundTuple.y  == null){
            return 0d;
        } else{
            return boundTuple.z[boundTuple.y];
        }
    }

    public static double getBin(double start, double end, int nrBins, double value, boolean indexOnLB){
        double step = Math.abs((end - start)) / nrBins;
        double binIdx = indexOnLB ? Math.floor((value - start) / step): Math.ceil((value - start) / step);

        return start + (binIdx * step);
    }

    public static double getMCFromCache(int id1, int id2, int id3, TimeSeries[] stocks){
        TimeSeries s1 = stocks[id1];
        TimeSeries s2 = stocks[id2];
        TimeSeries s3 = stocks[id3];

        return (s1.pairwiseCorrelations[s2.id] + s1.pairwiseCorrelations[s3.id]) / Math.sqrt(2 + 2*s2.pairwiseCorrelations[s3.id]);
    }

    public static int getGroupIndex(int i, int j, int n){
        return (n*(n-1)/2) - (n-i)*((n-i)-1)/2 + j - i - 1;
    }

}
