package streaming;
import _aux.Pair;
import _aux.Tuple3;
import _aux.lib;

import java.util.*;

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

    public static Pair<List<int[]>, List<int[]>> symmetricDifference(List<int[]> list1, List<int[]> list2){


        Pair<List<int[]>, List<int[]>> result = new Pair<>(new ArrayList<>(), new ArrayList<>());

        if (list1.size() == 0){
            result.y = list2;
            return result;
        } else if (list2.size() == 0){
            result.x = list1;
            return result;
        }

        Comparator<int[]> arrayComparator = Comparator.<int[]>comparingInt(a -> a[0]).thenComparing(a -> a[1]).thenComparing(a -> a[2]);
//        Sort both lists
        list1.sort(arrayComparator);
        list2.sort(arrayComparator);

        int n = list1.size();
        int m = list2.size();
        int i=0;
        int j=0;

        while (i < n || j < m){
            int[] arr1 = list1.get(Math.min(i, n-1));
            int[] arr2 = list2.get(Math.min(j, m-1));
            if (Arrays.equals(arr1, arr2)){
                i++; j++;
            } else {
                if ((arrayComparator.compare(arr1, arr2) < 0 && i < n) || (j >= m-1 && i < n)){
                    result.x.add(arr1);
                    i++;
                } else {
                    result.y.add(arr2);
                    j++;
                }
            }
        }
        return result;
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
