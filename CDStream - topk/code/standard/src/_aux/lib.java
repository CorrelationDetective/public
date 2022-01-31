package _aux;

import streaming.DCC;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class lib {
    public static String numberFormatter(double f) {
        return String.format("%.2f", f);
    }

    public static double[] mergeArray(double[] arr1, double[] arr2)
    {
        double[] merged = new double[arr1.length+arr2.length];
        System.arraycopy(arr1, 0, merged, 0, arr1.length);
        System.arraycopy(arr2, 0, merged, arr1.length, arr2.length);
        return merged;
    }

    public static void div(double[]in, double den) {
        for (int i=0;i<in.length;i++) in[i]/=den;
    }
    public static double computeAngleOverNormalized(double[]in1, double[]in2) {
        double cosVal = 0;
        for (int i=0;i<in1.length;i++) cosVal+=in1[i]*in2[i];
        cosVal/=in1.length; // the l2 of each vector is sqrt(m), so i need to divide by sqrt(m)*sqrt(m)=m
        double res  = Math.acos(cosVal);
        if (res>=Math.PI)
            res-=Math.PI/2;
        return res;
    }
    public static double euclidean(double[] in1, double[] in2) {
        double d = 0;
        for (int i=0;i<in1.length;i++) {
            double dd = in1[i]-in2[i];
            d+=(dd*dd);
        }
        return Math.sqrt(d);
    }
    public static double l2(double[] in1) {
        double d = 0;
        for (int i=0;i<in1.length;i++) {
            double dd = in1[i];
            d+=(dd*dd);
        }
        return Math.sqrt(d);
    }
    public static double l1(double[] in1, double[] in2) {
        double d = 0;
        for (int i=0;i<in1.length;i++) {
            double dd = Math.abs(in1[i]-in2[i]);
            d+=dd;
        }
        return d;
    }
    public static double l1Max(double[] in1, double[] in2) {
        double d = 0;
        for (int i=0;i<in1.length;i++) {
            d = Math.max(d,Math.abs(in1[i]-in2[i]));
        }
        return d;
    }

    public static double angle(double[] in1, double[] in2){
        double frac = lib.dot(in1, in2) / (lib.l2(in1)*lib.l2(in2));
        frac = Math.min(frac, 1);
        frac = Math.max(frac, -1);
        return Math.acos(frac);
    }

    public static double angleFromCorr(double corr){
        return Math.acos(corr);
    }

    public static double cov(double[] in1, double[] in2) {
        double cov=0;
        for (int i=0;i<in1.length;i++) {
            cov+=in1[i]*in2[i];
        }
        cov = cov/in1.length;
        return cov;
    }
    public static double[] add(double[] in1, double[] in2) {
        double[] ret = in1.clone();
        for (int i=0;i<in2.length;i++)
            ret[i]+=in2[i];
        return ret;
    }

    public static double[] subtract(double[] in1, double[] in2) {
        double[] ret = in1.clone();
        for (int i=0;i<in2.length;i++)
            ret[i]-=in2[i];
        return ret;
    }

    public static double[] avg(double[] in1, double[] in2) {
        double[] ret = new double[in1.length];
        for (int i=0;i<in2.length;i++)
            ret[i] = (in1[i] + in2[i]) / 2;
        return ret;
    }

    public static double dot(double[] in1, double[] in2) {
        double ans = 0d;
        for (int i=0;i<in1.length;i++)
            ans += in1[i]*in2[i];
        return ans;
    }

    public static double pearsonWithAlreadyNormedVectors(double[] in1, double[] in2) {
        double corr=0;
        for (int i=0;i<in1.length;i++) {
            corr+=in1[i]*in2[i];
        }
        return corr/in1.length;
    }

    static void computeMeanStdev(double[] z) {
        double sum = Arrays.stream(z).reduce(0, Double::sum);
        double sumSquare = Arrays.stream(z).reduce(0, (a,b) -> a+b*b);
        double avg = sum/z.length;
        double var = sumSquare/z.length + avg*avg;
        double stdev = Math.sqrt(var);
        System.err.println(avg  + " " + stdev);
    }
    public static double getCorrelationFromEuclidean(double euclidean, int dimLength) {
        return 1d- euclidean*euclidean/(2d*dimLength);
    }

    public static double corrToDist(double corr, int m) {
        return Math.sqrt(2*m*(1 - corr));
    }

    public static double std(double[] z) {
        double sum = Arrays.stream(z).reduce(0, Double::sum);
        double sumSquare = Arrays.stream(z).reduce(0, (a,b) -> a+b*b);
        double avg = sum/z.length;
        double var = sumSquare/z.length - avg*avg;
        var=Math.max(var + 1E-16, -var); // for floating point errors
        double stdev = Math.sqrt(var);
        return stdev;
    }

    public static double[] znorm(double[] z) {
        double[] z_new = z.clone();
        double sum = Arrays.stream(z).reduce(0, Double::sum);
        double avg = sum/z.length;
        double stdev = lib.std(z);
        for (int i=0;i<z.length;i++){
            z_new[i]=(z_new[i]-avg)/stdev;
            if(Double.isNaN(z_new[i])){
                System.out.println("debug: NaN result of znorm");
            }
        }
        return z_new;
    }

    public static <T> Stream<T> getStream(Collection<T> collection, boolean parallel){
        if(parallel){
            return collection.parallelStream().parallel();
        }else{
            return collection.stream().sequential();
        }
    }

    public static <T> Stream<T> getStream(T[] collection, boolean parallel){
        if(parallel){
            return Arrays.stream(collection).parallel();
        }else{
            return Arrays.stream(collection).sequential();
        }
    }

    public static double[] repeatDouble(double[] arr, int newLength) {
        double[] dup = Arrays.copyOf(arr, newLength);
        for (int last = arr.length; last != 0 && last < newLength; last <<= 1) {
            System.arraycopy(dup, 0, dup, last, Math.min(last << 1, newLength) - last);
        }
        return dup;
    }

    public static void insertionSort(List<DCC> list, Comparator<DCC> comparator){
        int n = list.size();

        for (int j = 1; j < n; j++){
            DCC key = list.get(j);
            int i = j-1;
            while ((i > -1) && comparator.compare(list.get(i), key) > 0){
                list.set(i+1, list.get(i));
                i--;
            }

            list.set(i+1, key);
        }
    }

    public static Map<Boolean, List<DCC>> insertionSortAndPartition(List<DCC> list, Comparator<DCC> comparator,
                                                                    Predicate<DCC> partitionPredicate){
        int n = list.size();
        List<DCC> falseList = new ArrayList<>(n);
//        Copy of input list
        List<DCC> trueList = new ArrayList<>(list);

        int j = 1;
        while (j < n){
            DCC key = trueList.get(j);
            if (!partitionPredicate.test(key)){
                falseList.add(key);
                trueList.remove(j);
                n--;
                continue;
            }
            int i = j-1;
            while ((i > -1) && comparator.compare(trueList.get(i), key) > 0){
                trueList.set(i+1, trueList.get(i));
                i--;
            }

            trueList.set(i+1, key);
            j++;
        }

        Map<Boolean, List<DCC>> returnMap = new HashMap<>();
        returnMap.put(true, trueList);
        returnMap.put(false, falseList);
        return returnMap;
    }


    public static double round(double y, int decimals){
        return Math.round(y * Math.pow(10, decimals)) / Math.pow(10, decimals);
    }

    public static double[][] transpose(double[][] matrix) {
        double[][] t = new double[matrix[0].length][matrix.length];
        for (int i=0;i<matrix.length;i++)
            for (int j=0;j<matrix[0].length;j++)
                t[j][i]=matrix[i][j];
        return t;
    }

//    public static double[] getColumn(double[][] array, int colID){ // convenience method to get a vector out of the dataset
//        double[] col = new double[array.length];
//        for(int i=0; i<array.length; i++){
//            col[i]=array[i][colID];
//        }
//        return col;
//    }


}
