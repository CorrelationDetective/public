

import Jama.Matrix;

import java.util.*;
import java.util.stream.Stream;

public class lib {
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

    public static double getCov(double[] in1, double[] in2) {
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

    public static double getEuclideanFromCorrelation(double corr, int dimLength) {
        return Math.sqrt(2*dimLength*(1-corr));
    }

    public static double[] znorm(double[] z) {
        double sum = Arrays.stream(z).reduce(0, Double::sum);
        double sumSquare = Arrays.stream(z).reduce(0, (a,b) -> a+b*b);
        double avg = sum/z.length;
        double var = sumSquare/z.length - avg*avg;
        var=Math.max(var + 1E-16, -var); // for floating point errors
        double stdev = Math.sqrt(var);
        for (int i=0;i<z.length;i++){
            z[i]=(z[i]-avg)/stdev;
            if(Double.isNaN(z[i])){
                System.out.println("debug: NaN result of znorm");
            }
        }
        return z;
    }

//    public static double[] getColumn(double[][] array, int colID){ // convenience method to get a vector out of the dataset
//        double[] col = new double[array.length];
//        for(int i=0; i<array.length; i++){
//            col[i]=array[i][colID];
//        }
//        return col;
//    }

    public static double get3CorrFrom2Corrs(double corr1, double corr2, double corr3){
        return (corr1 + corr2)/Math.sqrt((2 + 2*corr3));
    }

    public static double get4CorrFrom2Corrs(double corr12, double corr13, double corr14, double corr23, double corr24, double corr34){
        return (corr12 + corr13 + corr14)/Math.sqrt((3 + 2*(corr23+corr24+corr34)));
    }


    public double calculateMultipole(ArrayList<Integer> vectorIDs, double[][] globalPairwiseCorrMatrix){
        int size = vectorIDs.size();
        double[][] corrMat = new double[size][size];
        int v1; int v2;
        for(int i = 0; i< size; i++){
            v1 = vectorIDs.get(i);
            corrMat[i][i] = 1;
            for(int j=i; j<size; j++){
                v2 = vectorIDs.get(j);
                double pair = globalPairwiseCorrMatrix[v1][v2];
                corrMat[i][j] = pair;
                corrMat[j][i] = pair;
            }
        }

        Matrix corrMatMat = new Matrix(corrMat);

        double[] eigs = corrMatMat.eig().getRealEigenvalues();

        double minimal = 1;
        for(double e : eigs){
            if(e<minimal){
                minimal=e;
            }
        }

        double multipoleCorr = 1-minimal;
        return multipoleCorr;


    }

    public static double calculateMultipole(double[][] corrmat){
        Matrix corrmat_jama = new Matrix(corrmat);
        double[] eigenVals = corrmat_jama.eig().getRealEigenvalues();

        double out = Double.MAX_VALUE;
        for(double e : eigenVals){
            if(e < out){
                out = e;
            }
        }
        return 1 - out;
    }

    public static <T> Stream<T> getStream(Collection<T> collection, boolean parallel){
        if(parallel){
            return collection.parallelStream().parallel();
        }else{
            return collection.stream().sequential();
        }
    }



}
