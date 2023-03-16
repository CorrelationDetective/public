package _aux;

import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import org.apache.commons.math3.exception.DimensionMismatchException;
import queries.ResultTuple;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class lib {
    public static double[][] transpose(double[][] matrix) {
        double[][] t = new double[matrix[0].length][matrix.length];
        for (int i=0;i<matrix.length;i++)
            for (int j=0;j<matrix[0].length;j++)
                t[j][i]=matrix[i][j];
        return t;
    }

    public static double log2(double x) {
        return Math.log(x)/Math.log(2);
    }

    public static double[] add(double[] in1, double[] in2) {
        double[] res = new double[in1.length];
        for (int i=0;i<in1.length;i++) res[i]=in1[i]+in2[i];
        return res;
    }

    public static double avg(double[] z){
        return Arrays.stream(z).reduce(0, Double::sum)/z.length;
    }
    public static double var(double[] z){
        double sum = Arrays.stream(z).reduce(0, Double::sum);
        double sumSquare = Arrays.stream(z).reduce(0, (a,b) -> a+b*b);
        double avg = sum/z.length;
        return sumSquare/z.length - avg*avg;
    }

    public static double std(double [] z){return Math.sqrt(var(z));}

    public static double min(double[] z){return Arrays.stream(z).min().getAsDouble();}
    public static double max(double[] z){return Arrays.stream(z).max().getAsDouble();}

    public static double[] sub(double[] in1, double[] in2) {
        double[] res = new double[in1.length];
        for (int i=0;i<in1.length;i++) res[i]=in1[i]-in2[i];
        return res;
    }

    //    Multiply by scalar
    public static double[] scale(double[] in1, double in2) {
        double[] res = new double[in1.length];
        for (int i=0;i<in1.length;i++) res[i]=in1[i]*in2;
        return res;
    }

    public static double[] sadd(double[] in1, double in2) {
        double[] res = new double[in1.length];
        for (int i=0;i<in1.length;i++) res[i]=in1[i]+in2;
        return res;
    }

//    Get element-wise mean of list of vectors
    public static double[] elementwiseAvg(FastArrayList<double[]> in) {
        double[] res = new double[in.get(0).length];
//        Iterate over all vectors
        for (int i=0;i<in.size();i++) {
//            Add every element in vector to the result
            for (int j=0;j<in.get(0).length;j++) {
                res[j]+=in.get(i)[j];
            }
        }
//        Divide by number of vectors
        for (int j=0;j<in.get(0).length;j++) {
            res[j]/=in.size();
        }
        return res;
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

    public static double[] rank(double[] in) {
        Integer[] indexes = new Integer[in.length];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = i;
        }
        Arrays.sort(indexes, new Comparator<Integer>() {
            @Override
            public int compare(final Integer i1, final Integer i2) {
                return Double.compare(in[i1], in[i2]);
            }
        });
        return IntStream.range(0, indexes.length).mapToDouble(i -> indexes[i]).toArray();
    }

    public static double dot(double[] in1, double[] in2) {
        double d = 0;
        for (int i=0;i<in1.length;i++) {
            double dd = in1[i]*in2[i];
            d+=dd;
        }
        return d;
    }

    public static double angle(double[] in1, double[] in2) {
        return Math.acos(Math.min(Math.max(lib.dot(in1, in2), -1),1));
    }

    public static double euclidean(double[] in1, double[] in2) {
        double d = 0;
        for (int i=0;i<in1.length;i++) {
            double dd = in1[i]-in2[i];
            d+=(dd*dd);
        }
        return Math.sqrt(d);
    }

    public static double euclideanSquared(double[] in1, double[] in2) {
        double d = euclidean(in1, in2);
        return d*d;
    }

    public static double minkowski(double[] in1, double[] in2, double p) {
        double d = 0;
        for (int i=0;i<in1.length;i++) {
            double dd = Math.pow(Math.abs(in1[i]-in2[i]), p);
            d+=dd;
        }
        return Math.pow(d, 1/p);
    }

    public static double chebyshev(double[] in1, double[] in2) {
        double d = 0;
        for (int i=0;i<in1.length;i++) {
            double dd = Math.abs(in1[i]-in2[i]);
            if (dd>d) d=dd;
        }
        return d;
    }

    public static double[] mmul(double[] v, double[][] M) throws DimensionMismatchException {
        if (v.length != M[0].length){throw new DimensionMismatchException(v.length, M[0].length);}

        int m = M.length;
        double[] out = new double[m];

        for (int i = 0; i < m; i++) {
            out[i] = lib.dot(v,M[i]);
        }
        return out;
    }

    public static double[] znorm(double[] v) {
        double[] z = v.clone();
        double sum = 0;
        double sumSquare = 0;
        for (int i=0;i<z.length;i++) {
            sum+=z[i];
            sumSquare+=z[i]*z[i];
        }
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

    public static double[][] znorm(double[][] Z) {
        for (int i=0;i<Z.length;i++) {
            Z[i]=znorm(Z[i]);
        }
        return Z;
    }

    //    zero-sum and l2-normalize a vector
    public static double[] l2norm(double[] v){
        double[] z = v.clone();
        double sum = Arrays.stream(z).reduce(0, Double::sum);
        double avg = sum / v.length;
        z = lib.sadd(v,-1*avg);
        double sumSquare = Arrays.stream(z).reduce(0, (a,b) -> a+b*b);
        double norm = Math.sqrt(sumSquare);
        z = lib.scale(z,1/norm);
        return z;
    }

    public static double[][] l2norm(double[][] Z) {
        for (int i=0;i<Z.length;i++) {
            Z[i]= l2norm(Z[i]);
        }
        return Z;
    }

    public static <T> Stream<T> getStream(Collection<T> collection, boolean parallel){
        if(parallel){
            return collection.parallelStream().parallel();
        }else{
            return collection.stream().sequential();
        }
    }

    public static <T> Stream<T> getStream(Stream<T> stream, boolean parallel){
        return parallel ? stream.parallel(): stream.sequential();
    }

    public static IntStream getStream(BitSet bitSet, boolean parallel){
        return parallel ? bitSet.stream().parallel(): bitSet.stream().sequential();
    }

    public static <T> Stream<T> getStream(T[] array, boolean parallel){
        if(parallel){
            return Arrays.stream(array).parallel();
        }else{
            return Arrays.stream(array).sequential();
        }
    }

    public static double nanoToSec(long nano){return nano/1E9;}

    public static void printBar(Logger logger){
        logger.fine("-------------------------------------");
    }

    public static FastLinkedList<String> readCSV(String filename){
        FastLinkedList<String> lines = new FastLinkedList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }

    public static double[][] readMatrix(String filename){
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            String line;
            FastLinkedList<double[]> matrix = new FastLinkedList<>();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                double[] row = new double[values.length];
                for (int i = 0; i < values.length; i++) {
                    row[i] = Double.parseDouble(values[i]);
                }
                matrix.add(row);
            }
            return matrix.toArray(new double[matrix.size()][]);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void writeMatrix(String filename, double[][] matrix) {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(filename));
            for (int i = 0; i < matrix.length; i++) {
                for (int j = 0; j < matrix[i].length; j++) {
                    bw.write(matrix[i][j] + ",");
                }
                bw.write("\n");
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeMatrix(String filename, FastArrayList<double[]> matrix) {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(filename));
            for (int i = 0; i < matrix.size(); i++) {
                for (int j = 0; j < matrix.get(i).length; j++) {
                    bw.write(matrix.get(i)[j] + ",");
                }
                bw.write("\n");
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static FastArrayList<ResultTuple> getResultsFromFile(String filename) {
        FastLinkedList<ResultTuple> results = new FastLinkedList<>();
        FastLinkedList<String> lines = lib.readCSV(filename);

//        Skip header
        lines.poll();

        while(!lines.isEmpty()){
            String[] split = lines.poll().split(",");
            FastArrayList<Integer> LHS = new FastArrayList<>(Arrays.stream(split[0].split("-")).map(Integer::parseInt).collect(Collectors.toList()));

            FastArrayList<Integer> RHS = new FastArrayList<>(split[1].length());
            if (split[1].length() > 0) {
                RHS = new FastArrayList<>(Arrays.stream(split[1].split("-")).map(Integer::parseInt).collect(Collectors.toList()));
            }
            double sim = Double.parseDouble(split[4]);

            Long timestamp = 0L;
            if (split.length > 5 && !split[5].equals("null")) {
                timestamp = Long.parseLong(split[5]);
            }
            results.add(new ResultTuple(LHS,RHS, new FastArrayList<>(0), new FastArrayList<>(0), sim, timestamp));
        }
        return new FastArrayList<>(results);
    }
}
