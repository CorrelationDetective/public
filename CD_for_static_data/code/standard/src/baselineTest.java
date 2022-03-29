import Jama.Matrix;
import _aux.lib;

import java.awt.*;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.IntStream;

import static _aux.lib.znorm;
import static data.readFMRI.readfmri;
import static data.readFMRI.transpose;



public class baselineTest {
    int n_vec;


    public static void main(String[] args) throws Exception {
        ArrayList<String> header = new ArrayList<>();
        int desired_stocks = 400;
        double tau = 0.4;
        double minJump = 0.1;
        double[][] fmri = SimpleTest.loadData("../SLPMultipolesPreprocessed.csv", desired_stocks, 200, header);
        int pmax = 4;


        if(args.length > 0){
            fmri = SimpleTest.loadData(args[0], 1000, 200, header);
            tau = Double.parseDouble(args[1]);
            minJump = Double.parseDouble(args[2]);
            pmax = Integer.parseInt(args[3]);
        }else{
            System.out.println("using some default test arguments that may not be interesting...");
        }




        int n_dim = fmri[0].length;
        int n_stocks = fmri.length;

        for (int i = 0; i < fmri.length; i++) fmri[i] = znorm(fmri[i]);

        calcAllMultipoles(fmri, header, tau, minJump, pmax);

//        calcAllQuadpoles(fmri, header, tau, minJump);
    }


    public static double[][] fillPairwise(double[][] data) {
        double[][] pairwise = new double[data.length][data.length];
        for (int i = 0; i < pairwise.length; i++) {
            pairwise[i][i] = 1;
            for (int j = i + 1; j < pairwise.length; j++) {
                pairwise[i][j] = lib.pearsonWithAlreadyNormedVectors(data[i], data[j]);
                pairwise[j][i] = pairwise[i][j];
            }
        }
        return pairwise;
    }


    public static void calcAllMultipoles(double[][] data, ArrayList<String> header, double tau, double minJump, int pmax) {

        long start = System.currentTimeMillis();
        System.out.println("start at " + LocalTime.now());
        System.out.println("nvec: " + data.length + ", ndim: " + data[0].length);







        double[][] pairwise = baselineTest.fillPairwise(data);

        int n_positive = IntStream.range(0, pairwise.length-pmax).parallel().mapToObj(i -> {
            ArrayList<ArrayList<Integer>> out = new ArrayList<>(); // just to keep track of the length
            //            System.out.println((double)i/pairwise.length);
            ArrayList<Integer> vecIDs = new ArrayList<>();
            for(int pos = 0; pos<pmax; pos++){
                vecIDs.add(i + pos);
            }

            while(vecIDs != null){
                double[][] smallPairwise = new double[pmax][pmax];
                for(int pos = 0; pos< pmax; pos++){
                    smallPairwise[pos][pos] = 1;
                    for(int pos2 = pos+1; pos2<pmax; pos2++){
                        smallPairwise[pos][pos2] = pairwise[vecIDs.get(pos)][vecIDs.get(pos2)];
                        smallPairwise[pos2][pos] = smallPairwise[pos][pos2];
                    }
                }
                double corr = calculateMultipole(smallPairwise);
                double maxsubsetcorr = calcMaxSubsetMultipole(smallPairwise);

                if(corr >= tau && corr - maxsubsetcorr >= minJump){
                    out.add(vecIDs);
                }
                vecIDs = nextComb(vecIDs, data.length);
            }

            System.out.print(".");
            return out;

        })
                .mapToInt(Collection::size)
                .sum();


        long stop = System.currentTimeMillis();
        System.out.println();

        System.out.println("time in seconds:" + (stop - start) / 1000);
        System.out.println("positive:" + n_positive);
    }

    private static ArrayList<Integer> nextComb(ArrayList<Integer> comb, int n_vec){
        ArrayList<Integer> newComb = new ArrayList<>();

        int idToIncrease = comb.size()-1;
        while(comb.get(idToIncrease) >= n_vec-(comb.size() - idToIncrease)){
            idToIncrease--;
            if(idToIncrease<1){
                return null;
            }
        }

        for(int i=0; i< idToIncrease; i++){
            newComb.add(comb.get(i));
        }

        newComb.add(comb.get(idToIncrease) + 1);

        for(int i = idToIncrease+1; i<comb.size(); i++){
            newComb.add(comb.get(i-1) + 1);
            if(newComb.get(i) > n_vec-1){
                return null;
            }
        }


        return newComb;

    }

    private static double calcMaxSubsetMultipole(double[][] smallPairwise) {

        double max = 0;

        for (int i = 0; i < smallPairwise.length; i++) {
            double[][] subsetPairwise = new double[smallPairwise.length - 1][smallPairwise.length - 1];

            int i1 = 0;
            int i2 = 0;
            for (int j = 0; j < smallPairwise.length; j++) {
                if (j == i) continue;
                i2 = 0;
                for (int k = 0; k < smallPairwise.length; k++) {
                    if (k == i) {
                        continue;
                    }
                    subsetPairwise[i1][i2] = smallPairwise[j][k];

                    i2++;
                }
                i1++;
            }
            double subsetCorr = calculateMultipole(subsetPairwise);

            max = Math.max(max, subsetCorr);

        }

        return max;
    }


    public static double calculateMultipole(double[][] corrmat) {
        Matrix corrmat_jama = new Matrix(corrmat);
        double[] eigenVals = corrmat_jama.eig().getRealEigenvalues();

        double out = Double.MAX_VALUE;
        for (double e : eigenVals) {
            if (e < out) {
                out = e;
            }
        }
        return 1 - out;
    }


    public static void calcAllQuadpoles(double[][] data, ArrayList<String> header, double tau, double minJump) {


        long start = System.currentTimeMillis();


        System.out.println("start at " + LocalTime.now());
        System.out.println("nvec: " + data.length + ", ndim: " + data[0].length);


        double[][] pairwise = new double[data.length][data.length];
        IntStream.range(0, pairwise.length).parallel().forEach(i -> {
            pairwise[i][i] = 1;
            for (int j = i + 1; j < data.length; j++) {
                pairwise[i][j] = lib.pearsonWithAlreadyNormedVectors(data[i], data[j]);
                pairwise[j][i] = pairwise[i][j];
            }
        });

        long n_positive = IntStream.range(0, pairwise.length).parallel().mapToObj(i1 -> {
                    ArrayList<Boolean> out = new ArrayList<>(); // just to keep track of the length
            for (int i2 = 0; i2 < data.length; i2++) {
//                if (i2 == i1) continue;
                for (int i3 = i2 + 1; i3 < data.length; i3++) {
//                    if (i3 == i1) continue;
//                    for (int i4 = i3 + 1; i4 < data.length; i4++) {
//                        if (i4 == i1) continue;
                        double pairwise12 = pairwise[i1][i2];
                        double pairwise13 = pairwise[i1][i3];
//                        double pairwise14 = pairwise[i1][i4];
                        double pairwise23 = pairwise[i2][i3];
//                        double pairwise24 = pairwise[i2][i4];
//                        double pairwise34 = pairwise[i3][i4];

//                        double corr = lib.get4CorrFrom2Corrs(pairwise12, pairwise13, pairwise14, pairwise23, pairwise24, pairwise34);

                        double corr = lib.get3CorrFrom2Corrs(pairwise12, pairwise13, pairwise23);
//
//
//                        max3corr = Math.max(max3corr, lib.get3CorrFrom2Corrs(pairwise12, pairwise14, pairwise24));
//                        max3corr = Math.max(max3corr, lib.get3CorrFrom2Corrs(pairwise13, pairwise14, pairwise34));

//                        if(i1==10 && i2==29 && i3 == 42 && i4 == 53){
//                            System.out.println("check");
//                        }

                        double max2corr = Math.max(Math.max(pairwise12, pairwise13), pairwise23);
//                        max2corr = Math.max(Math.max(Math.max(max2corr, pairwise14), pairwise24), pairwise34);


                        if (corr >= tau && corr - max2corr > minJump && corr - max2corr > minJump) { //
                            out.add(true);
//                            System.out.println(header.get(i1) + ", " + header.get(i2) + ", " + header.get(i3) + ", " + "\t \t corr: " + corr + "\t\t max subset corr: " + max2corr);
                        }

                    }
//                }
            }
            return out;
        }).mapToInt(Collection::size).sum();
            System.out.println("positives: " + n_positive);
        System.out.println("runtime: "+ (double) (System.currentTimeMillis()-start)/1000);

    }
}

