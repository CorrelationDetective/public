import _aux.lib;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

import static data.readFMRI.transpose;

public class ParamGridTest {

    // this class is to test for multiple parameter combinations


    public static void main(String[] args){
        System.out.println("number of threads: " + ForkJoinPool.commonPool().getParallelism());



        String filename = "../sustainable_cleaned_esg_lognorm.csv";
//        String filename = "../SLPMultipolesPreprocessed.csv";




        if(args.length > 0){
            filename = args[0];
//            metric = args[1];
        }



        int desired_vec = 2500;
        int desired_dim = 200;

        ArrayList<String> header = new ArrayList<>();

        double[][] data = loadData(filename, desired_vec, desired_dim, header);
        for (int i = 0; i < data.length; i++){
            data[i] = lib.znorm(data[i]);
        }
        int n_dim = data[0].length;





        boolean useEmpiricalBounds = true;  //NOTE: setting to false will result in poor performance. always use true!
        //clustering:
        int max_clust_levels = 20;
        int defaultDesiredClusters = 10; // set to Integer.MAX_VALUE for unrestricted and clustering based on epsilon only
        boolean useKMeans = true;
        int breakFirstKLevelsToMoreClusters = 0;
        int clusteringRetries = 50;
        double start_epsilon = 0.5 * Math.sqrt(2*n_dim*(1-0.25));
        double epsilon_multiplier = 0.8;

        //approximation + others
        double[] taus = new double[]{0.95, 0.9, 0.85};
        int[] maxPLefts = new int[]{1, 2};
        int[] maxPRights = new int[]{2, 3, 4};
        int topK = 10_000_000;

        boolean parallelEvaluation = true;
        double shrinkFactor = 0;
        int numberOfPriorityBuckets = 50;
        double maxApproximationSize = Math.sqrt(n_dim * (1- (-10)));

        String metric = "multipearson";



        for(int maxPLeft : maxPLefts){
            for(int maxPRight : maxPRights){
                for(double tau : taus) {
                    if (maxPLeft + maxPRight > 4) continue; // max cardinality 5 for now!
                    System.gc();
                    System.out.println("\n--------------------- new run starting ---------------------\n");
                    configurationParameters params = new configurationParameters(data, metric, tau, start_epsilon,
                            epsilon_multiplier, max_clust_levels, defaultDesiredClusters, useKMeans, breakFirstKLevelsToMoreClusters,
                            clusteringRetries, parallelEvaluation, shrinkFactor, maxApproximationSize, header, useEmpiricalBounds,
                            numberOfPriorityBuckets, topK, maxPLeft, maxPRight);
                    SimpleTest.run(params, false);
                }
            }
        }



        System.out.println("MultiPearson finished here! Now starting multipoles... \n\n\n\n");
        System.gc();


        metric = "multipole";

        maxPLefts = new int[]{3, 4};
        taus = new double[]{0.95, 0.9, 0.85, 0.8};
//        minJumps = new double[]{-5., 0.05, 0.1, 0.15};
        int maxPRight = 0; // ignored for multipoles

        for(int maxPLeft : maxPLefts){
            for(double tau : taus){
                System.gc();
                System.out.println("new run starting:");
                configurationParameters params = new configurationParameters(data, metric, tau, start_epsilon,
                        epsilon_multiplier, max_clust_levels, defaultDesiredClusters, useKMeans, breakFirstKLevelsToMoreClusters,
                        clusteringRetries, parallelEvaluation, shrinkFactor, maxApproximationSize, header, useEmpiricalBounds,
                        numberOfPriorityBuckets, topK, maxPLeft, maxPRight);
                SimpleTest.run(params, false);
            }

        }






    }




    static double[][] loadData(String fileLoc, int desired_vecs, int desired_dim, ArrayList<String> header){
        double[][] dataArray;
        try{
            dataArray = data.readFMRI.readfmri(fileLoc, header);
            assert dataArray != null;
            return transpose(dataArray);
        }catch (Exception ignored){
            System.out.println("no csv file, trying weather and stock methods...");
        }

        try{
            dataArray = SimpleTest.loadDataWeather(fileLoc, desired_vecs, desired_dim);
            for (int i=0;i<desired_vecs;i++) header.add("TS" + (i+1));
            return dataArray;
        }catch (Exception ignored){}

        try{
            for (int i=0;i<desired_vecs;i++) header.add("TS" + (i+1));
            dataArray = SimpleTest.loadFloatData(fileLoc, desired_vecs, desired_dim);
            return dataArray;
        }catch (Exception ignored){}

        System.err.println("none of the data loading methods worked on the file that you provided!" +
                " A csv with header and without row indices should do just fine :)");
        dataArray = new double[1][1];
        return dataArray;
    }
}
