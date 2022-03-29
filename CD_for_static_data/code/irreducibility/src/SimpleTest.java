
import _aux.PostProcessResults;
import _aux.lib;
import bounding.AprioriPruning;
import bounding.CorrelationBounding;
import bounding.HierarchicalBounding;
import clustering.ClusterCombination;
import data.Measure;
import data.readWeatherData;

import javax.security.auth.login.Configuration;
import java.io.BufferedReader;
import java.io.FileReader;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ForkJoinPool;

import static _aux.lib.getStream;
import static data.readFMRI.readfmri;
import static data.readFMRI.transpose;


public class SimpleTest {


    public static void main(String[] args) {

        System.out.println("number of threads: " + ForkJoinPool.commonPool().getParallelism());

        int desired_stocks = 2500;
        double tau;
        double minJump;
        String fileLoc;
        String metric;
        int topK;
        int maxPLeft;
        int maxPRight;

        if(args.length>0){
            fileLoc = args[0];
            metric = args[1];
            tau = Double.parseDouble(args[2]);
            minJump = Double.parseDouble(args[3]);
            topK = Integer.parseInt(args[4]);
            maxPLeft = Integer.parseInt(args[5]);
            maxPRight = Integer.parseInt(args[6]);

        }else{ // quick editing for local test runs
            fileLoc = "../SLPMultipolesPreprocessed.csv";
            metric = "multipole";
            tau = 0.6;
            minJump = 0;
            topK = 10_000_000; // note that topK + no-supersets is ill-defined and we only keep this here to prevent an exorbitant amount of results in case of poor settings
            maxPLeft=4;
            maxPRight=0;
        }
        int desired_n_dim = 200;
        minJump = 0; // not relevant for nu-supersets -> just set to 0.




        ArrayList<String> header = new ArrayList<>();


//
        double[][] fmri = loadData(fileLoc, desired_stocks, desired_n_dim, header);






        for (int i = 0; i < fmri.length; i++){
            fmri[i] = lib.znorm(fmri[i]);
        }
        int n_dim=fmri[0].length;
        int n_stocks=fmri.length;




        ////////////////////////////////    params:    /////////////////////////////////////////////////////////////////

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



        boolean parallelEvaluation = true;
        double shrinkFactor = 0;
        int numberOfPriorityBuckets = 50;
        double maxApproximationSize = Math.sqrt(n_dim * (1- (-10)));
//        maxApproximationSize = Double.MAX_VALUE;


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        configurationParameters params = new configurationParameters(fmri, metric, tau,  start_epsilon, epsilon_multiplier,
                max_clust_levels, defaultDesiredClusters, useKMeans, breakFirstKLevelsToMoreClusters, clusteringRetries,
                parallelEvaluation, shrinkFactor, maxApproximationSize, header, useEmpiricalBounds, numberOfPriorityBuckets,
                topK, maxPLeft, maxPRight);

        run(params, false);
//
    }


    public static void run(configurationParameters par, boolean printResults){
        System.out.println("\n--------------------- new run starting ---------------------\n");


        double[][] data = par.data;
        int n_stocks = data.length;
        int n_dim = data[0].length;

        System.out.println("Starting time " + LocalDateTime.now());
        long currentms = System.currentTimeMillis();

        System.out.println("dataset: n_vec = " + n_stocks + "; n_dim = " + n_dim);
        System.out.println("parallel evaluation enabled: " + par.parallelEvaluation);

        System.out.println("parameters -- tau (correlation threshold) = " + par.tau  + "; minJump = " + 0 + "; start epsilon = " + par.start_epsilon +

                "; max hierarchical clustering levels = " + par.max_clust_levels + "; epsilon multiplier per level = " + par.epsilon_multiplier + "; k: " + par.defaultDesiredClusters+ "; clustering retries: " + par.clusteringRetries);
        System.out.println("approximation shrinkage factor is " + par.shrinkFactor +"; max approximation radius threshold: " + par.maxApproximationSize +" topK: " + par.topK);
        System.out.println("maxPLeft: " + par.maxPLeft +", maxPRight: " + par.maxPRight);


//
        HierarchicalBounding HB;
        HB = new HierarchicalBounding(par.data, par.max_clust_levels, par.defaultDesiredClusters, par.useKMeans,
                par.breakFirstKLevelsToMoreClusters, par.clusteringRetries, par.parallelEvaluation, par.vecNames);


        List<String> results;
        List<ClusterCombination> positiveDCCs;

        if(par.metric.contains("multipole")){
        AprioriPruning AP = new AprioriPruning(HB, par.maxPLeft, par.tau, par.useEmpiricalBounds, par.parallelEvaluation, par.vecNames);
        AP.pruneWithApriori(par.start_epsilon, par.epsilon_multiplier, par.shrinkFactor, par.maxApproximationSize,
        par.numberOfPriorityBuckets, par.topK, currentms);
        CorrelationBounding CB = AP.CB;
        positiveDCCs = AP.positiveDCCs;
         results = AP.positiveResultSet;
        }else if(par.metric.contains("multipearson") || par.metric.contains("MP")){
            HB.recursiveBounding(par.start_epsilon, par.tau, par.epsilon_multiplier, par.useEmpiricalBounds, par.shrinkFactor, par.maxApproximationSize, par.numberOfPriorityBuckets, par.topK,
                    par.maxPLeft, par.maxPRight, currentms);
            positiveDCCs = HB.positiveDCCs; ;
            results = HB.positiveResultSet;
        }else{
            System.err.println("The metric you proivided is not recognized. use multipole or multipearson");
            return;
        }




//
//////

////



//        double baseline_comparisons = 0;
//        for(int p =2; p<=par.maxP; p++){
//            baseline_comparisons += Math.pow(n_stocks, p);
//        }


        System.out.println("number of clustering levels: " + HB.maxLevels + "; #clusters in final layer: " + HB.clustersPerLevel.get(par.max_clust_levels-1).size());
//        System.out.println("number of comparisons: " + HB.n_comparisons + " ("+ HB.n_2bound_comparisons +
//                " full comparisons for 2-correlations; " + HB.n_3bound_comparisons + " lookup comparisons using the 2-corr bounds; " + HB.n_2corr_lookups + " lookups in the 2-correlation table)");


//        System.out.println("number of undecided datapoint combinations: " + HB.n_undecided);
//        System.out.println("baseline comparisons: " + baseline_comparisons);
        int dur =(int) (System.currentTimeMillis()-currentms)/1000;

//        System.out.println("Unavoidable comparisons out of all done are:"+ HB.unavoidableComparisons/(double) (HB.n_comparisons + HB.n_undecided));
        System.out.println("Ending time " + LocalDateTime.now());
        System.out.println("Total duration seconds " + dur); ;
//        System.out.println("baseline - n_decided: " + (baseline_comparisons - AP.n_decided));
        System.out.println("Number of positives: " + results.size());
        System.out.println("lowest corr in positives: " + getStream(positiveDCCs, par.parallelEvaluation).map(ClusterCombination::getLB).reduce(Math::min).orElse(0.));





//         print the results if desired:
        if(printResults){
            CorrelationBounding CB = HB.CB;
            PostProcessResults.printResultSet(positiveDCCs, CB, par.tau, par.vecNames, par.parallelEvaluation);
        }


    }




    static double[][] loadFloatData(String filename, int desired_stocks, int n_dim) {
        double[][] stockData = new double[desired_stocks][n_dim];
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            for (int i=0;i<desired_stocks;i++) {
                String[] line = br.readLine().split(",");
                for (int p=0;p<n_dim;p++)
                    stockData[i][p] = Float.parseFloat(line[p]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stockData;

    }

    public static double[][] loadDataWeather(String filename, int desired_stocks, int n_dim) {

        double[][] dataArray = readWeatherData.readWeather(filename, 2708, 200, Measure.Attribute.slp);
        ArrayList<double[]> data = new ArrayList<>(dataArray.length);
        for(int i = 0; i< dataArray.length; i++){
            data.add(dataArray[i]);
        }
//        Collections.shuffle(data, new Random(123));
        if (n_dim != 0) {
            double[][] reducedData = new double[desired_stocks][n_dim];
            int step =  dataArray[0].length/n_dim;
            int stepi = dataArray.length/desired_stocks;
            for(int i=0; i<desired_stocks; i++){
                for(int j = 0; j<n_dim; j++){
                    reducedData[i][j] = data.get(i*stepi)[j*step];
                }
            }
            return reducedData;
        }else{
            return dataArray;
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

