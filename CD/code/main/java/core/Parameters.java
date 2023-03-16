package core;

import _aux.StatBag;
import algorithms.AlgorithmEnum;
import clustering.ClusteringAlgorithmEnum;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import queries.QueryTypeEnum;
import queries.RunningThreshold;
import similarities.MultivariateSimilarityFunction;
import similarities.functions.TotalCorrelation;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RequiredArgsConstructor
public class Parameters {
//    Logging
    @NonNull @Getter public Logger LOGGER;
    @Getter public String dateTime = (new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss")).format(new Date());
    @NonNull @Getter public String codeVersion;
    @NonNull @Getter public boolean saveStats;
    @NonNull @Getter public String experimentId;
    @Getter public boolean experiment;

    @NonNull @Getter public boolean saveResults;
    @NonNull @Getter public String resultPath;
    @Getter public int threads = ForkJoinPool.getCommonPoolParallelism();

    //    Run details
    @NonNull @Getter public AlgorithmEnum algorithm;
    @NonNull @Getter public boolean parallel;
    @NonNull @Getter public boolean random;
    @NonNull @Getter public int seed;

    //    Query
    @NonNull @Getter public QueryTypeEnum queryType;
    @NonNull @Getter public MultivariateSimilarityFunction simMetric;
    @NonNull @Getter public String aggPattern;
    @NonNull @Getter public double tau;
    public RunningThreshold runningThreshold;
    @NonNull @Getter public double minJump;
    @NonNull @Getter public boolean irreducibility;
    @NonNull @Getter public int topK;
    @Getter public double[][] Wl;
    @Getter public double[][] Wr;
    @NonNull @Getter public int maxPLeft;
    @NonNull @Getter public int maxPRight;
    @NonNull @Getter public boolean allowSideOverlap;

    //    Data
    @NonNull @Getter public String outputPath;
    @NonNull @Getter public String[] headers;
    @NonNull @Getter public double[][] data;
    @NonNull @Getter public int n;
    @NonNull @Getter public int m;
    @NonNull @Getter public int partition;

    //    Bounding
    @NonNull @Getter public boolean empiricalBounding;

    //    Clustering
    @NonNull @Getter public int kMeans;
    @NonNull @Getter public boolean geoCentroid;
    @NonNull @Getter public double startEpsilon;
    @NonNull @Getter public double epsilonMultiplier;
    @Getter public int maxLevels = 20;
    @Getter public ClusteringAlgorithmEnum clusteringAlgorithm = ClusteringAlgorithmEnum.KMEANS;
    @Getter public int breakFirstKLevelsToMoreClusters = 0;
    @Getter public int clusteringRetries = 50;

    //    Top-k
    @NonNull @Getter public double shrinkFactor;
    @NonNull @Getter public double maxApproximationSizeRatio;
    @Getter public double maxApproximationSize;

//    Misc
    @Getter public StatBag statBag;
    @Getter public Random randomGenerator;
    @Setter @Getter public double[][] pairwiseDistances;


    public void init(){
        check();
        setDependentVariables();
    }

    public void setDependentVariables(){
        experiment = experimentId != "";
        maxApproximationSize = simMetric.getMaxApproximationSize(maxApproximationSizeRatio);
        statBag = new StatBag(experiment);
        simMetric.setStatBag(statBag);
        randomGenerator = random ? new Random(): new Random(seed);
        runningThreshold = new RunningThreshold(tau, LOGGER);

        //        Get weight lists
        double[][][] weights = createWeights(aggPattern, maxPLeft, maxPRight);
        Wl = weights[0];
        Wr = weights[1];
    }

    public Parameters clone(){
        Parameters clone = new Parameters(LOGGER, codeVersion, saveStats, experimentId, saveResults, resultPath, algorithm, parallel,
                random, seed, queryType, simMetric, aggPattern, tau, minJump, irreducibility, topK, maxPLeft, maxPRight,
                allowSideOverlap, outputPath, headers, data, n, m, partition, empiricalBounding, kMeans, geoCentroid,
                startEpsilon, epsilonMultiplier, shrinkFactor, maxApproximationSizeRatio);
        clone.setPairwiseDistances(pairwiseDistances);
        clone.init();
        return clone;
    }

//    Check and correct parameter values for incorrect configs
    private void check(){
        corrPatternChecks();
        empiricalBoundingCheck();
        aggPatternChecks();
        queryTypeChecks();
        queryConstraintChecks();
        simMetricChecks();
    }

    private void queryTypeChecks(){
        //        Query-type specific corrections
        switch (queryType){
            case THRESHOLD: {
                if (topK > 0){
                    LOGGER.severe("Query type is threshold, but topK is > 0, setting topK to 0");
                    topK = 0;
                }
                if (shrinkFactor < 1){
                    LOGGER.severe("Query type is threshold, but shrinkFactor is < 1, setting shrinkFactor to 1");
                    shrinkFactor = 1;
                }
                break;
            }
            case TOPK: {
                if (topK <= 0){
                    LOGGER.severe("Query type is top-k, but topK is <= 0, setting topK to 100");
                    topK = 100;
                }
                if (minJump > 0) {
                    LOGGER.severe("Query type is top-k, but minJump is > 0, setting minJump to 0");
                    minJump = 0;
                }
                if (irreducibility) {
                    LOGGER.severe("Query type is top-k, but irreducibility is true, setting irreducibility to false");
                    irreducibility = false;
                }
                if (tau > 0){
                    LOGGER.severe("Query type is top-k, but tau is > 0, setting tau to 0");
                    tau = 0;
                }
                break;
            }
        }
    }

    private void queryConstraintChecks(){
        //        Irreducibility specific corrections
        if (irreducibility && minJump > 0){
            LOGGER.severe("Irreducibility is true, but minJump is > 0, setting minJump to 0");
            minJump = 0;
        }
    }

    private void simMetricChecks(){
//        Specific class checks
        if (simMetric instanceof TotalCorrelation){
//           Always have irreducibility for threshold queries with TC
            if (!irreducibility && minJump == 0 && (queryType == QueryTypeEnum.THRESHOLD || queryType == QueryTypeEnum.PROGRESSIVE)) {
                LOGGER.severe("Irreducibility is trivial for threshold/progressive queries with total correlation. Irreducibility is set to true.");
                irreducibility = true;
            }
        }
    }

    //        Check if pleft and pright are correctly chosen
    private void corrPatternChecks(){
        if (!simMetric.isTwoSided() && maxPRight > 0){
            LOGGER.severe("The chosen similarity metric is not two-sided, but pright is > 0, adding pright to pleft");
            maxPLeft += maxPRight;
            maxPRight = 0;
        }
    }

    //        Check if empirical bounding is possible
    private void empiricalBoundingCheck(){
        if (empiricalBounding && !simMetric.hasEmpiricalBounds()){
            LOGGER.severe("The chosen similarity metric does not support empirical bounding, setting empirical bounding to false");
            empiricalBounding = false;
        }
    }

    private void aggPatternChecks(){
        //        If custom aggregation pattern is chosen, check if minJump is set to 0
        if (aggPattern.startsWith("custom") && minJump > 0){
            LOGGER.severe("Custom aggregation pattern is chosen, but minJump is > 0, setting minJump to 0");
            minJump = 0;
        }
    }

    // Create aggregation function from pattern.
    // Is list because it also needs to consider subset correlations (i.e. mc(1,1), mc(1,2), mc(2,2))
    private static double[][][] createWeights(String aggPattern, int maxPLeft, int maxPRight){
        double[][] Wl = new double[maxPLeft][maxPLeft];
        double[][] Wr = new double[maxPRight][maxPRight];
        switch (aggPattern){
            case "avg": {
                for (int i = 1; i <= maxPLeft; i++) {
                    double[] w = new double[i];
                    Arrays.fill(w, 1d/i);
                    Wl[i-1] = w;
                }
                for (int i = 1; i <= maxPRight; i++) {
                    double[] w = new double[i];
                    Arrays.fill(w, 1d/i);
                    Wr[i-1] = w;
                }
                break;
            }
            case "sum": {
                for (int i = 1; i <= maxPLeft; i++) {
                    double[] w = new double[i];
                    Arrays.fill(w, 1d);
                    Wl[i-1] = w;
                }
                for (int i = 1; i <= maxPRight; i++) {
                    double[] w = new double[i];
                    Arrays.fill(w, 1d);
                    Wr[i-1] = w;
                }
                break;
            }
            default: {
                Pattern pattern = Pattern.compile("custom((\\(([0-9.]+,{0,1})+\\)){2})");
                Matcher matcher = pattern.matcher(aggPattern);
                if (matcher.matches()){
                    String[] leftRight = matcher.group(1).split("\\)\\(");
                    String[] left = leftRight[0].substring(1).split(",");
                    String[] right = leftRight[1].substring(0, leftRight[1].length()-1).split(",");
                    double[] fullLeft =Arrays.stream(left).mapToDouble(Double::parseDouble).toArray();
                    double[] fullRight =Arrays.stream(right).mapToDouble(Double::parseDouble).toArray();
                    for (int i = 1; i <= maxPLeft; i++) {
                        double[] w = Arrays.copyOfRange(fullLeft, 0, i);
                        Wl[i-1] = w;
                    }
                    for (int i = 1; i <= maxPRight; i++) {
                        double[] w = Arrays.copyOfRange(fullRight, 0, i);
                        Wr[i-1] = w;
                    }
                } else {
                    throw new InputMismatchException("Aggregation pattern not recognized, should be 'avg', 'sum' or 'custom(u0,u1,...,uPLeft)(w0,w1,...,wPRight)'");
                }
                break;
            }
        }

        return new double[][][]{Wl, Wr};
    }
}

