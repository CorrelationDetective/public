package core;

import _aux.StatBag;
import algorithms.AlgorithmEnum;
import algorithms.streaming.sdstream.SDStream;
import algorithms.streaming.StreamingAlgorithm;
import clustering.ClusteringAlgorithmEnum;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import queries.QueryTypeEnum;
import queries.RunningThreshold;
import similarities.MultivariateSimilarityFunction;
import streaming.AggregationMethod;
import streaming.ArrivalQueue;
import streaming.BatchModelEnum;
import streaming.TimeSeries;
import streaming.index.DccIndex;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.logging.Level.*;

@RequiredArgsConstructor
public class Parameters {
//    Logging

    @NonNull @Getter public Level logLevel;
    @Getter public Logger LOGGER;
    @Getter public String dateTime = (new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss")).format(new Date());
    @NonNull @Getter public String codeVersion;
    @NonNull @Getter public boolean saveStats;
    @NonNull @Getter public String experimentId;
    @Getter public boolean experiment;

    @NonNull @Getter public boolean saveResults;
    @NonNull @Getter public String resultPath;
    public FileWriter resultWriter;

    @Getter public int threads = ForkJoinPool.getCommonPoolParallelism()*2;
    public final ForkJoinPool forkJoinPool = new ForkJoinPool(threads);
    //    public static final ForkJoinPool forkJoinPool = new ForkJoinPool(Math.min(80, Runtime.getRuntime().availableProcessors()*4));

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
    @Getter public RunningThreshold runningThreshold;
    @NonNull @Getter public double minJump;
    @NonNull @Getter public boolean irreducibility;
    @NonNull @Getter public int topK;
    @NonNull @Getter public int topKBufferRatio;

    @Getter public double[][] Wl;
    @Getter public double[][] Wr;
    @NonNull @Getter public int maxPLeft;
    @NonNull @Getter public int maxPRight;
    @NonNull @Getter public boolean allowSideOverlap;

    //    Data
    @NonNull @Getter public String outputPath;
    @NonNull @Getter public int n;
    @Getter public int maxT;

    //    Bounding
    @NonNull @Getter public boolean empiricalBounding;

    //    Clustering
    @NonNull @Getter public int kMeans;
    @Getter public double startEpsilon;
    @Getter public double epsilonMultiplier = .8;
    @Getter public int maxLevels = 20;
    @Getter public ClusteringAlgorithmEnum clusteringAlgorithm = ClusteringAlgorithmEnum.KMEANS;
    @Getter public int breakFirstKLevelsToMoreClusters = 0;
    @Getter public int clusteringRetries = 50;

    //    Top-k
    @NonNull @Getter public double shrinkFactor;
    @NonNull @Getter public double maxApproximationSizeRatio;
    @Getter public double maxApproximationSize;

//    Streaming
    //    Attribute indicating current simulated time
    public double time = 0;

    //    Attribute indicating the current time step
    public int epoch = 0;

//    Number of basic windows in the sliding window
    @NonNull @Getter public int slidingWindowSize;

//    Simulated time per epoch (previously called "timepoint")
    @NonNull @Getter public int timePerEpoch;

//    One update of the result set (i.e. handling of one batch)
    @NonNull @Getter public int epochs;
    @NonNull @Getter public BatchModelEnum batchModel;

//    Number of arrivals per epoch (only for countbased batch model)
    @NonNull @Getter public int arrivalBatchSize;

//    Number of epochs per basic window
    @NonNull @Getter public int basicWindowSize;

//    The method of aggregation to compute the basic window digest
    @NonNull @Getter public AggregationMethod basicWindowAggMethod;

    @NonNull @Getter public TimeSeries[] timeSeries;
    @NonNull @Getter public ArrivalQueue arrivalQueue;

//    SDHybrid parameters
    @Setter @Getter public StreamingAlgorithm activeAlgorithm;
    @NonNull @Getter public int boostEpochs;
    @NonNull @Getter public int boostFactor;
    @NonNull @Getter public int warmupEpochs;
    @Setter @Getter public int warmupStop;

//    Inferred streaming
    @Getter public DccIndex ubIndex;
    @Getter public DccIndex lbIndex;

//    Misc
    @Getter public StatBag statBag;
    @Getter public Random randomGenerator;
    @Setter @Getter public double[][] pairwiseDistances;


    public void init(){
        LOGGER = getLogger(logLevel);
        check();
        setDependentVariables();
    }

    //    Check and correct parameter values for incorrect configs
    private void check(){
        corrPatternChecks();
        empiricalBoundingCheck();
        aggPatternChecks();
        streamingParameterChecks();
        queryTypeChecks();
        queryConstraintChecks();
        hybridParameterChecks();
    }

    public void setDependentVariables(){
        experiment = !(experimentId.equals("") || experimentId.equals("test"));

        LOGGER.info("ExperimentId: " + experimentId + " DCC monitoring = " + !experiment);

        maxApproximationSize = simMetric.simToDist(simMetric.MIN_SIMILARITY + maxApproximationSizeRatio*simMetric.SIMRANGE);
        statBag = new StatBag(experiment, epochs);
        simMetric.setStatBag(statBag);
        randomGenerator = random ? new Random(): new Random(seed);
        runningThreshold = new RunningThreshold(tau, LOGGER);

//        Increase topK to include buffer size
        topK *= topKBufferRatio;

        //        Simmetric dependent parameters
        if (algorithm != AlgorithmEnum.CD && !simMetric.canStream()){
            LOGGER.severe("The chosen similarity metric does not support streaming, setting algorithm to SD");
            algorithm = AlgorithmEnum.CD;
        }
        startEpsilon = simMetric.simToDist(0.81*simMetric.MAX_SIMILARITY);
        warmupStop = warmupEpochs;

//        Initialize dcc index if streaming
        ubIndex = new DccIndex(n);
        lbIndex = new DccIndex(n);

        //        Get weight lists
        double[][][] weights = createWeights(aggPattern, maxPLeft, maxPRight);
        Wl = weights[0];
        Wr = weights[1];

//        Create filewriter
        if (saveResults){


            try{
//            Make root dirs if necessary
                String rootdirname = Pattern.compile("\\/[A-z_0-9.]+.csv").matcher(resultPath).replaceAll("");
                new File(rootdirname).mkdirs();

                File file = new File(resultPath);

                resultWriter = new FileWriter(file, false);

    //            Write header
                resultWriter.write("lhs,rhs,headers1,headers2,sim,timestamp,epoch\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public Parameters clone(){
        Parameters clone = new Parameters(logLevel, codeVersion, saveStats, experimentId, saveResults, resultPath, algorithm, parallel,
                random, seed, queryType, simMetric, aggPattern, tau, minJump, irreducibility, topK, topKBufferRatio, maxPLeft, maxPRight,
                allowSideOverlap, outputPath, n, empiricalBounding, kMeans,
                shrinkFactor, maxApproximationSizeRatio,
                slidingWindowSize, timePerEpoch, epochs, batchModel, arrivalBatchSize, basicWindowSize, basicWindowAggMethod,
                timeSeries, arrivalQueue,
                boostEpochs, boostFactor, warmupEpochs
                );
        clone.setPairwiseDistances(pairwiseDistances);
        clone.init();
        return clone;
    }

    public boolean usingSDStream(){ return activeAlgorithm instanceof SDStream; }
    public boolean useIndex(){
        return algorithm != AlgorithmEnum.CD && (epoch == 0 || usingSDStream());
    }

    public void resetThreshold(){
        runningThreshold = new RunningThreshold(tau, LOGGER);
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
                if (!algorithm.equals(AlgorithmEnum.CD)) { // STREAMING
                    if (topKBufferRatio <= 0){
                        LOGGER.severe("Topk buffer size is <= 0, setting topk buffer size to 2");
                        topKBufferRatio = 2;
                    }
                } else {
                    if (topKBufferRatio != 1){
                        LOGGER.severe("Running oneshot with an unnecessary topk buffer, setting topk buffer size to 1");
                        topKBufferRatio = 1;
                    }
                }
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

    private void streamingParameterChecks(){
        if (queryType.equals(QueryTypeEnum.PROGRESSIVE)){
            LOGGER.severe("Progressive queries are not supported for streaming, setting query type to top-k");
            queryType = QueryTypeEnum.TOPK;
        }

        if (timePerEpoch <= 0){
            LOGGER.severe("Timepoint is <= 0, setting timepoint to 1");
            timePerEpoch = 1;
        }

//        Batch model checks
        if (batchModel.equals(BatchModelEnum.COUNTBASED) && arrivalBatchSize <= 0){
            LOGGER.severe("Batch model is count-based, but arrivalBatchSize is <= 0, setting arrivalBatchSize to 10");
            arrivalBatchSize = 10;
        }

        if (batchModel.equals(BatchModelEnum.TIMEBASED)){
            if (arrivalBatchSize > 0){
                LOGGER.severe("Batch model is time-based, but arrivalBatchSize is > 0, setting arrivalBatchSize to 0");
                arrivalBatchSize = 0;
            }
        }
    }

    private void hybridParameterChecks(){
        if (epochs < warmupEpochs){
            LOGGER.severe("Warmup epochs is > epochs, setting warmup epochs to epochs/2");
            warmupEpochs = epochs/2;
        }
        if (boostEpochs == 0) boostFactor = 1; // to prevent error
        if (boostFactor == 0) boostFactor = 1; // to prevent error

        if (algorithm == AlgorithmEnum.CDHYBRID){
            if (epochs < boostEpochs + warmupEpochs){
                LOGGER.severe("Number of epochs is too small, setting epochs to boostEpochs + warmupEpochs");
                epochs = boostEpochs + warmupEpochs;
            }
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

    public Logger getLogger(Level logLevel){
        Logger mainLogger = Logger.getLogger("com.logicbig");
        mainLogger.setUseParentHandlers(false);

        String ANSI_BLACK = "\u001B[30m";
        String ANSI_RED = "\u001B[31m";
        String ANSI_GREEN = "\u001B[32m";
        String ANSI_YELLOW = "\u001B[33m";
        String ANSI_BLUE = "\u001B[34m";
        String ANSI_PURPLE = "\u001B[35m";
        String ANSI_CYAN = "\u001B[36m";
        String ANSI_WHITE = "\u001B[37m";

        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter() {
            private static final String format = "%1$s[%2$tF %2$tT] [%3$s] [%4$s - epoch %5$d] %6$s %n";
            @Override
            public synchronized String format(LogRecord lr) {
                String color = ANSI_WHITE;
                if (lr.getLevel().equals(WARNING)) {
                    color = ANSI_YELLOW;
                }   else if (lr.getLevel().equals(SEVERE)) {
                    color = ANSI_RED;
                }
                return String.format(format,
                        color,
                        new Date(lr.getMillis()),
                        lr.getLevel().getLocalizedName(),
                        getActiveAlgorithm() != null ? getActiveAlgorithm().getClass().getSimpleName(): "Main",
                        epoch,
                        lr.getMessage()
                );
            }
        });
        handler.setLevel(logLevel);
        mainLogger.addHandler(handler);
        mainLogger.setLevel(logLevel);

        return mainLogger;
    }
}

