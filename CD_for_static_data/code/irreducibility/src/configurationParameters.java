import java.util.List;

public class configurationParameters {
    public double start_epsilon;
    public double epsilon_multiplier;
    public int max_clust_levels;
    public int defaultDesiredClusters;
    public double tau;
    public double[][] data;
    public boolean useKMeans;
    public int breakFirstKLevelsToMoreClusters, clusteringRetries;
    public boolean parallelEvaluation;
    public double shrinkFactor;
    public double maxApproximationSize;
    public List<String> vecNames;
    public boolean useEmpiricalBounds;
    public int numberOfPriorityBuckets;
    public int topK;
    public int maxPLeft;
    public int maxPRight;
    public String metric;

    public configurationParameters(double[][] data, String metric, double tau, double start_epsilon, double epsilon_multiplier,
                                   int max_clust_levels, int defaultDesiredClusters,  boolean useKMeans,
                                   int breakFirstKLevelsToMoreClusters, int clusteringRetries, boolean parallelEvaluation,
                                   double shrinkFactor, double maxApproximationSize, List<String> vecNames, boolean useEmpiricalBounds,
                                   int numberOfPriorityBuckets, int topK, int maxPLeft, int maxPRight) {

        this.start_epsilon = start_epsilon;
        this.epsilon_multiplier = epsilon_multiplier;
        this.max_clust_levels = max_clust_levels;
        this.defaultDesiredClusters = defaultDesiredClusters;
        this.tau = tau;
        this.data = data;
        this.useKMeans = useKMeans;
        this.breakFirstKLevelsToMoreClusters = breakFirstKLevelsToMoreClusters;
        this.clusteringRetries = clusteringRetries;
        this.parallelEvaluation = parallelEvaluation;
        this.shrinkFactor = shrinkFactor;
        this.maxApproximationSize = maxApproximationSize;
        this.vecNames = vecNames;
        this.useEmpiricalBounds = useEmpiricalBounds;
        this.numberOfPriorityBuckets = numberOfPriorityBuckets;
        this.topK = topK;
        this.maxPLeft = maxPLeft;
        this.maxPRight = maxPRight;
        this.metric = metric;
    }
}
