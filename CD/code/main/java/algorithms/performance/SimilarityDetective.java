package algorithms.performance;

import _aux.*;
import _aux.lists.FastArrayList;
import algorithms.Algorithm;
import _aux.StageRunner;
import bounding.ClusterCombination;
import bounding.RecursiveBounding;
import clustering.HierarchicalClustering;
import core.Parameters;
import queries.ResultSet;
import queries.ResultTuple;

public class SimilarityDetective extends Algorithm {
    public HierarchicalClustering HC;
    public RecursiveBounding RB;

    public SimilarityDetective(Parameters par) {
        super(par);
        HC = new HierarchicalClustering(par);
    }

    @Override
    public ResultSet run() {
        StageRunner stageRunner = new StageRunner(par.LOGGER);

//        Start the timer
        par.statBag.stopWatch.start();

//        STAGE 1 - Compute pairwise distances if using empirical bounds
        par.setPairwiseDistances(
                stageRunner.run("Compute pairwise distances",
                        () -> par.simMetric.computePairwiseDistances(par.data, par.parallel), par.statBag.stopWatch)
        );

//        STAGE 2 - Hierarchical clustering
        stageRunner.run("Hierarchical clustering", () -> HC.run(), par.statBag.stopWatch);

//        STAGE 3 - Recursive bounding
        RB = new RecursiveBounding(par, HC.clusterTree.get(0).getFirst(), HC.singletonClusters);
        ResultSet resultSet = stageRunner.run("Recursive bounding", () -> RB.run(), par.statBag.stopWatch);

        par.statBag.stopWatch.stop();
        par.statBag.totalDuration = lib.nanoToSec(par.statBag.stopWatch.getNanoTime());
        par.statBag.stageDurations = stageRunner.stageDurations;
        this.prepareStats();

        return resultSet;
    }



    @Override
    public void prepareStats(){
//        Manually set postprocessing stage time
        par.statBag.stageDurations.add(new Stage("Handling postponed branches (during RB)",
                par.statBag.getDFSTime().doubleValue() / 1e9));
    }

    @Override
    public void printStats(StatBag statBag) {
        par.LOGGER.fine("----------- Run statistics --------------");

//        CCs and lookups
        par.LOGGER.fine(String.format("%-30s %d","nLookups:", par.statBag.getNLookups().get()));
        par.LOGGER.fine(String.format("%-30s %d","nCCs:", par.statBag.getNCCs().get()));
        par.LOGGER.fine(String.format("%-30s %d","nSeqCCs:", par.statBag.getNSecCCs().get()));
        par.LOGGER.fine(String.format("%-30s %d","nParallelCCs:", par.statBag.getNParallelCCs().get()));
        par.LOGGER.fine(String.format("%-30s %.1f","avgCCSize:",
                (double) par.statBag.getTotalCCSize().get() / (double) par.statBag.getNCCs().get()));

//        DCCs
        par.LOGGER.fine(String.format("%-30s %d","nPosDCCs:", par.statBag.getNPosDCCs().get()));
        par.LOGGER.fine(String.format("%-30s %d","nNegDCCs:", par.statBag.getNNegDCCs().get()));

        this.printStageDurations(statBag);
    }

}
