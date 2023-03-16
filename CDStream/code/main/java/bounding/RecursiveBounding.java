package bounding;

import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import queries.*;
import core.Parameters;
import clustering.Cluster;
import lombok.NonNull;
import streaming.index.DccIndex;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class RecursiveBounding {

    @NonNull private final Parameters par;
    @NonNull private final Cluster rootCluster;
    @NonNull private final Cluster[] singletonClusters;
    public ResultSet resultSet;

    //    Statistics
    public AtomicLong nNegDCCs = new AtomicLong(0);
    public double DFSTime;

//    Constructor
    public RecursiveBounding(Parameters par, Cluster rootCluster, Cluster[] singletonClusters) {
        this.par = par;
        this.rootCluster = rootCluster;
        this.singletonClusters = singletonClusters;
        resultSet = new ResultSet(par.queryType, par.topK, par.irreducibility);

//        Sync necessary attributes with ResursiveBoundingTask
        RecursiveBoundingTask.resetPostponedDCCs();
    }

    public ResultSet run() {
//        Always do full climb unless custom aggregation is used
        boolean fullClimb = !par.aggPattern.contains("custom");

//        Expand topK when running topk or progressive query
        boolean expandTopK = par.queryType == QueryTypeEnum.TOPK || par.queryType == QueryTypeEnum.PROGRESSIVE;

        try {
            complexityClimb(fullClimb, expandTopK);
        } catch (Exception e) {
//            Get root cause
            while (e.getCause() != null) {
                e = (Exception) e.getCause();
            }
            if (e instanceof ProgressiveStopException) {
                par.LOGGER.info(e.getMessage());
            } else {
                e.printStackTrace();
                System.exit(1);
            }
        }

//        Set statistics
        par.statBag.addToStat(par.statBag.getNPosDCCs(), resultSet::size);

//        Convert to tuples
        return resultSet;
    }

    public void complexityClimb(boolean fullClimb, boolean expandTopK) throws ProgressiveStopException {
        // Setup first iteration
        int pLeft = par.maxPRight > 0 ? 1 : 2;
        int pRight = par.maxPRight > 0 ? 1 : 0;

        ClusterCombination rootCandidate = getRootCandidate(pLeft,pRight);

        // Do first iteration with shrinkFactor 1
        double runningShrinkFactor = 1;

        while (true) {
            int headPLeft = rootCandidate.getLHS().size();
            int headPRight = rootCandidate.getRHS().size();

//            -------------- Handle complexity level --------------
            par.LOGGER.info(String.format("Starting on combinations with complexity (%d,%d)", headPLeft, headPRight));
            mineSimilarityPattern(rootCandidate, runningShrinkFactor);
            par.LOGGER.info(String.format("----- Done with complexity level, current resultSet size: %d", resultSet.size()));

//            -------------- Prepare for next complexity level -------------

//            Check if we are done
            if (headPLeft == par.maxPLeft && headPRight == par.maxPRight)
                break;

//            Always expand left first
            boolean expandLeft = headPRight == par.maxPRight || (headPLeft == headPRight && headPLeft < par.maxPLeft);

            //  Expand topK if necessary
            if (expandTopK){
                expandTopK(expandLeft, headPLeft, headPRight);
            }

            //  Increase complexity by expanding the rootCandidate (i.e. fully traverse combination tree)
            int nextPLeft = fullClimb ? (expandLeft ? headPLeft + 1 : headPLeft): par.maxPLeft;
            int nextPRight = fullClimb ? (expandLeft ? headPRight : headPRight + 1): par.maxPRight;
            rootCandidate = getRootCandidate(nextPLeft, nextPRight);

            //   Set shrink factor back to original value
            runningShrinkFactor = par.shrinkFactor;
        }
    }

    public ClusterCombination getRootCandidate(int pLeft, int pRight){
        FastArrayList<Cluster> LHS = new FastArrayList<>(pLeft);
        FastArrayList<Cluster> RHS = new FastArrayList<>(pRight);
        for (int i = 0; i < pLeft; i++) {
            LHS.add(rootCluster);
        }
        for (int i = 0; i < pRight; i++) {
            RHS.add(rootCluster);
        }
        return new ClusterCombination(LHS, RHS, 0, (int) Math.pow(rootCluster.size(), pLeft+pRight));
    }

    public void mineSimilarityPattern(ClusterCombination rootCandidate, double shrinkFactor) throws ProgressiveStopException {
        try {
//            Start with initial BFS scan with given shrinkFactor, postponing CCs if necessary
            par.forkJoinPool.invoke(new RecursiveBoundingTask(rootCandidate, shrinkFactor, par, resultSet));

            par.LOGGER.info("Done with initial scan, now starting DFS with threshold " + par.getRunningThreshold().get());

//            Skip if not postponing
            if (shrinkFactor == 1)
                return;

            long startTime = System.nanoTime();

            // Now iterate over approximated DCCs without shrinking, starting with highest priority
            runCCs(RecursiveBoundingTask.postponedDCCs, par, 1, resultSet);

            DFSTime += (System.nanoTime() - startTime) / 1e9;
            par.LOGGER.info("Done with full scan, threshold now " + par.getRunningThreshold().get());

        } catch (RuntimeException e) {
            Exception rootCause = e;
            while (rootCause.getCause() != null) {
                rootCause = (Exception) rootCause.getCause();
            }
            if (rootCause instanceof ProgressiveStopException) {
                throw (ProgressiveStopException) rootCause;
            } else {
                throw e;
            }
        }
    }

    public static void runCCs(Collection<ClusterCombination> ccQueue, Parameters par, int shrinkFactor, ResultSet resultSet) throws ProgressiveStopException{
        //  Invoke all tasks
        FastArrayList<RecursiveBoundingTask> tasks = new FastArrayList<>(ccQueue.size());
        for (ClusterCombination cc : ccQueue){
            RecursiveBoundingTask task = new RecursiveBoundingTask(cc, shrinkFactor, par, resultSet);
            if (par.parallel){
                par.forkJoinPool.execute(task);
            } else {
                par.forkJoinPool.invoke(task);
            }
            tasks.add(task);
        }

        //  Wait for all tasks to finish
        for (RecursiveBoundingTask task : tasks) {
            task.join();
        }
    }

    //      Expand the topmost combinations in the topk and compute their similarity exactly to increase threshold
    public void expandTopK(boolean expandLeft, int currentLSize, int currentRSize) throws ProgressiveStopException {
        int nPeak = resultSet.size();
        if (nPeak < 1) return;

        boolean equalSideLength = currentLSize == currentRSize;

//          Copy old ResultDCCs so that we can reset it after exhaustivePrioritization (to avoid duplicates)
        PriorityQueue<ClusterCombination> oldResultDCCs = new PriorityQueue<>(resultSet.getResults().size(), ClusterCombination::compareTo);
        oldResultDCCs.addAll(resultSet.getResults());

//        Get topK combinations (of maximal size!) and sort them by their similarity in descending order
        FastArrayList<ClusterCombination> topKCombinations = new FastArrayList<>(2* par.topK);
        for (ClusterCombination cc: resultSet.getResults()){
//            Check if maximal
            if (!(cc.getLHS().size() == currentLSize && cc.getRHS().size() == currentRSize)){
                continue;
            }

            topKCombinations.add(cc);

//        Expand both sides for topK combinations with equally-sized sides (e.g. (1,1) or (2,2))
            if (equalSideLength){
                topKCombinations.add(cc.getMirror());
            }
        }

//        Create this to avoid duplicates
        HashSet<ClusterCombination> exhaustiveCombinations = new HashSet<>(nPeak * par.n);

//        Iterate over array in reverse order (descending on similarity)
        FastArrayList<RecursiveBoundingTask> tasks = new FastArrayList<>(topKCombinations.size() * par.n);
        for (ClusterCombination topCC: topKCombinations) {
            if (!topCC.isSingleton()){
                par.getLOGGER().severe("Something went wrong in topK updating; found non-singleton combination");
            }

            FastArrayList<Cluster> oldSide = expandLeft ? topCC.getLHS() : topCC.getRHS();
            FastArrayList<Cluster> otherSide = expandLeft ? topCC.getRHS() : topCC.getLHS();

//            Expand topCC by adding all singleton clusters and compute their similarity exactly
            for (Cluster c: singletonClusters){

//                No duplicate vectors or side overlap allowed
                if (oldSide.contains(c) || otherSide.contains(c)) continue;

//                Create new CC and sort sides in descending order (to avoid duplicates)
                ClusterCombination newCC = topCC.expand(c, expandLeft);
                newCC.sortSides(false);
                newCC.setArtificial(true);

//                Skip duplicates
                if (exhaustiveCombinations.contains(newCC)) continue;
                exhaustiveCombinations.add(newCC);

                RecursiveBoundingTask task = new RecursiveBoundingTask(newCC, 1, par, resultSet);
                if (par.parallel){
                    par.forkJoinPool.execute(task);
                } else {
                    par.forkJoinPool.invoke(task);
                }
                tasks.add(task);
            }
        }
//          Join tasks to wait until all are done
        for (RecursiveBoundingTask task : tasks) {
            task.join();
        }

        par.getLOGGER().info("Threshold after topK expanding: " + par.getRunningThreshold().get());

//      Reset ResultDCCs
        resultSet.setResults(oldResultDCCs);
    }
}