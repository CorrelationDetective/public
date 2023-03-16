package algorithms.baselines;

import _aux.*;
import _aux.lists.FastArrayList;
import algorithms.Algorithm;
import _aux.StageRunner;
import bounding.ClusterCombination;
import bounding.RecursiveBoundingTask;
import core.Parameters;
import queries.QueryTypeEnum;
import queries.ResultObject;
import queries.ResultSet;
import queries.ResultTuple;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class Baseline extends Algorithm {
    public static final ForkJoinPool forkJoinPool = new ForkJoinPool(Math.min(80, Runtime.getRuntime().availableProcessors()*4));
    double[] WlFull;
    double[] WrFull;
    public static ResultSet resultSet;

    public Baseline(Parameters par) {
        super(par);
        WlFull = par.Wl[par.Wl.length-1];
        WrFull = par.maxPRight > 0 ? par.Wr[par.Wr.length-1]: null;
        resultSet = new ResultSet(par.queryType, par.topK, par.irreducibility, par.saveResults);

//        Sync necessary attributes with AssessCandidateTask
        AssessCandidateTask.par = par;
        AssessCandidateTask.resultSet = resultSet;
    }

    public abstract void prepare();

    @Override
    public ResultSet run() {
        StageRunner stageRunner = new StageRunner(par.LOGGER);

        //        Start the timer
        par.statBag.stopWatch.start();

        // --> STAGE 1 - Prepare
        stageRunner.run("Preparation phase", this::prepare, par.statBag.stopWatch);

        // --> STAGE 2 - Iterate and handle candidatesGet candidate pairs
        stageRunner.run("Iterate candidates", this::iterateCandidates, par.statBag.stopWatch);

        par.statBag.stopWatch.stop();
        par.statBag.totalDuration = lib.nanoToSec(par.statBag.stopWatch.getNanoTime());
        par.statBag.stageDurations = stageRunner.stageDurations;

        return resultSet;
    }

    private void iterateCandidates() {

//        TODO MAKE DEPENDENT OF ALLOWSIDEOVERLAP PARAMETER
        FastArrayList<AssessCandidateTask> tasks = new FastArrayList<>(par.n * par.n);
        for (int i = 0; i < par.n; i++) {
            for (int j = 0; j < par.n; j++) {
                if (i==j) continue;

                FastArrayList<Integer> LHS = new FastArrayList<>(par.maxPLeft);
                LHS.add(i);

                FastArrayList<Integer> RHS = new FastArrayList<>(par.maxPRight);

                if (par.maxPRight > 0) {
                    RHS.add(j);
                } else {
                    LHS.add(j);
                }

                AssessCandidateTask task = new AssessCandidateTask(LHS,RHS,0, this.getSimilarityComputer());
                if (par.parallel){
                    forkJoinPool.execute(task);
                } else {
                    forkJoinPool.invoke(task);
                }
                tasks.add(task);
            }

            //  Wait for all tasks to finish
            for (AssessCandidateTask task : tasks) {
                task.join();
            }
        }
    }

    //    Go over candidate and check if it (or its subsets) has a significant similarity
    public Function getSimilarityComputer(){
        return o -> computeSimilarity((Pair<FastArrayList<Integer>, FastArrayList<Integer>>) o);
    }

    public abstract double computeSimilarity(Pair<FastArrayList<Integer>, FastArrayList<Integer>> candidate);

    public double[] linearCombination(FastArrayList<Integer> idx, double[] W){
        double[] v = new double[par.m];
        for (int i = 0; i < idx.size(); i++) {
            v = lib.add(v, lib.scale(par.data[idx.get(0)], W[i]));
        }
        return v;
    }

    @Override
    public void prepareStats(){}

    @Override
    public void printStats(StatBag statBag){
        par.LOGGER.fine("----------- Run statistics --------------");
        this.printStageDurations(statBag);
    }
}
