package algorithms.statics.baselines;

import _aux.*;
import _aux.lists.FastArrayList;
import _aux.StageRunner;
import algorithms.Algorithm;
import bounding.ClusterCombination;
import core.Parameters;
import queries.ResultSet;

import java.util.concurrent.*;
import java.util.function.Function;

public abstract class Baseline extends Algorithm {
    double[] WlFull;
    double[] WrFull;
    public static ResultSet resultSet;

    public Baseline(Parameters par) {
        super(par);
        WlFull = par.Wl[par.Wl.length-1];
        WrFull = par.maxPRight > 0 ? par.Wr[par.Wr.length-1]: null;
        resultSet = new ResultSet(par.queryType, par.topK, par.irreducibility);

//        Sync necessary attributes with AssessCandidateTask
        AssessCandidateTask.par = par;
        AssessCandidateTask.resultSet = resultSet;
    }

    public abstract void prepare();

    @Override
    public ResultSet run() {
        StageRunner stageRunner = new StageRunner(par.LOGGER);

        //        Start the timer
        if (!par.statBag.stopWatch.isStarted()) par.statBag.stopWatch.start();

//        Reset everything that could possibly be changed by previous runs
        par.resetThreshold();

        // --> STAGE 1 - Prepare
        stageRunner.run("Preparation phase", this::prepare, par.statBag.stopWatch);

        // --> STAGE 2 - Iterate and handle candidatesGet candidate pairs
        stageRunner.run("Iterate candidates", this::iterateCandidates, par.statBag.stopWatch);

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
                    par.forkJoinPool.execute(task);
                } else {
                    par.forkJoinPool.invoke(task);
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

    public abstract ClusterCombination computeSimilarity(Pair<FastArrayList<Integer>, FastArrayList<Integer>> candidate);

    @Override
    public void prepareStats(){}

    @Override
    public void printStats(StatBag statBag){
        par.LOGGER.fine("----------- Run statistics --------------");
        this.printStageDurations(statBag);
    }
}
