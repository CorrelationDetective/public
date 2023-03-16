package algorithms.streaming;

import _aux.StageRunner;
import _aux.lib;
import algorithms.streaming.sdstream.SDStream;
import bounding.ClusterCombination;
import bounding.RecursiveBoundingTask;
import core.Parameters;
import lombok.Getter;
import queries.ResultSet;

import java.util.LinkedList;
import java.util.concurrent.*;

public class SDHybrid extends StreamingAlgorithm {
    private final static int coolDownEpochs = 10;
    private int epochsSinceSwitch = 0;


//    Epochs with actual arrivals -- necessary to avoid switching too early
    private boolean inWarmup = true;
    private int arrivalEpoch = 0;
    @Getter protected final SDStream sdStream;
    @Getter protected final SDOneShot sdOneShot;

    //    Old statistics used for switching
    private ResultSet oldResultSet;
    private long oldDccCount;
    private ConcurrentHashMap oldClusterCache;


    public SDHybrid(Parameters par) {
        super(par);

        //        HYBRID
        this.sdStream = new SDStream(par);
        this.sdOneShot = new SDOneShot(par);

//        Set initial algorithm
        par.setActiveAlgorithm(sdStream);

    }

    @Override
    public ResultSet run() {
        StageRunner stageRunner = stageRunners[par.epoch-1];

//        The fact that we are here means that we have a new arrival
        arrivalEpoch++;

//      STAGE H1: Choose algorithm to run with
        stageRunner.run("Choosing run algorithm", this::chooseAlgorithm, par.statBag.stopWatch);

//        Sync the necessary global variables
        StreamingAlgorithm activeAlgorithm = par.getActiveAlgorithm();
        activeAlgorithm.updatedTS = this.updatedTS;
        activeAlgorithm.oldDistances = this.oldDistances;
        activeAlgorithm.resultSet = this.resultSet;

//      STAGE H2: Run algorithm
        long start = System.nanoTime();
        ResultSet rs = stageRunner.run("Running algorithm", activeAlgorithm::run, par.statBag.stopWatch);
        double runtime = lib.nanoToSec(System.nanoTime() - start);

//      STAGE H3: Update regressors
        stageRunner.run("Updating regressor", () -> activeAlgorithm.runtimePredictor.update(nArrivals, runtime), par.statBag.stopWatch);

        return rs;
    }

    public void chooseAlgorithm(){
        if (arrivalEpoch == 1) par.getLOGGER().info(String.format("---- Running warmup period for %d epochs (with arrivals) ----", par.warmupEpochs));

//        Consider cooldown period
        if (epochsSinceSwitch >= coolDownEpochs || arrivalEpoch < par.warmupEpochs){
//            Consider warmup period
            if (arrivalEpoch == par.warmupEpochs / 2 + 1){
                par.getLOGGER().info("---- Warmup period finished for SDStream ----");
                switchToOneshot();
                return;
            }
//            No warmup anymore -> choose by runtime prediction
            else if (arrivalEpoch > par.warmupEpochs){
//                If just out of warmup, set some stats
                if (inWarmup){
                    inWarmup = false;
                    par.setWarmupStop(par.epoch);
                }

                double pStreamRuntime = sdStream.runtimePredictor.predict(nArrivals);
                double pOneShotRuntime = sdOneShot.runtimePredictor.predict(nArrivals);

                par.getLOGGER().fine(String.format("Predicted runtime for stream: %.2f, for one-shot: %.2f", pStreamRuntime, pOneShotRuntime));

//                Now oneshot but streaming faster -> switch
                if (!par.usingSDStream() && pStreamRuntime < pOneShotRuntime){
                    switchToStream();
                    return;
                }
//                Now streaming but oneshot faster -> switch
                else if (par.usingSDStream() && pStreamRuntime > pOneShotRuntime){
                    switchToOneshot();
                    return;
                }
            }
        } else {
            par.getLOGGER().fine(String.format("Cooling down for %d epochs (with arrivals)", coolDownEpochs - epochsSinceSwitch));
        }
        epochsSinceSwitch++;
    }

    public void switchToOneshot(){
        if (!par.usingSDStream()) {
            par.getLOGGER().severe("Asked to switch to oneshot but already using oneshot");
            return;
        }
        par.getLOGGER().warning("---- Switching to SDOneshot! ----");
        par.setActiveAlgorithm(sdOneShot);
        epochsSinceSwitch = 0;

//        Set old resultSet to find differences later
        oldResultSet = this.resultSet.clone();
        oldClusterCache = new ConcurrentHashMap(par.simMetric.pairwiseClusterCache);

//        Set old distances to find differences later
        this.oldDistances = lib.deepCopy(par.pairwiseDistances);
        oldDccCount = par.statBag.getNIndexedDCCs().get();
    }

    public void switchToStream(){
        if (par.usingSDStream()) {
            par.getLOGGER().severe("Asked to switch to stream but already using stream");
            return;
        }

        par.getLOGGER().warning("---- Switching to SDStream! ----");
        par.setActiveAlgorithm(sdStream);
        epochsSinceSwitch = 0;

//        Set statistics back to old
        par.statBag.getNIndexedDCCs().set(oldDccCount);
        par.simMetric.pairwiseClusterCache = oldClusterCache;

        if (this.oldResultSet == null){
            throw new IllegalStateException("Asked to switch to stream but no oldResultSet saved");
        }

        // Handle new negatives; rebound old cc and index as negative DCC
        for (ClusterCombination cc: oldResultSet.getResults()){
            if (!resultSet.getResults().contains(cc)) {
//                Bound and index old cc
                new RecursiveBoundingTask(cc, 1, par, resultSet).invoke();
            }
        }

//        Handle new positives; remove current cc, rebound old cc and add to resultset
//        (make sure that we add dupeblock to resultset for when negative superset gets triggered)
        LinkedList<ClusterCombination> resultSetCopy = new LinkedList<>(resultSet.getResults());
        for (ClusterCombination cc : resultSetCopy){
            if (!oldResultSet.getResults().contains(cc)) {
//                Remove current cc
                resultSet.getResults().remove(cc);

//                Bound and index old cc
                new RecursiveBoundingTask(cc, 1, par, resultSet).invoke();
            }
        }
    }


}