package algorithms;

import _aux.*;
import _aux.lists.FastArrayList;
import core.Parameters;
import queries.ResultSet;
import queries.ResultTuple;

import java.util.ArrayList;

public abstract class Algorithm {
    public Parameters par;

    public Algorithm(Parameters parameters){
        this.par = parameters;
    }

    public abstract ResultSet run();
    public abstract void printStats(StatBag statBag);
    public abstract void prepareStats();

    public void printStageDurations(StatBag statBag){
        lib.printBar(par.LOGGER);
        int i = 0;
        for (Stage stageDuration: statBag.stageDurations){
            if (stageDuration.expectedDuration != null){
                par.LOGGER.fine(String.format("Duration stage %d. %-50s: %.5f sec (estimated %.5f sec)",
                        i, stageDuration.name, stageDuration.duration, stageDuration.expectedDuration));
            } else {
                par.LOGGER.fine(String.format("Duration stage %d. %-50s: %.5f sec",
                        i, stageDuration.name, stageDuration.duration));
            }
            i++;
        }
        par.LOGGER.info(String.format("%-68s: %.5f sec", "Total duration", statBag.totalDuration));
    }


}
