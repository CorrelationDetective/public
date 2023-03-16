package algorithms.streaming.sdstream;

import _aux.lib;
import _aux.lists.FastArrayList;
import algorithms.streaming.SDOneShot;
import algorithms.streaming.StreamingAlgorithm;
import bounding.ClusterCombination;
import bounding.RecursiveBoundingTask;
import core.Parameters;
import lombok.Getter;
import queries.ResultSet;

import java.util.*;
import java.util.concurrent.*;

public class SDStream extends StreamingAlgorithm {
    //    Old statistics used for switching
    @Getter
    protected final SDOneShot sdOneShot;

    public SDStream(Parameters par) {
        super(par);
        this.sdOneShot = new SDOneShot(par);

        par.setActiveAlgorithm(this);
    }

    @Override
    public ResultSet run() {
        if (updatedTS.size() == 0) {
            return resultSet;
        }

//        Query index for all violated ClusterCombinations
        Set<ClusterCombination> vCCs = par.forkJoinPool.invoke(new QueryIndexTask(new ArrayList<>(updatedTS), 0, updatedTS.size(), par, oldDistances));

//        Update statistics
        par.statBag.getViolationCounts()[par.epoch-1] = vCCs.size();

        par.getLOGGER().fine("Number of violations: " + vCCs.size());

//        Handle all violations (i.e. rebound the CC)
        par.forkJoinPool.invoke(new HandleViolationTask(new ArrayList<>(vCCs), 0, vCCs.size(), par, resultSet));

        return resultSet;
    }
}
