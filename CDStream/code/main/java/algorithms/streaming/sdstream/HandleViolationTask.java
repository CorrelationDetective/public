package algorithms.streaming.sdstream;

import _aux.lists.FastArrayList;
import bounding.ClusterCombination;
import bounding.RecursiveBoundingTask;
import core.Parameters;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import queries.ResultSet;
import streaming.TimeSeries;
import streaming.index.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.*;
import java.util.concurrent.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class HandleViolationTask extends RecursiveAction {

    @NonNull private final ArrayList<ClusterCombination> violationCCs;
    @NonNull private final int start;
    @NonNull private final int end;
    @NonNull private final Parameters par;
    @NonNull private final ResultSet resultSet;


    @Override
    protected void compute() {
//        Check if updatedTs is small enough to be processed by this thread alone, otherwise break into subtasks
        if (end - start < 200 | !par.parallel){
            for (int i = start; i < end; i++) {
                ClusterCombination vCC = violationCCs.get(i);
                handleViolation(vCC);
            }
        } else { // divide and conquer -- break updatedTS into 2
            int mid = (start + end) / 2;
            HandleViolationTask leftTask = new HandleViolationTask(violationCCs, start, mid, par, resultSet);
            HandleViolationTask rightTask = new HandleViolationTask(violationCCs, mid, end, par, resultSet);

//            Invoke subtasks
            ForkJoinTask.invokeAll(List.of(leftTask, rightTask));
            leftTask.join();
            rightTask.join();
        }
    }

    private void handleViolation(ClusterCombination vCC){
        //            Set as indecisive and negative (default state) and remove from resultSet if positive
        if (vCC.isPositive()){
            resultSet.remove(vCC);
        }

//            Clone and Kill DCC to make sure it is filtered out in the future
        ClusterCombination ccClone = vCC.clone();
        vCC.kill();
        par.statBag.addToStat(par.statBag.getNIndexedDCCs(), () -> -1);
        par.statBag.addToStat(par.statBag.getTotalIndexedDCCsSize(), () -> -1 * vCC.size());

        new RecursiveBoundingTask(ccClone, 1, par, resultSet).invoke();
    }
}
