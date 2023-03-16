package algorithms.streaming.sdstream;

import _aux.lists.FastArrayList;
import bounding.ClusterCombination;
import core.Parameters;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import streaming.TimeSeries;
import streaming.index.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class QueryIndexTask extends RecursiveTask<Set<ClusterCombination>> {

    @NonNull private final ArrayList<TimeSeries> updatedTS;
    @NonNull private final int start;
    @NonNull private final int end;
    @NonNull private final Parameters par;
    @NonNull private final double[][] oldDistances;

    @Override
    protected Set<ClusterCombination> compute() {
//        Check if updatedTs is small enough to be processed by this thread alone, otherwise break into subtasks
        if (!par.parallel | (updatedTS.size() > 0 && end - start == 1)){
            Set<ClusterCombination> violations = new HashSet<>();
            for (int i = start; i < end; i++) {
                TimeSeries ts = updatedTS.get(i);
                violations.addAll(queryFullIndex(ts));
            }
            return violations;
        } else { // divide and conquer -- break updatedTS into 2
            int mid = (start + end) / 2;
            QueryIndexTask leftTask = new QueryIndexTask(updatedTS, start, mid, par, oldDistances);
            QueryIndexTask rightTask = new QueryIndexTask(updatedTS, mid, end, par, oldDistances);

//            Combine sets of queried CCs
            return ForkJoinTask.invokeAll(List.of(leftTask, rightTask))
                    .stream()
                    .map(ForkJoinTask::join)
                    .flatMap(Set::stream)
                    .collect(Collectors.toSet());
        }
    }

    public Set<ClusterCombination> queryFullIndex(TimeSeries ts){
        Set<ClusterCombination> violations = querySingleIndex(ts, par.lbIndex, false);
        violations.addAll(querySingleIndex(ts, par.ubIndex, true));
        return violations;
    }

    private Set<ClusterCombination> querySingleIndex(TimeSeries ts, DccIndex index, boolean queryUB){
        int tid = ts.id;
        IndexColumn indexColumn = index.indexColumns[tid];

//        Iterate over all extrema pair groups and find violations
        Set<ClusterCombination> violations = new HashSet<>();

        for (ExtremaPairGroup extremaPairGroup: indexColumn.getExtremaPairGroups().values()){
            ExtremaPair ep = extremaPairGroup.extremaPair;

//                    Get the distance we should not exceed (tid may participate in the pair)
            double thDist = ep.contains(tid) ? oldDistances[ep.id1][ep.id2] : par.pairwiseDistances[ep.id1][ep.id2];

//                    Cluster groups to remove after the iteration
            FastArrayList<ClusterGroup> cgToRemove = new FastArrayList<>(extremaPairGroup.size());

//                    Iterate over the clusters (in size descending order) until we find a non-violation
            for (ClusterGroup cg: extremaPairGroup.getClusterGroups().values()){
//                  Find min/max similarity pair with other cluster
                    double extremeDist = queryUB ? Double.MIN_VALUE : Double.MAX_VALUE;

                    for (Integer otherTid : cg.cluster.pointsIdx){
                        if (otherTid == tid) continue;

                        double dist = par.pairwiseDistances[tid][otherTid];
                        if (queryUB ? dist > extremeDist : dist < extremeDist){
                            extremeDist = dist;
                        }
                    }

//                            Check if the extreme similarity is above/below the threshold
                    if (queryUB ? extremeDist > thDist : extremeDist < thDist) {
//                            Lazily filter out all dead DCCs (indecisive or positive while negative)
                        cg.filterOutDeadDCCs();

//                            Add all DCCs to violations
                        violations.addAll(cg.getPositiveDCCs());
                        violations.addAll(cg.getNegativeDCCs());

//                            Remove clusterGroup from index (to avoid duplicate violations)
                        cgToRemove.add(cg);
                    } else {
//                        Non-violation -- all other cluster groups will also be non-violations
                        break;
                    }
            }

//          Remove all clusterGroups that were found to be violated
            extremaPairGroup.removeAll(cgToRemove);
        }

        return violations;
    }
}
