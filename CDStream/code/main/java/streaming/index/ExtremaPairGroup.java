package streaming.index;

import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import clustering.Cluster;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.TreeMap;

@RequiredArgsConstructor
public class ExtremaPairGroup {
    @NonNull public ExtremaPair extremaPair;

//    Keyed and Sorted by cluster size (descending)
    @Getter private TreeMap<Cluster, ClusterGroup> clusterGroups = new TreeMap<>(Comparator.comparingInt(Cluster::getSize).reversed());

    public ClusterGroup getOrAdd(Cluster cluster){
        ClusterGroup clusterGroup = clusterGroups.get(cluster);
        if (clusterGroup == null){
            clusterGroup = new ClusterGroup(cluster);
            synchronized (clusterGroups){
                clusterGroups.put(cluster, clusterGroup);
            }
        }
        return clusterGroup;
    }

    public void remove(ClusterGroup cg){
        clusterGroups.remove(cg.cluster);
    }

    public void removeAll(FastArrayList<ClusterGroup> cgs){
        for (ClusterGroup cg : cgs) {
            clusterGroups.remove(cg.cluster);
        }
    }

    @Override
    public boolean equals(Object o){
        if (this == o) return true;
        if (!(o instanceof ExtremaPairGroup)) return false;
        ExtremaPairGroup other = (ExtremaPairGroup) o;
        return extremaPair.equals(other.extremaPair);
    }

    @Override
    public int hashCode(){
        return extremaPair.hashCode();
    }

    public String toString(){
        return String.format("size = %d", clusterGroups.size());
    }

    public int size(){
        return clusterGroups.size();
    }
}
