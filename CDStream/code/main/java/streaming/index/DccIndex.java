package streaming.index;

import _aux.lists.FastLinkedList;

import java.util.HashMap;

public class DccIndex {
    /**
     * index:                   n x IndexColumn
     * IndexColumn:             HashMap<ExtremaPairGroup>
     * ExtremaPairGroup:     ExtremaPair, Treemap<ClusterGroup>
     * ClusterGroup:         Cluster, PositiveDCCs, NegativeDCCs, MinimumJumpDCCs
     */

    public IndexColumn[] indexColumns;

    public DccIndex(int n){
        indexColumns = new IndexColumn[n];
        for (int i = 0; i < n; i++) {
            indexColumns[i] = new IndexColumn();
        }
    }
}
