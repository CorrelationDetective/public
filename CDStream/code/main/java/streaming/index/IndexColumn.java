package streaming.index;

import _aux.lists.FastLinkedList;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;

@RequiredArgsConstructor
public class IndexColumn {
    @NonNull @Getter private HashMap<ExtremaPair, ExtremaPairGroup> extremaPairGroups = new HashMap();

    public ExtremaPairGroup getOrAdd(ExtremaPair extremaPair){
        ExtremaPairGroup extremaPairGroup = extremaPairGroups.get(extremaPair);
        if (extremaPairGroup == null){
            extremaPairGroup = new ExtremaPairGroup(extremaPair);
            synchronized (extremaPairGroups){
                extremaPairGroups.put(extremaPair, extremaPairGroup);
            }
        }
        return extremaPairGroup;
    }

    public String toString(){
        return String.format("size = %d", extremaPairGroups.size());
    }

    public int size(){
        return extremaPairGroups.size();
    }
}
