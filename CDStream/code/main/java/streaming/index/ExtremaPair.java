package streaming.index;

import clustering.Cluster;
import lombok.NonNull;

public class ExtremaPair {
    @NonNull public final int id1;
    @NonNull public final int id2;
    public ExtremaPair(int id1, int id2) {
        this.id1 = Math.min(id1, id2);
        this.id2 = Math.max(id1, id2);
    }

    public String toString(){
        return String.format("<%d,%d>", id1, id2);
    }

    public boolean contains(int id){
        return id1 == id || id2 == id;
    }

    public int hashCode(){
        int result = id1;
        result = 31 * result + id2;
        return result;
    }

    public boolean equals(Object o){
        if (this == o) return true;
        if (!(o instanceof ExtremaPair)) return false;
        ExtremaPair ep = (ExtremaPair) o;
        return id1 == ep.id1 && id2 == ep.id2;
    }


}
