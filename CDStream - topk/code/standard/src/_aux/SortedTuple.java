package _aux;


public class SortedTuple<T1 extends Comparable<T1>, T2 extends Comparable<T2>> implements Comparable<SortedTuple<T1,T2>> {
    T1 key;
    T2 value;

    public SortedTuple(T1 key, T2 value) {
        this.key=key;
        this.value=value;
    }

    public int compareTo(SortedTuple<T1,T2> o) {
        return this.key.compareTo(o.key);
    }
    public String toString() {
        return "" + key.toString() + "," + value.toString();
    }
}
