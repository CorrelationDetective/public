package _aux.lists;


import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Stream;

// only implement fast methods to force developers to use right data structure
public class FastLinkedList<T> implements Iterable<T>{
    private LinkedList<T> list;

    public FastLinkedList(){
        list = new LinkedList<>();
    }

    public FastLinkedList(FastLinkedList<T> newList){
        list = new LinkedList<>(newList.toList());
    }

    public FastLinkedList(FastArrayList<T> newList){
        list = new LinkedList<>(newList.toList());
    }

    public FastLinkedList(List<T> newList){
        list = new LinkedList<>(newList);
    }

    public FastLinkedList(Queue<T> queue){
        list = new LinkedList<>(queue);
    }

    //    Standard methods

    public int hashCode(){
        return list.hashCode();
    }

    public String toString(){ return String.format("size = %d", list.size()); }

    public int size(){
        return list.size();
    }

    public boolean isEmpty(){
        return list.isEmpty();
    }

    public void clear(){
        list.clear();
    }

    //    Iterable interface
    public Iterator<T> iterator(){
        return list.iterator();
    }

    public Stream<T> stream(){
        return list.stream();
    }
    public List<T> toList(){
        return list;
    }

    public T[] toArray(T[] a){
        return list.toArray(a);
    }

//    Specific methods
    public void add(T item){
        list.add(item);
    }

    public void addAll(FastLinkedList<T> items){
        while (!items.isEmpty()){
            list.add(items.removeFirst());
        }
    }

    public T poll(){
        return ((LinkedList<T>) list).poll();
    }

    public T getFirst(){
        return ((LinkedList<T>) list).getFirst();
    }

    public T removeFirst(){
        return ((LinkedList<T>) list).removeFirst();
    }

    public void addFirst(T item){
        ((LinkedList<T>) list).addFirst(item);
    }

    public T getLast(){
        return ((LinkedList<T>) list).getLast();
    }

    public T removeLast(){
        return ((LinkedList<T>) list).removeLast();
    }

    public boolean equals(FastLinkedList<T> other){
        return list.equals(other.list);
    }
}
