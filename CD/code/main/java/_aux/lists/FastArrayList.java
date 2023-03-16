package _aux.lists;

import lombok.NonNull;

import java.util.*;
import java.util.stream.Stream;

// only implement fast methods to force developers to use right data structure
public class FastArrayList<T> implements Iterable<T> {
    private ArrayList<T> list;
    @NonNull private final int capacity;

    public FastArrayList(int capacity){
        this.capacity = capacity;
        list = new ArrayList<>(capacity);
    }

    public FastArrayList(FastArrayList<T> newList){
        this.capacity = newList.size();
        list = new ArrayList<>(newList.toList());
    }

    public FastArrayList(FastLinkedList<T> newList){
        this.capacity = newList.size();
        list = new ArrayList<>(newList.toList());
    }

    public FastArrayList(List<T> newList){
        this.capacity = newList.size();
        list = new ArrayList<>(newList);
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
        if (list.size() == capacity){
            throw new RuntimeException("FastArrayList is full");
        }
        list.add(item);
    }

    public void add(int index, T item){
        if (list.size() == capacity){
            throw new RuntimeException("FastArrayList is full");
        }
        list.add(index, item);
    }

    public void addAll(FastArrayList<T> items){
        if (list.size() == capacity && items.size() > 0){
            throw new RuntimeException("FastArrayList is full");
        }
        list.addAll(items.list);
    }

    public T remove(int index){
        if (index >= list.size()){
            throw new RuntimeException("Index out of bounds");
        }
        return list.remove(index);
    }

    public T remove(T item){
        if (!list.contains(item)){
            throw new RuntimeException("Item not in list");
        }
        return list.remove(list.indexOf(item));
    }

    public T get(int index){
        return list.get(index);
    }

    public boolean contains(T item){
        return list.contains(item);
    }

    public void sort(Comparator<T> comparator){
        list.sort(comparator);
    }

    public void shuffle(Random random){
        Collections.shuffle(list, random);
    }

    public boolean equals(FastArrayList<T> other){
        return list.equals(other.list);
    }

}
