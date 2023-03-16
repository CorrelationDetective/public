package queries;

import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.stream.Collectors;

@RequiredArgsConstructor
public class ResultTuple implements ResultObject {
    @NonNull @Getter public FastArrayList<Integer> LHS;
    @NonNull @Getter public FastArrayList<Integer> RHS;
    @NonNull @Getter public FastArrayList<String> lHeaders;
    @NonNull @Getter public FastArrayList<String> rHeaders;
    @NonNull @Getter public double similarity;
    @NonNull @Getter public long timestamp;

    private boolean sorted = false;

    public String toString() {
        return String.format("%s | %s -> %.3f",
                LHS.stream().map(Object::toString).collect(Collectors.joining("-")),
                RHS.stream().map(Object::toString).collect(Collectors.joining("-")),
                similarity
        );
    }

    public int size(){
        return LHS.size() + RHS.size();
    }

    public void sortSides(){
        if (!sorted) {
            LHS.sort(Integer::compareTo);
            RHS.sort(Integer::compareTo);
            sorted = true;
        }
    }


    @Override
    public boolean equals(Object other){
        if (other == null) return false;
        ResultTuple otherTuple = (ResultTuple) other;

//        Make sure sides are same size
        if (LHS.size() != otherTuple.LHS.size() || RHS.size() != otherTuple.RHS.size()){
            return false;
        }

        boolean equalSides = LHS.size() == RHS.size();

//        Sort sides of both tuples
        this.sortSides();
        otherTuple.sortSides();

        // check LHS
        for (int i = 0; i < LHS.size(); i++) {
            if (!LHS.get(i).equals(otherTuple.LHS.get(i))){
                return false;
            }
        }

        // check RHS
        for (int i = 0; i < RHS.size(); i++) {
            if (!RHS.get(i).equals(otherTuple.RHS.get(i))){
                return false;
            }
        }
        return true;
    }

    public int compareTo(ResultTuple other){
        if (other == null) return 1;
        if (similarity > other.similarity) return 1;
        if (similarity < other.similarity) return -1;
        return 0;
    }

    public FastLinkedList<ResultTuple> getSubsets(){
        FastLinkedList<ResultTuple> subsets = new FastLinkedList<>();
        for (int i = 0; i < LHS.size(); i++) {
            FastArrayList<Integer> newLHS = new FastArrayList<>(LHS);
            newLHS.remove(i);
            subsets.add(new ResultTuple(newLHS, RHS, lHeaders, rHeaders, similarity, 0));
        }
        for (int i = 0; i < RHS.size(); i++) {
            FastArrayList<Integer> newRHS = new FastArrayList<>(RHS);
            newRHS.remove(i);
            subsets.add(new ResultTuple(LHS, newRHS, lHeaders, rHeaders, similarity, 0));
        }
        return subsets;
    }
}
