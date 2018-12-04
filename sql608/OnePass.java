package sql608;

import sql608.heap.TupleHeap;
import storageManager.Block;
import storageManager.MainMemory;
import storageManager.Tuple;

import java.util.ArrayList;
import java.util.HashSet;

public class OnePass {
    // clear all blocks of Main Memory
    private static void clearMainMemory(MainMemory mainMemory) {
        int numberOfBlocks = mainMemory.getMemorySize();
        for (int i = 0; i < numberOfBlocks; i++) {
            mainMemory.getBlock(i).clear();
        }
    }

    // all tuples can fit into main memory
    private static boolean validateOnePass(ArrayList<Tuple> selectedTuples) {
        int numberOfFieldsPerTuple = selectedTuples.get(0).getNumOfFields();
        int max_capacity = (8 / numberOfFieldsPerTuple) * mainMemory.getMemorySize();
        return max_capacity >= selectedTuples.size();
    }

    public static ArrayList<Tuple> duplicate(ArrayList<Tuple> selectedTuples,
                                             ArrayList<String> selectedFieldNamesList,
                                             MainMemory mainMemory) {
        // selected tuples < main memory
        if (!validateOnePass(selectedTuples)) return null;

        ArrayList<Tuple> result = new ArrayList<>();
        clearMainMemory(mainMemory);
        mainMemory.setTuples(0, selectedTuples);
        // use hash to eliminate duplicate
        HashSet<FieldTuple> set = new HashSet<>();
        int memoryNumberOfBlocks = mainMemory.getMemorySize();
        for (int i = 0; i < memoryNumberOfBlocks; i++) {
            Block block = mainMemory.getBlock(i);
            if (!block.isEmpty()) {
                for (Tuple tuple : block.getTuples()) {
                    FieldTuple uniqueTuple = new FieldTuple(tuple, selectedFieldNamesList);
                    if (set.add(uniqueTuple)) {
                        result.add(tuple);
                    }
                }
            }
        }
        return result;
    }

    // using heap to do the ordering
    public static ArrayList<Tuple> onePassOrder(ArrayList<Tuple> selectedTuples,
                                                ArrayList<String> sortFields,
                                                MainMemory mainMemory) {
        if (!validateOnePass(selectedTuples)) return null;
        clearMainMemory(mainMemory);
        mainMemory.setTuples(0, selectedTuples);
        ArrayList<Tuple> result = new ArrayList<>();
        // set up a heap to get ordered tuples
        TupleHeap heap = buildHeap(sortFields);
        offerTuplesFromMemToHeap(heap);
        while (!heap.isEmpty()) result.add(heap.poll());
        return result;
    }

}
