package sql608.algorithm;

import sql608.helper.Fields;
import sql608.Heap.HeapFunc;
import sql608.Heap.TupleHeap;
import sql608.Heap.TupleHeap2;
import sql608.Heap.TupleWithDiskId;
import sql608.helper.Write;
import storageManager.*;

import java.util.ArrayList;
import java.util.Map;

public class TwoPass {
    // clear all blocks of Main Memory
    private static void clearMainMemory(MainMemory mainMemory) {
        int numberOfBlocks = mainMemory.getMemorySize();
        for (int i = 0; i < numberOfBlocks; i++) {
            mainMemory.getBlock(i).clear();
        }
    }

    /* two pass merge sort algorithm */
    public static void sort(Relation relation, ArrayList<String> sortFields,
                            MainMemory mainMemory) {
        int numMemBlocks = mainMemory.getMemorySize();
        int numRelationBlocks = relation.getNumOfBlocks();
        int numSublists = (numRelationBlocks % numMemBlocks == 0) ?
                numRelationBlocks / numMemBlocks : numRelationBlocks / numMemBlocks + 1;

        /*
        get sublists of the relation, sort in memory and write back to disk
        i represent sublist id, j represent mem block id
        */
        for (int i = 0; i < numSublists; i++) {
            clearMainMemory(mainMemory);
            // get in main memory one sublist of relation (10 blocks) from disk
            for (int j = 0; j < numMemBlocks; j++) {
                int curRelationInd = i * numMemBlocks + j;
                if (curRelationInd >= numRelationBlocks) break;
                relation.getBlock(curRelationInd, j);  // read in main memory
            }
            // build a Heap with comparator based on fields
            TupleHeap Heap1 = HeapFunc.build(sortFields);
            // push all Tuples in the main memory into Heap
            HeapFunc.offer(Heap1, mainMemory);

            clearMainMemory(mainMemory);
            int memBlockIndex = 0;
            // pull Tuples out from Heap and input to main memory one by one until main memory full
            while (!Heap1.isEmpty()) {
                if (mainMemory.getBlock(memBlockIndex).isFull()) memBlockIndex++;
                Block memBlock = mainMemory.getBlock(memBlockIndex);
                memBlock.appendTuple(Heap1.poll());
            }
            // output main memory (sorted) back into disk which is a sorted list
            // following the end of the relation (num of relation blocks)
            for (int j = 0; j < numMemBlocks; j++) {
                int diskBlockInd = numRelationBlocks + i * numMemBlocks + j;
                relation.setBlock(diskBlockInd, j);
            }
        }

        /*
        merge sublists in the disk
        by read the head of each sublist back to main mem
        record remaining tuples for each sublist in the main memory
        */
        int[] tuplesRemain = new int[numSublists];
        TupleHeap2 Heap2 = HeapFunc.build2(sortFields);
        /* init: read the head of each sublists and offer into heap2 */
        for (int i = 0; i < numSublists; i++) {
            /* diskListId: start block id of each sublist in the disk */
            int diskListId = numRelationBlocks + i * numMemBlocks;
            mainMemory.getBlock(0).clear();
            relation.getBlock(diskListId, 0);
            Block block = mainMemory.getBlock(0);
            tuplesRemain[i] = block.getNumTuples();
            if (block.getNumTuples() <= 0) continue;
            for (Tuple tuple : block.getTuples()) {
                if (tuple.isNull()) continue;
                Heap2.offer(new TupleWithDiskId(tuple, diskListId));
            } }

        // use Heap to fill memory
        clearMainMemory(mainMemory);
        int blockOne = 0;  // read in next tuple from disk and offer to Heap
        int blockTwo = 1;  // store the tuple poll from Heap in main mem
        int curDiskBlockId = 0;  // write back from the start of disk of original relation
        boolean isLastDiskBlockFull = false;

        while (!Heap2.isEmpty()) {
            TupleWithDiskId tuple2 = Heap2.poll();
            // store polled tuple in the mem block 1
            mainMemory.getBlock(blockTwo).appendTuple(tuple2.tuple);

            int diskId = tuple2.diskId; // start of block index for one sublist
            int sublistId = (diskId - numRelationBlocks) / numMemBlocks;
            tuplesRemain[sublistId]--;
            /*
            if we remaining Tuples of sublist in the main mem is 0,
            we add the next block of the sublist to main mem 0 if there is one.
            then into Heap2
            */
            if (tuplesRemain[sublistId] == 0) {
                // not end of the sublist
                if ((diskId - numRelationBlocks) % numMemBlocks != 9
                        && diskId + 1 < numRelationBlocks * 2) {
                    mainMemory.getBlock(blockOne).clear();
                    relation.getBlock(diskId + 1, blockOne);
                    Block curMemBlock = mainMemory.getBlock(blockOne);
                    if (!curMemBlock.isEmpty() && curMemBlock.getNumTuples() > 0) {
                        // read next block into memory
                        tuplesRemain[sublistId] = curMemBlock.getNumTuples();
                        for (Tuple tuple : curMemBlock.getTuples()) {
                            if (tuple.isNull()) {
                                tuplesRemain[sublistId]--;
                                continue;
                            }
                            Heap2.offer(new TupleWithDiskId(tuple, diskId + 1));
                        }
                    }
                }
            }

            isLastDiskBlockFull = false;
            if (mainMemory.getBlock(blockTwo).isFull()) {
                relation.setBlock(curDiskBlockId, blockTwo);
                curDiskBlockId++;
                mainMemory.getBlock(blockTwo).clear();
                isLastDiskBlockFull = true;  // mem block is full, so written disk block is full
            }
        }

        /* delete helper sublist, start from the last used block */
        if (isLastDiskBlockFull) {
            relation.deleteBlocks(curDiskBlockId);
        } else {
            relation.deleteBlocks(curDiskBlockId + 1);
        }
    }

    public static String join(String tableOne, String tableTwo, boolean isDistinct,
                              Map<String, ArrayList<String>> tableAttrMap,
                              MainMemory mainMemory, SchemaManager schemaManager) {
        Relation relationOne = schemaManager.getRelation(tableOne);
        Relation relationTwo = schemaManager.getRelation(tableTwo);

        Schema tempSchema = Join.twoSchema(relationOne.getSchema(), relationTwo.getSchema(), tableOne, tableTwo);
        String tempRelationName = tableOne + "Join" + tableTwo;
        Relation tempRelation = schemaManager.createRelation(tempRelationName, tempSchema);

        /*
        use 2-9 main mem to read relation one
        1 for read relation two, 0 for joined and write back
        */
        int memPosOne = 2;
        int memPosTwo = 1;
        int memTempPos = 0;
        int memSize = mainMemory.getMemorySize();
        int numGroups = relationOne.getNumOfBlocks() / (memSize - 2);
        Fields preFieldsOne = null;
        Fields preFieldsTwo = null;
        /* loop over group(8 blocks) of relation one */
        for (int i = 0; i <= numGroups; i++) {
            if (i * numGroups >= relationOne.getNumOfBlocks()) break;
            clearMainMemory(mainMemory);
            relationOne.getBlocks(i * numGroups, memPosOne,
                    Math.min(memSize - 2, relationOne.getNumOfBlocks()-i*numGroups));
            preFieldsTwo = null;
            /* loop over all relation two blocks */
            for (int j = 0; j < relationTwo.getNumOfBlocks(); j++) {
                mainMemory.getBlock(memPosTwo).clear();
                relationTwo.getBlock(j, memPosTwo);
                if (mainMemory.getBlock(memPosTwo).isEmpty()) continue;
                if (j != 0) preFieldsOne = null;
                for (int k = memPosOne; k < memSize; k++) {
                    if (mainMemory.getBlock(k).isEmpty()) continue;
                    /* join two tuples */
                    for (Tuple tupleOne : mainMemory.getBlock(k).getTuples()) {
                        if (tupleOne.isNull()) continue;
                        /* duplicates elimination */
                        if (isDistinct && !tableOne.contains("Join")) {
                            Fields curFieldsOne = new Fields(tableAttrMap.get(tableOne), tupleOne);
                            if (preFieldsOne != null && preFieldsOne.equals(curFieldsOne)) continue;
                            preFieldsOne = curFieldsOne;
                        }
                        for (Tuple tupleTwo : mainMemory.getBlock(memPosTwo).getTuples()) {
                            if (tupleTwo.isNull()) continue;
                            /* duplicates elimination */
                            if (isDistinct && !tableTwo.contains("Join")) {
                                Fields curFieldsTwo = new Fields(tableAttrMap.get(tableTwo), tupleTwo);
                                if (preFieldsTwo != null && preFieldsTwo.equals(curFieldsTwo)) continue;
                                preFieldsTwo = curFieldsTwo;
                            }
                            Tuple joinedTuple = Join.twoTuples(tupleOne, tupleTwo, tempRelationName, schemaManager);
                            if (joinedTuple != null) {
                                mainMemory.getBlock(memTempPos).clear();
                                Write.tuple(joinedTuple, tempRelation, mainMemory, memTempPos);
                            }
                        }
                    }
                }
            }
        }
        clearMainMemory(mainMemory);
        return tempRelationName;
    }
}
