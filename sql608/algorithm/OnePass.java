package sql608.algorithm;

import sql608.helper.Fields;
import sql608.Heap.HeapFunc;
import sql608.Heap.TupleHeap;
import sql608.helper.Write;
import storageManager.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OnePass {
    // clear all blocks of Main Memory
    private static void clearMainMemory(MainMemory mainMemory) {
        int numberOfBlocks = mainMemory.getMemorySize();
        for (int i = 0; i < numberOfBlocks; i++) {
            mainMemory.getBlock(i).clear();
        }
    }

    public static void sort(Relation relation, ArrayList<String> sortFields,
                            MainMemory mainMemory) {
        clearMainMemory(mainMemory);
        /* read all blocks in to main mem */
        int numBlocks = relation.getNumOfBlocks();
        relation.getBlocks(0, 0, numBlocks);

        TupleHeap heap = HeapFunc.build(sortFields);
        HeapFunc.offer(heap, mainMemory);

        clearMainMemory(mainMemory);
        int curBlockId = 0;
        boolean isLastDiskBlockFull = false;

        while (!heap.isEmpty()) {
            isLastDiskBlockFull = false;
            mainMemory.getBlock(curBlockId).appendTuple(heap.poll());
            if (mainMemory.getBlock(curBlockId).isFull()) {
                isLastDiskBlockFull = true;
                curBlockId++;
            }
        }
        if (isLastDiskBlockFull) {
            relation.setBlocks(0, 0, curBlockId);
        } else {
            relation.setBlocks(0, 0, curBlockId + 1);
        }
    }

    /* Doing DISTINCT when joining */
    public static String crossJoin(String tableOne, String tableTwo, boolean isDistinct,
                                   Map<String, ArrayList<String>> tableAttrMap,
                                   MainMemory mainMemory, SchemaManager schemaManager) {
        Relation relationOne = schemaManager.getRelation(tableOne);
        Relation relationTwo = schemaManager.getRelation(tableTwo);
        int tableOneSize = relationOne.getNumOfBlocks();
        int tableTwoSize = relationTwo.getNumOfBlocks();

        /* use smaller table to be the one in the main memory */
        Relation smallTable, largeTable;
        String smallTableName, largeTableName;
        if (tableOneSize < tableTwoSize) {
            smallTable = relationOne;
            largeTable = relationTwo;
            smallTableName = tableOne;
            largeTableName = tableTwo;
        } else {
            smallTable = relationTwo;
            largeTable = relationOne;
            smallTableName = tableTwo;
            largeTableName = tableOne;
        }

        /* prepare a temporary relation */
        Schema smallTableSchema = smallTable.getSchema();
        Schema largeTableSchema = largeTable.getSchema();
        Schema tempSchema = Join.twoSchema(smallTableSchema, largeTableSchema, smallTableName, largeTableName);
        String tempRelationName = smallTableName + "Join" + largeTableName;
        Relation tempRelation = schemaManager.createRelation(tempRelationName, tempSchema);

        clearMainMemory(mainMemory);
        /* First put all blocks of small table into main memory */
        smallTable.getBlocks(0, 0, smallTable.getNumOfBlocks());
        Fields preLargeTuple = null;

        /* loop one: process large table block by block */
        for (int i = 0; i < largeTable.getNumOfBlocks(); i++) {
            /* read a block from large table to main mem block 9 */
            mainMemory.getBlock(9).clear();
            largeTable.getBlock(i, 9);
            Block block = mainMemory.getBlock(9);

            // store all the tuple here, not using a memory block
            ArrayList<Tuple> joinedTuples = new ArrayList<>();

            /* start to join one block with all blocks in the main mem */
            /* loop over each tuple in the block of large table */
            for (Tuple tupleLargerTable : block.getTuples()) {
                if (tupleLargerTable.isNull()) continue;
                /* skip duplicates if equal to previous tuple
                (already sorted before join if isDistinct is true)*/
                if (isDistinct &&
                        !largeTableName.contains("Join")) {
                    Fields curCustomTuple = new Fields(tableAttrMap.get(largeTableName),
                            tupleLargerTable);
                    if (preLargeTuple != null && preLargeTuple.equals(curCustomTuple)) continue;
                    preLargeTuple = curCustomTuple;
                }

                Set<Fields> uniqueTupleSet = new HashSet<>();
                /* join with all tuples of smaller table */
                for (int j = 0; j < mainMemory.getMemorySize() - 1; j++) {
                    Block blockSmallerTable = mainMemory.getBlock(j);
                    if (!blockSmallerTable.isEmpty()) {
                        for (Tuple tupleSmallerTable : blockSmallerTable.getTuples()) {
                            if (tupleSmallerTable.isNull()) continue;
                            if (isDistinct) {
                                Fields tuple = new Fields(tupleSmallerTable.getSchema().getFieldNames(),
                                                           tupleSmallerTable);
                                if (uniqueTupleSet.contains(tuple)) continue;
                                uniqueTupleSet.add(tuple);
                            }
                            /* join two tuples */
                            Tuple joinedTuple = Join.twoTuples(tupleSmallerTable, tupleLargerTable,
                                    tempRelationName, schemaManager);
                            if (joinedTuple != null) joinedTuples.add(joinedTuple);
                        }
                    }
                }
            }
            /* finish join */

            /* write to disk */
            for (Tuple tuple : joinedTuples) {
                Write.tuple(tuple, tempRelation, mainMemory, 9);
            }
            mainMemory.getBlock(9).clear();

            /* Finish for one block of large table */
        }
        clearMainMemory(mainMemory);
        return tempRelationName;
    }

}
