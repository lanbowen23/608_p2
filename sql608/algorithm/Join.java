package sql608.algorithm;

import sql608.helper.Write;
import storageManager.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Join {
    // clear all blocks of Main Memory
    private static void clearMainMemory(MainMemory mainMemory) {
        int numberOfBlocks = mainMemory.getMemorySize();
        for (int i = 0; i < numberOfBlocks; i++) {
            mainMemory.getBlock(i).clear();
        }
    }

    /* joint the schema of two Relation */
    public static Schema twoSchema(Schema schemaOne, Schema schemaTwo, String tableOne, String tableTwo) {
        ArrayList<String> newFieldNames = new ArrayList<>();
        ArrayList<FieldType> newFieldTypes = new ArrayList<>();
        for (int i = 0; i < schemaOne.getNumOfFields(); i++) {
            String fieldName = schemaOne.getFieldName(i);
            if (!fieldName.contains("."))
                fieldName = tableOne + "." + fieldName;
            newFieldNames.add(fieldName);
            newFieldTypes.add(schemaOne.getFieldTypes().get(i));
        }
        for (int j = 0; j < schemaTwo.getNumOfFields(); j++) {
            String fieldName = schemaTwo.getFieldName(j);
            if (!fieldName.contains("."))
                fieldName = tableTwo + "." + fieldName;
            newFieldNames.add(fieldName);
            newFieldTypes.add(schemaTwo.getFieldTypes().get(j));
        }
        return new Schema(newFieldNames, newFieldTypes);
    }

    /* return the joined Tuple */
    public static Tuple twoTuples(Tuple tupleOne, Tuple tupleTwo, String tmpRelation,
                                  SchemaManager schemaManager) {
        /* The max fields of a block is 8 */
        if (tupleOne.getSchema().getNumOfFields() + tupleTwo.getSchema().getNumOfFields() > 8) {
            return null;
        }
        if (tupleOne.isNull() || tupleTwo.isNull()) return null;

        /* Set all the fields for the joint tuple */
        Relation tempRelation = schemaManager.getRelation(tmpRelation);
        Tuple jointTuple = tempRelation.createTuple();
        int i = 0;  // index for tuple field
        while (i < tupleOne.getNumOfFields()) {
            Field field = tupleOne.getField(i);
            if (field.type == FieldType.INT) jointTuple.setField(i, field.integer);
            else jointTuple.setField(i, field.str);
            i++;
        }
        /* follow the num of fields of the first tuple, set next fields */
        while (i < tupleOne.getNumOfFields() + tupleTwo.getNumOfFields()) {
            Field field = tupleTwo.getField(i - tupleOne.getNumOfFields());
            if (field.type == FieldType.INT) jointTuple.setField(i, field.integer);
            else jointTuple.setField(i, field.str);
            i++;
        }
        return jointTuple;
    }

    /* consider small relation to be joined first */
    private static int[] getJoinOrder(ArrayList<String> tableLists, SchemaManager schemaManager) {
        int[] joinOrder = null;
//        HashMap<Integer, Integer> tableToSize = new HashMap<>();
//        int numTable = tableLists.size();
//        for (int i =0; i<numTable; i++) {
//            int tableSize = schemaManager.getRelation(tableLists.get(i)).getNumOfTuples();
//            tableToSize.put(i, tableSize);
//        }
        List<int[]> orders = getAllOrders(tableLists.size());
        int minSize = Integer.MAX_VALUE;
        for (int[] order : orders) {
            int curSize = getTotalJoinSize(tableLists, order, schemaManager);
            if (curSize < minSize) {
                minSize = curSize;
                joinOrder = order;
            }
        }
        return joinOrder;
    }

    private static List<int[]> getAllOrders(int tableListSize) {
        List<int[]> result = new ArrayList<>();
        boolean[] used = new boolean[tableListSize];
        permute(result, 0, new int[tableListSize], used);
        return result;
    }

    private static void permute(List<int[]> result, int start, int[] arr, boolean[] used) {
        if (start == used.length) {
            result.add(arr.clone());
            return;
        }
        for (int i = 0; i < used.length; i++) {
            if (used[i]) continue;
            used[i] = true;
            arr[start] = i;
            permute(result, start + 1, arr, used);
            used[i] = false;
        }
    }

    private static int getTotalJoinSize(ArrayList<String> tableLists, int[] order,
                                        SchemaManager schemaManager) {
        int len = tableLists.size();
        int[] tableSize = new int[len];
        for (int i = 0; i < len; i++) {
            // get number of Tuples in the relation
            tableSize[i] = schemaManager.getRelation(tableLists.get(i)).getNumOfTuples();
        }
        int Cost = 0;
        int cost = tableSize[order[0]];
        for (int i = 1; i < order.length - 1; i++) {
            cost *= tableSize[order[i]];
            Cost += cost;
        }
        return Cost;
    }

    private static void sortRelation(Relation relation, ArrayList<String> sortFields,
                                     MainMemory mainMemory) {
        if (relation.getNumOfBlocks() <= mainMemory.getMemorySize()) {
            OnePass.sort(relation, sortFields, mainMemory);
        } else {
            TwoPass.sort(relation, sortFields, mainMemory);
        }
    }

    /* store all tuples of relation into a tuple List */
    private static void storeTuplesToList(ArrayList<Tuple> list, Relation relation,
                                          MainMemory mainMemory) {
        int numRelationBlocks = relation.getNumOfBlocks();
        int curDiskId = 0;
        do {
            /* read in numBlocks to main memory */
            int numBlocks = 10 > numRelationBlocks ? numRelationBlocks : 10;
            relation.getBlocks(curDiskId, 0, numBlocks);
            for (int i = 0; i < numBlocks; i++) {
                Block block = mainMemory.getBlock(i);
                /* pass the holes after deletion */
                if (block.getNumTuples() == 0) continue;
                list.addAll(block.getTuples()); // add all the Tuples inside this block
            }
            numRelationBlocks -= numBlocks;
            curDiskId += numBlocks;
        } while (numRelationBlocks > 0);
    }

    private static String crossJoinTwoTables(String tableOne, String tableTwo, boolean isDistinct,
                                             Map<String, ArrayList<String>> tableAttrMap,
                                             SchemaManager schemaManager, MainMemory mainMemory) {
        int mainMemorySize = mainMemory.getMemorySize();
        Relation relationOne = schemaManager.getRelation(tableOne);
        Relation relationTwo = schemaManager.getRelation(tableTwo);
        int tableOneSize = relationOne.getNumOfBlocks();
        int tableTwoSize = relationTwo.getNumOfBlocks();

        if (tableOneSize >= mainMemorySize && tableTwoSize >= mainMemorySize) {
            return TwoPass.join(tableOne, tableTwo, isDistinct, tableAttrMap,
                    mainMemory, schemaManager);
        }
        return OnePass.crossJoin(tableOne, tableTwo, isDistinct, tableAttrMap,
                mainMemory, schemaManager);
    }

    // Can do the sorting if necessary at first
    public static String crossJoinTables(ArrayList<String> tableLists, boolean isDistinct,
                                         Map<String, ArrayList<String>> tableAttrMap,
                                         MainMemory mainMemory, SchemaManager schemaManager) {
        clearMainMemory(mainMemory);
        if (tableLists.size() == 1 ) return tableLists.get(0);

        /* sort the table if has DISTINCT,
         do eliminate duplication when joining the tuple */
        if (isDistinct) {
            for (String table : tableLists) {
                if (tableAttrMap.containsKey(table)) {
                    sortRelation(schemaManager.getRelation(table), tableAttrMap.get(table),
                            mainMemory);
                }
            }
        }

        /* get the total size of all tables which going to be joined */
        int numTotalBlocks = 0;
        for (String table : tableLists) {
            int tableBlocks = schemaManager.getRelation(table).getNumOfBlocks();
            numTotalBlocks += tableBlocks;
        }

        /*
        if total size of all tables is smaller than memory
        then the join operation can be done in main mem
        actually the t12345 in the "test.txt" satisfy this
        */
        if (numTotalBlocks <= mainMemory.getMemorySize()) {
            /* temporary joint table name */
            String tmpName = tableLists.get(0);
            Relation relationOne = schemaManager.getRelation(tmpName);
            Schema tmpSchema = relationOne.getSchema();

            /* tables to all its Tuples map, For future read and join */
            Map<String, ArrayList<Tuple>> tableToTuples = new HashMap<>();
            for (String table : tableLists) {
                tableToTuples.put(table, new ArrayList<>());
                storeTuplesToList(tableToTuples.get(table), schemaManager.getRelation(table),
                                  mainMemory);
            }

            List<String> relationsToDelete = new ArrayList<>();
            /* continue join with next table */
            for (int i = 1; i < tableLists.size(); i++) {
                Schema joinTmpSchema = twoSchema(tmpSchema, schemaManager.getSchema(tableLists.get(i)),
                                                 tmpName, tableLists.get(i));
                String joinTmpName = tmpName + "Join" + tableLists.get(i);
                schemaManager.createRelation(joinTmpName, joinTmpSchema);
                tableToTuples.put(joinTmpName, new ArrayList<>());
                /* Two loops to join the two tables and store all tuples in tableMap */
                for (Tuple tupleOne : tableToTuples.get(tmpName)) {
                    if (tupleOne == null || tupleOne.isNull()) continue;
                    for (Tuple tupleTwo : tableToTuples.get(tableLists.get(i))) {
                        if (tupleTwo == null || tupleTwo.isNull()) continue;
                        Tuple jointTuple = twoTuples(tupleOne, tupleTwo, joinTmpName, schemaManager);
                        tableToTuples.get(joinTmpName).add(jointTuple);
                    }
                }

                // tmpName is used to join with next table in the tableList
                tmpName = joinTmpName;
                tmpSchema = joinTmpSchema;
                relationsToDelete.add(tmpName);
            }
            for (int i = 0; i < relationsToDelete.size() - 1; i++) {
                schemaManager.deleteRelation(relationsToDelete.get(i));
            }
            for (Tuple tuple : tableToTuples.get(tmpName)) {
                Write.tuple(tuple, schemaManager.getRelation(tmpName), mainMemory, 5);
            }
            return tmpName;
        }

        /*
        total size of tables can not fit in memory
        Need to find the best join order for crossJoin first to reduce internal size
        */
        int[] joinOrder = getJoinOrder(tableLists, schemaManager);
        ArrayList<String> tempTables = new ArrayList<>();

        /* join with next table one by one */
        String tempTableOne = tableLists.get(joinOrder[0]);
        for (int i = 1; i < tableLists.size(); i++) {
            String tempTableTwo = tableLists.get(joinOrder[i]);
            tempTableOne = crossJoinTwoTables(tempTableOne, tempTableTwo,
                            isDistinct, tableAttrMap, schemaManager, mainMemory);
            if (i != tableLists.size() - 1) tempTables.add(tempTableOne);
        }

        for (String t : tempTables) schemaManager.deleteRelation(t);
        return tempTableOne;
    }

}
