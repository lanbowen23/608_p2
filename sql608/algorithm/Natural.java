package sql608.algorithm;

import sql608.helper.RelationIterator;
import sql608.helper.Write;
import storageManager.*;

import java.util.ArrayList;
import java.util.List;

public class Natural {
    // clear all blocks of Main Memory
    private static void clearMainMemory(MainMemory mainMemory) {
        int numberOfBlocks = mainMemory.getMemorySize();
        for (int i = 0; i < numberOfBlocks; i++) {
            mainMemory.getBlock(i).clear();
        }
    }

    /* compare twoTuples */
    public static int compare(Tuple o1, Tuple o2, String keyOne, String keyTwo) {
        Field fieldOne = o1.getField(keyOne);
        Field fieldTwo = o2.getField(keyTwo);
        if (fieldOne.type == FieldType.INT) {
            return fieldOne.integer - fieldTwo.integer;
        } else {
            return fieldOne.str.compareTo(fieldTwo.str);
        }
    }

    public static String join(String tableOne, String tableTwo, String condition,
                              MainMemory mainMemory, SchemaManager schemaManager) {
        /* course.sid = course2.sid */
        clearMainMemory(mainMemory);
        Relation relationOne = schemaManager.getRelation(tableOne);
        Relation relationTwo = schemaManager.getRelation(tableTwo);
        Schema schemaOne = schemaManager.getSchema(tableOne);
        Schema schemaTwo = schemaManager.getSchema(tableTwo);

        /* get the specific field to be sort and join on */
        String fieldOne = condition.split("=")[0].trim();
        if (!schemaOne.getFieldNames().contains(fieldOne) && fieldOne.contains(".")) {
            fieldOne = fieldOne.split("\\.")[1];
        }
        String fieldTwo = condition.split("=")[1].trim();
        if (!schemaTwo.getFieldNames().contains(fieldTwo) && fieldTwo.contains(".")) {
            fieldTwo = fieldTwo.split("\\.")[1];
        }

        /* create a temporary relation for joint table */
        Schema tempSchema = Join.twoSchema(schemaOne, schemaTwo, tableOne, tableTwo);
        String tempRelationName = tableOne + "naturalJoin" + tableTwo;
        Relation tempRelation = schemaManager.createRelation(tempRelationName, tempSchema);

        /* before natural join, we need to sort the joining Tables */
        ArrayList<String> sortFieldsOne = new ArrayList<>();
        ArrayList<String> sortFieldsTwo = new ArrayList<>();
        sortFieldsOne.add(fieldOne);
        sortFieldsTwo.add(fieldTwo);
        TwoPass.sort(relationOne, sortFieldsOne, mainMemory);
        TwoPass.sort(relationTwo, sortFieldsTwo, mainMemory);

        /* use iterator to get tuples of one block from each relation */
        /* which means only use one block to store the processing tuples from one relation */
        RelationIterator relationIteratorOne = new RelationIterator(relationOne, mainMemory, 0);
        RelationIterator relationIteratorTwo = new RelationIterator(relationTwo, mainMemory, 1);
        Tuple tupleOne = null;
        Tuple tupleTwo = null;
        boolean isBegin = true;
        while ((isBegin || (tupleOne != null && tupleTwo != null))) {
            if (isBegin) {
                tupleOne = relationIteratorOne.next();
                tupleTwo = relationIteratorTwo.next();
                isBegin = false;
            }
            /* find same tuple from two relations */
            int compare = compare(tupleOne, tupleTwo, fieldOne, fieldTwo);
            if (compare < 0) tupleOne = relationIteratorOne.next();
            else if (compare > 0) tupleTwo = relationIteratorTwo.next();
            else {
                List<Tuple> sameTupleListOne = new ArrayList<>();
                List<Tuple> sameTupleListTwo = new ArrayList<>();
                Tuple preTupleOne = tupleOne;
                Tuple preTupleTwo = tupleTwo;
                sameTupleListOne.add(preTupleOne);
                sameTupleListTwo.add(preTupleTwo);
                /* move tuple to the next which may still be same */
                /* skip the hole */
                while (tupleOne != null) {
                    tupleOne = relationIteratorOne.next();
                    if (tupleOne == null) break;
                    if (compare(preTupleOne, tupleOne, fieldOne, fieldOne) == 0) {
                        sameTupleListOne.add(tupleOne);
                    } else break;
                }
                while (tupleTwo != null) {
                    tupleTwo = relationIteratorTwo.next();
                    if (tupleTwo == null) break;
                    if (compare(preTupleTwo, tupleTwo, fieldTwo, fieldTwo) == 0) {
                        sameTupleListTwo.add(tupleTwo);
                    } else break;
                }

                for (Tuple sameTupleOne : sameTupleListOne) {
                    for (Tuple sameTupleTwo : sameTupleListTwo) {
                        Tuple joinedTuple = Join.twoTuples(sameTupleOne, sameTupleTwo,
                                tempRelationName, schemaManager);
                        if (joinedTuple == null) continue;
                        Write.tuple(joinedTuple, tempRelation, mainMemory,9);
                    }
                }
            }
        }
        return tempRelationName;
    }

}
