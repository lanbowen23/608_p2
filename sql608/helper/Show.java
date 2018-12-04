package sql608.helper;

import sql608.ExpressionTree;
import sql608.ParserContainer;
import storageManager.Block;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Tuple;

import java.util.ArrayList;

// Show the results to the interface for selection methods
public class Show {
    // able to check DISTINCT with sorted relation
    public static void tuples(ParserContainer parserContainer, Relation relation,
                              ArrayList<String> fieldNames, MainMemory mainMemory) {
        int numOfRelationBlocks = relation.getNumOfBlocks();
        int numOfRows = 0;
        String prev = "nullPrev";
        // write down the head
        if (parserContainer.getTable() != null
                && !parserContainer.getTable().contains("CrossJoin")
                && !parserContainer.getTable().contains("naturalJoin"))
        {
            for (int i = 0; i < fieldNames.size(); i++) {
                String selectedFieldName = fieldNames.get(i);
                if (selectedFieldName.contains(".")) {
                    fieldNames.set(i, selectedFieldName.split("\\.")[1]);
                }
                System.out.print(fieldNames.get(i) + "\t");
            }
        }

        System.out.println();

        for (int i = 0; i < numOfRelationBlocks; i++) {
            // read a block from disk to main memory
            relation.getBlock(i, 0);
            Block mainMemoryBlock = mainMemory.getBlock(0);
            if (mainMemoryBlock.getNumTuples() == 0) continue;
            // read tuple in the block
            for (Tuple tuple : mainMemoryBlock.getTuples()) {
                if (tuple.isNull()) continue;
                if (parserContainer.isWhere()) {
                    // construct expression tree to implement the conditions
                    ExpressionTree expressionTree =
                            new ExpressionTree(parserContainer.getWhereCondition(), parserContainer.getTable());
                    // check this tuple satisfy the condition or not
                    if (!ExpressionTree.checkCondition(tuple, expressionTree.getRoot())) continue;
                }

                StringBuilder sb = new StringBuilder();
                for (String field : fieldNames) {
                    String val = tuple.getField(field).toString();
                    // TODO MINI
                    if (val.equals("-2147483648") || val.equals("null")) val = "NULL";
                    sb.append(val).append("\t");
                }
                String cur = sb.toString();
                // handle distinct
                if (parserContainer.isDistinct() && cur.equals(prev)) continue;
                prev = cur;
                System.out.println(cur);
                numOfRows++;
            }
        }
        System.out.println("---------------------------");
        System.out.println(numOfRows + " rows of results");
    }

    // used in one pass, tuple already chosen in the selectQueryFromSingleTable method
    public static void tuples(ParserContainer parserContainer, ArrayList<Tuple> selectedTuples,
                              ArrayList<String> selectedFieldNamesList) {
        String prev = "nullPrev";
        int numberOfRows = 0;
        if (parserContainer.getTable() != null && !parserContainer.getTable().contains("CrossJoin"))
        {
            for (int i = 0; i < selectedFieldNamesList.size(); i++) {
                String selectedFieldName = selectedFieldNamesList.get(i);
                if (selectedFieldName.contains(".")) {
                    selectedFieldNamesList.set(i, selectedFieldName.split("\\.")[1]);
                }
                System.out.print(selectedFieldNamesList.get(i) + "\t");
            }
        }
        System.out.println();
        for (Tuple tuple : selectedTuples) {
            if (tuple.isNull()) continue;
            StringBuilder sb = new StringBuilder();
            for (String field : selectedFieldNamesList) {
                sb.append(tuple.getField(field)).append("\t");
            }
            String cur = sb.toString();
            if (parserContainer.isDistinct() && cur.equals(prev)) continue;
            prev = cur;
            System.out.println(cur);
            numberOfRows++;
        }
        System.out.println("-------------------------------------------------------");
        System.out.println(numberOfRows + " rows of results");
    }

}
