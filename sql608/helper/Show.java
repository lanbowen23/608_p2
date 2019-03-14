package sql608.helper;

import sql608.algorithm.ExpressionTree;
import sql608.parse.ParserContainer;
import storageManager.Block;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Tuple;

import java.util.ArrayList;

/* Print out the tuple to the interface for selection methods */
public class Show {
    /* Print out all fields of tuples in the relation satisfy the condition */
    /* able to handle DISTINCT after sorting */
    public static void tuples(ParserContainer parserContainer, Relation relation,
                              ArrayList<String> fieldNames, MainMemory mainMemory) {
        /* print the head of the relation */
        if (parserContainer.getTable() != null
            && !parserContainer.getTable().contains("Join")
            && !parserContainer.getTable().contains("naturalJoin")) {
            for (int i = 0; i < fieldNames.size(); i++) {
                String selectedFieldName = fieldNames.get(i);
                if (selectedFieldName.contains(".")) {
                    fieldNames.set(i, selectedFieldName.split("\\.")[1]);
                }
                System.out.print(fieldNames.get(i) + "\t");
            }
        }
        System.out.println();

        /* start to print the tuples */
        int numOfRows = 0;
        String prev = null;  // hold a previous pointer to eliminate duplicate
        /* loop over blocks of relation from disk to main memory */
        int numRelationBlocks = relation.getNumOfBlocks();
        for (int i = 0; i < numRelationBlocks; i++) {
            relation.getBlock(i, 0);
            Block memBlock = mainMemory.getBlock(0);

            if (memBlock.getNumTuples() == 0) continue;
            /* read and print tuples in the mem block */
            for (Tuple tuple : memBlock.getTuples()) {
                if (tuple.isNull()) continue;
                /* use expression tree to check where condition */
                if (parserContainer.isWhere()) {
                    ExpressionTree expressionTree =
                            new ExpressionTree(parserContainer.getConditions(), parserContainer.getTable());
                    if (!ExpressionTree.checkCondition(tuple, expressionTree.getRoot())) continue;
                }

                StringBuilder sb = new StringBuilder();
                for (String field : fieldNames) {
                    String val = tuple.getField(field).toString();
                    /* handle null case */
                    String MIN = Integer.toString(Integer.MIN_VALUE);
                    if (val.equals(MIN) || val.equals("null")) val = "NULL";

                    sb.append(val).append("\t");
                }
                String cur = sb.toString();
                /* handle distinct (duplicate eliminate)*/
                if (parserContainer.isDistinct() && cur.equals(prev)) continue;
                prev = cur;
                System.out.println(cur);
                numOfRows++;
            }
        }
        System.out.println("---------------------------");
        System.out.println(numOfRows + " rows of results");
    }

}
