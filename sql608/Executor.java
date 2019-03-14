package sql608;


import sql608.algorithm.*;
import sql608.helper.*;
import sql608.parse.Parser;
import sql608.parse.ParserContainer;
import storageManager.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/*
physical implementation for sql
use expressionTree structure to deal with the WHERE clause
use Heap to deal with the ORDER BY and DISTINCT
optimize the selection
implement natural join with two pass merge sort
optimize join order
*/

public class Executor {
    public Parser parser;
    public Disk disk;
    public MainMemory mainMemory;
    public SchemaManager schemaManager;

    Executor() {
        parser = new Parser();
        mainMemory = new MainMemory();
        disk = new Disk();
        schemaManager = new SchemaManager(mainMemory, disk);
        disk.resetDiskIOs();
        disk.resetDiskTimer();
    }

    /* clear all blocks of Main Memory */
    private void clearMainMemory() {
        int numberOfBlocks = mainMemory.getMemorySize();
        for (int i = 0; i < numberOfBlocks; i++) {
            mainMemory.getBlock(i).clear();
        }
    }

    void exec(String sql) {
        String action = sql.trim().toLowerCase().split("[\\s]+")[0];
        try {
            switch (action) {
                case "create":
                    this.createQuery(sql);
                    break;
                case "drop":
                    this.dropQuery(sql);
                    break;
                case "insert":
                    this.insertQuery(sql);
                    break;
                case "delete":
                    this.deleteQuery(sql);
                    break;
                case "select":
                    this.selectQuery(sql);
                    break;
                case "file":
                    String file = sql.trim().toLowerCase().split("[\\s]+")[1];
                    executeFile(file);
                    break;
                default:
                    System.out.println("Invalid query, try again");
                    break;
            }
        } catch (Exception e) {
            System.out.println("Exception: Debug Required");
        }
        System.out.println("-------------------------------------------------------");
    }

    private void executeFile(String file_name) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(file_name))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                exec(line);
            }
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    private void createQuery(String sql) throws Exception {
        TableContainer<String, ArrayList<String>, ArrayList<FieldType>> tableContainer = parser.parseCreate(sql);
        String tableName = tableContainer.tableName;
        Schema schema = new Schema(tableContainer.fieldList, tableContainer.fieldType);
        schemaManager.createRelation(tableName, schema);
    }

    private void dropQuery(String sql) {
        schemaManager.deleteRelation(parser.parseDrop(sql));
    }

    private void insertQuery(String sql) {
        double startTime = System.currentTimeMillis();
        long startDiskIO = disk.getDiskIOs();

        if (!sql.trim().toLowerCase().contains("select")) {
            insertQueryWithValues(sql);
        } else {
            insertQueryWithSelect(sql);
        }

        double stopTime = System.currentTimeMillis();
        long stopDiskIO = disk.getDiskIOs();
        double timeSpent = (stopTime - startTime)/1000;
        long diskIOTaken = stopDiskIO - startDiskIO;
        System.out.println("Execution time: " + timeSpent + "seconds");
        System.out.println("Disk IO taken: " + diskIOTaken);
        System.out.println();
    }

    /* For multiple-table case, separately consider the join and optimization */
    private void selectQuery(String sql) {
        double startTime = System.currentTimeMillis();
        long startDiskIO = disk.getDiskIOs();

        ParserContainer parserContainer = parser.parseSelect(sql);

        if (parserContainer.getTables() == null) selectFromSingleTable(parserContainer);
        else selectFromMultipleTables(parserContainer);

        double stopTime = System.currentTimeMillis();
        long stopDiskIO = disk.getDiskIOs();
        double timeSpent = (stopTime - startTime)/1000;
        long diskIOTaken = stopDiskIO - startDiskIO;
        System.out.println("Execution time: " + timeSpent + "seconds");
        System.out.println("Disk IO taken: " + diskIOTaken);
        System.out.println();
    }

    private void deleteQuery(String sql) {
        ParserContainer parserContainer = parser.parseDelete(sql);
        String tableName = parserContainer.getTable();
        String whereCondition = parserContainer.getConditions();
        Relation relation = schemaManager.getRelation(tableName);
        int numOfMemoryBlocks = mainMemory.getMemorySize();
        int numOfRelationBlocks = relation.getNumOfBlocks();
        int curPofRelationBlocks = 0;
        /* read into memory and invalidate */
        while (numOfRelationBlocks > 0) {
            // remaining unprocessed relation blocks may be smaller than 10
            int numberOfBlocksToMemory = Math.min(numOfMemoryBlocks, numOfRelationBlocks);
            // read into memory
            relation.getBlocks(curPofRelationBlocks, 0, numberOfBlocksToMemory);
            // start processing block by block in main memory
            for (int i = 0; i < numberOfBlocksToMemory; i++) {
                Block block = mainMemory.getBlock(i);
                if (block.getNumTuples() == 0) continue;
                // returns all the Tuples inside this block
                ArrayList<Tuple> tuples = block.getTuples();
                if (parserContainer.isWhere()) {
                    for (int j = 0; j < tuples.size(); j++) {
                        Tuple tuple = tuples.get(j);
                        ExpressionTree expressionTree =
                                new ExpressionTree(whereCondition, parserContainer.getTable());
                        if (ExpressionTree.checkCondition(tuple, expressionTree.getRoot()))
                            block.invalidateTuple(j);
                    }
                } else {
                    block.invalidateTuples();
                }
            }
            //reads several blocks from the memory and stores on the disk
            relation.setBlocks(curPofRelationBlocks, 0, numberOfBlocksToMemory);
            // minus processed portion
            numOfRelationBlocks -= numberOfBlocksToMemory;
            // log current progress by this pointer
            curPofRelationBlocks += numberOfBlocksToMemory;
        }
        System.out.println("Number of blocks of " + tableName + ": " + relation.getNumOfBlocks());
        System.out.println("Number of twoTuples of " + tableName + ": " + relation.getNumOfTuples());
        /*
        delete possible hole, it's too slow, will do it while select
        sort(relation, relation.getSchema().getFieldName(0));
        */
    }

    private void insertQueryWithValues(String sql) {
        clearMainMemory();
        TableContainer<String, ArrayList<String>, ArrayList<Field>> tableContainer = parser.parseInsertOneTuple(sql, null);
        String relationName = tableContainer.tableName;
        Relation relation = schemaManager.getRelation(relationName);
        if (relation == null) { return; }
        tableContainer = parser.parseInsertOneTuple(sql, relation);
        // creates an empty tuple of Schema
        Tuple tuple = relation.createTuple();
        ArrayList<String> filedNames = tableContainer.fieldList;
        ArrayList<Field> fields = tableContainer.fieldType;
        // loop over all fields within Schema
        for (int i = 0; i < filedNames.size(); i++) {
            Field field = fields.get(i);
            if (field == null) {
                System.out.println("null");
                tuple.setField(filedNames.get(i), "null");
            } else {
                if (field.type == FieldType.INT) {
                    tuple.setField(filedNames.get(i), field.integer);
                } else {
                    tuple.setField(filedNames.get(i), field.str);
                }
            }
        }
        Write.tuple(tuple, relation, mainMemory, 5);
        System.out.println("Number of blocks of " + relationName + ": " + relation.getNumOfBlocks());
        System.out.println("Number of twoTuples of " + relationName + ": " + relation.getNumOfTuples());
    }

    /* get rows in Select statement and insert into the table */
    private void insertQueryWithSelect(String sql) {
        /* INSERT INTO course (sid, homework, project, exam, grade) SELECT * FROM course */
        ValueContainer<String, ParserContainer> valueContainer = parser.parseInsertWithSelect(sql);
        String insertTableName = valueContainer.nodeName;
        Relation insertRelation = schemaManager.getRelation(insertTableName);
        ParserContainer selectParser = valueContainer.condition;

        String fromTableName = selectParser.getTable();
        Relation fromRelation = schemaManager.getRelation(fromTableName);
        if (fromRelation == null) return;

        /* get all the attribute fields returned by Select From part */
        ArrayList<String> selectFieldNamesList;
        ArrayList<String> selectAttributes = selectParser.getAttributes();
        if (selectAttributes.size() == 0) return;
        if (selectAttributes.size() == 1 && selectAttributes.get(0).equals("*")) {
            selectFieldNamesList = fromRelation.getSchema().getFieldNames();
        } else {
            selectFieldNamesList = selectAttributes;
        }

        clearMainMemory();
        /* Read in tuple from Select From part and append it to insert table */
        /* query in the "test.txt" is without DISTINCT and ORDER BY */
        int numRelationBlocks = fromRelation.getNumOfBlocks();
        if (!selectParser.isDistinct() && !selectParser.isOrder()) {
            for (int i = 0; i < numRelationBlocks; i++) {
                mainMemory.getBlock(0).clear();
                /* read a block of Select From table, get tuples from it */
                fromRelation.getBlock(i, 0);
                Block block = mainMemory.getBlock(0);
                if (block.getNumTuples() == 0) continue;
                for (Tuple tuple : block.getTuples()) {
                    /* check where condition */
                    if (selectParser.isWhere()) {
                        ExpressionTree expressionTree = new ExpressionTree(selectParser.getConditions(),
                                selectParser.getTable());
                        if (!ExpressionTree.checkCondition(tuple, expressionTree.getRoot())) {
                            continue;
                        }
                    }
                    /* creates an empty tuple of the Schema */
                    Tuple newTuple = insertRelation.createTuple();
                    for (String fieldName : selectFieldNamesList) {
                        Field curField = tuple.getField(fieldName);
                        if (curField.type == FieldType.INT) newTuple.setField(fieldName, curField.integer);
                        else newTuple.setField(fieldName, curField.str);
                    }
                    Write.tuple(newTuple, insertRelation, mainMemory, 9);
                }
            }
            return;
        }

        System.out.println("Number of blocks of " + insertTableName + ": " + insertRelation.getNumOfBlocks());
        System.out.println("Number of twoTuples of " + insertTableName + ": " + insertRelation.getNumOfTuples());
    }

    private void selectFromSingleTable(ParserContainer parserContainer) {
        String tableName = parserContainer.getTable();
        ArrayList<String> attributes = parserContainer.getAttributes();

        Relation relation = schemaManager.getRelation(tableName);
        if (relation == null || attributes.size() == 0) return; // relation is empty


        /* store the SELECT attributes to string list */
        ArrayList<String> fieldNames;
        /* SELECT * FROM course, attributes may be "*" */
        if (attributes.get(0).equals("*")) {
            fieldNames = relation.getSchema().getFieldNames();
        } else fieldNames = attributes;

        /* if no distinct or order by condition, just print result */
        if (!parserContainer.isDistinct() && !parserContainer.isOrder()) {
            Show.tuples(parserContainer, relation, fieldNames, mainMemory);
            return;
        }

        int numRelationBlocks = relation.getNumOfBlocks();
        int numMemBlocks = mainMemory.getMemorySize();
        /* relation can fit into main memory, use one pass algorithm */
        if (numRelationBlocks <= numMemBlocks) {
            if (parserContainer.isOrder()) {
                ArrayList<String> sortFields = new ArrayList<>();
                String orderAttr = parserContainer.getOrderAttribute();
                sortFields.add(orderAttr);
                for (String field : fieldNames) if (!field.equals(orderAttr)) sortFields.add(field);
                OnePass.sort(relation, sortFields, mainMemory);
            }
            else OnePass.sort(relation, fieldNames, mainMemory);
            Show.tuples(parserContainer, relation, fieldNames, mainMemory);
        } else {
        /* the table cannot fit into main memory, use two pass algorithm */
            if (parserContainer.isOrder()) {
            /*
            check ORDER field
            sortField should start with the ORDER BY attr (For Heap Comparator)
            then the other attributes
            */
                ArrayList<String> sortFields = new ArrayList<>();
                String orderAttr = parserContainer.getOrderAttribute();
                sortFields.add(orderAttr);
                for (String field : fieldNames) if (!field.equals(orderAttr)) sortFields.add(field);
                TwoPass.sort(relation, sortFields, mainMemory);
            } else TwoPass.sort(relation, fieldNames, mainMemory);

            /* able to do DISTINCT (eliminate duplication) after sorting */
            Show.tuples(parserContainer, relation, fieldNames, mainMemory);
        }
    }

    private void selectFromMultipleTables(ParserContainer parserContainer) {
        ExpressionTree expressionTree = new ExpressionTree(parserContainer.getConditions(), "");

        /*
        consider push down the row selection in the conditions
        delete those satisfied conditions in the parser container
        satisfied condition: e.g. course.homework = 100
        and generate a new tempcourse table substitute original one
        */
        if (parserContainer.isWhere()) selectFirst(expressionTree, parserContainer);

        ArrayList<String> attributes = parserContainer.getAttributes();
        ArrayList<String> tableLists = parserContainer.getTables();  // reference

        /*
        command: SELECT DISTINCT table.attr FROM tables
        distinct needs the sorting on the attr
        from table.attrs get related tables for attrs
        store tables and its corresponding field names start by attrs
        */
        Map<String, ArrayList<String>> tableToAttr = new HashMap<>();
        if (parserContainer.isDistinct()) {
            if (attributes.get(0).equals("*")) {
                for (String table : tableLists) {
                    tableToAttr.put(table, schemaManager.getRelation(table).getSchema().getFieldNames());
                }
            }
            else {
                /* split0: table | split1: attr ; get related tables from attr*/
                for (String attr : attributes) {
                    String[] split = attr.split("\\.");
                    if (!tableToAttr.containsKey(split[0])) tableToAttr.put(split[0], new ArrayList<>());
                    /* always start attr */
                    tableToAttr.get(split[0]).add(split[1]);
                }
                /* for each related table add rest of attrs */
                for (Map.Entry<String, ArrayList<String>> entry : tableToAttr.entrySet()) {
                    for (String attr : schemaManager.getRelation(entry.getKey()).getSchema().getFieldNames())
                    {
                        if (!entry.getValue().contains(attr)) {
                            entry.getValue().add(attr);
                        }
                    }
                }
            }
        }

        String joinedTableName;
        if (parserContainer.isWhere()) {
            /* First check if the Natural Join is feasible */
            ExpressionTree expressionTree2 = new ExpressionTree(parserContainer.getConditions(), "");
            ArrayList<ExpressionTreeNode> subNodes = ExpressionTree.getSubTreeNodes(expressionTree2).nodeName;

            /*
            check conditions satisfy natural join or not
            e.g. course.sid = course2.sid
            */
            ArrayList<ExpressionTreeNode> normalNodes = new ArrayList<>();
            ArrayList<ExpressionTreeNode> naturalJoinNodes = new ArrayList<>();
            for (ExpressionTreeNode node : subNodes) {
                if (isNaturalJoin(node)) naturalJoinNodes.add(node);
                else normalNodes.add(node);
            }
            /*
            do the Natural Join if there is satisfied condition node
            return tmp joined table
            natural joined table, without duplicate
            */
            // get undergoing tables (tmp) after select done first
            ArrayList<String> newTableList = parserContainer.getTables();
            HashSet<String> joinedTableSet = new HashSet<>();
            for (ExpressionTreeNode naturalJoinNode : naturalJoinNodes) {
                /* these names will not contain temp */
                ArrayList<String> tableNames = getTableNames(naturalJoinNode);
                if (tableNames.size() == 2) {
                    String tableOne = tableNames.get(0);
                    String tableTwo = tableNames.get(1);
                    /* both tables have been joined */
                    if (joinedTableSet.contains(tableOne) &&
                            joinedTableSet.contains(tableTwo)) {
                        normalNodes.add(naturalJoinNode);
                        continue;
                    }
                    /* check if the table already been joined, if so, use the joined table */
                    if (joinedTableSet.contains(tableOne)) {
                        // table name can become rnaturalJoint
                        for (String table : newTableList) {
                            String[] tables = table.split("temp|naturalJoin");
                            if (Arrays.asList(tables).contains(tableOne)) tableOne = table;
                        }
                    } else joinedTableSet.add(tableOne);
                    if (joinedTableSet.contains(tableTwo)) {
                        for (String newTable : newTableList) {
                            String[] tables = newTable.split("temp|naturalJoin");
                            if (Arrays.asList(tables).contains(tableTwo)) tableTwo = newTable;
                        }
                    } else joinedTableSet.add(tableTwo);
                    /* if selection optimized before, use temp table */
                    if (!parserContainer.getTables().contains(tableOne) &&
                            parserContainer.getTables().contains("temp" + tableOne)) {
                        tableOne = "temp" + tableOne;
                    }
                    if (!parserContainer.getTables().contains(tableTwo) &&
                            parserContainer.getTables().contains("temp" + tableTwo)) {
                        tableTwo = "temp" + tableTwo;
                    }
                    /* return a temporary table, delete old ones in tableList of parserContainer */
                    String tableName = Natural.join(tableOne, tableTwo,
                            naturalJoinNode.getString(naturalJoinNode), mainMemory, schemaManager);
                    newTableList.remove(tableOne);  // r
                    newTableList.remove(tableTwo);  // s
                    newTableList.add(tableName);  // rnaturalJoins
                }
            }
            /* store new temporary tables like r naturalJoin t naturalJoin s */
            parserContainer.setTables(newTableList);
            /* check if there is left table, if so, reset the condition in parserContainer */
            if(normalNodes.size() == 0) {
                parserContainer.setWhere(false);
                parserContainer.setConditions(null);
            } else {
                ExpressionTreeNode remainConditions = mergeNodes(normalNodes);
                parserContainer.setConditions(remainConditions.getString(remainConditions));
            }

            /* Cross Join */
            joinedTableName = Join.crossJoinTables(tableLists, parserContainer.isDistinct(), tableToAttr,
                    mainMemory, schemaManager);
        }
        else {
            /* no WHERE do cross join directly */
            joinedTableName = Join.crossJoinTables(tableLists, parserContainer.isDistinct(), tableToAttr,
                    mainMemory, schemaManager);
        }

        /* Finish Join: now it comes down to single table case */
        parserContainer.setTable(joinedTableName);
        parserContainer.setTables(null);

        /*
        only when table is larger than main mem size
        set distinct and do two pass sort before output
        If using one-pass, distinct is already done after join
        */
        if (parserContainer.isDistinct()) {
            parserContainer.setDistinct(false);
            for (String table : tableLists) {
                if (schemaManager.getRelation(table).getNumOfBlocks() > 10) {
                    parserContainer.setDistinct(true);
                    break;
                }
            }
        }

        selectFromSingleTable(parserContainer);
        schemaManager.deleteRelation(joinedTableName);
    }

    /* Natural Join Condition: two tables and their selected fields are same */
    private boolean isNaturalJoin(ExpressionTreeNode node) {
        boolean twoTable = getTableNames(node).size() == 2;
        boolean equals = node.getValue().equals("=");
        if (!twoTable || !equals) return false;
        String leftAttr = node.getLeft().getValue().split("\\.")[1];
        String rightAttr = node.getRight().getValue().split("\\.")[1];
        return leftAttr.equals(rightAttr);
    }

    /*
    Below is for doing the selection first which means
    if there is condition like "exam = 100"
    it will be executed before join
    */
    private void selectFirst(ExpressionTree expressionTree, ParserContainer parserContainer) {
        // retrieve the expression tree nodes separated by AND
        ArrayList<ExpressionTreeNode> subNodes = ExpressionTree.getSubTreeNodes(expressionTree).nodeName;
        ArrayList<ExpressionTreeNode> nodeWithMultiTable = new ArrayList<>();
        for (ExpressionTreeNode node : subNodes) {
            // find those relation coupled with selection
//            if (!node.getString(node).contains("|")) {
                ArrayList<String> relevantTables = getTableNames(node);
                if (relevantTables.size() > 1) nodeWithMultiTable.add(node);
//            }
        }
        // may have several course.id = course2.id which should be left
        ExpressionTreeNode mergeNode = mergeNodes(nodeWithMultiTable);
        // TODO influence the Condition parentheses
        String mergeCondition = mergeNode.getString(mergeNode).trim();
        parserContainer.setConditions(mergeCondition);

        ArrayList<String> tmpRelations = generateTempRelations(subNodes);

        /* modify the tables name to tempTableName in parser container Tables */
        for (String tmpName : tmpRelations) {
            String originalName = tmpName.replace("temp", "");
            for (int i = 0; i < parserContainer.getTables().size(); i++) {
                if (parserContainer.getTables().get(i).equals(originalName)) {
                    parserContainer.getTables().set(i, tmpName);
                }
            }
        }
    }

    private ExpressionTreeNode mergeNodes(ArrayList<ExpressionTreeNode> nodeList) {
        // only one node
        if (nodeList.size() == 1) return nodeList.get(0);

        ExpressionTreeNode root = new ExpressionTreeNode("&");
        ExpressionTreeNode left = nodeList.get(0);
        ExpressionTreeNode right;
        for (int i = 1; i < nodeList.size(); i++) {
            right = nodeList.get(i);
            ExpressionTreeNode tmp = new ExpressionTreeNode("&", left, right);
            root = tmp;
            left = tmp;
        }
        return root;
    }

    /* recursive retrieve relevant table names corresponding to one condition tree node */
    private ArrayList<String> getTableNames(ExpressionTreeNode node) {
        ArrayList<String> tables = new ArrayList<>();
        if (node == null) return tables;
        String nodeValue = node.getValue();
        String tableName = "";
        if (!nodeValue.contains(".")  &&
            nodeValue.matches("^[a-zA-Z]+[\\w\\d]*")) {
            tableName = nodeValue;
        } else if (nodeValue.contains(".")) {
            // in case course2.exam, don't need attr_name, just table name course2
            tableName = nodeValue.split("\\.")[0];
        }
        if (!tables.contains(tableName) &&
            !Objects.equals(tableName, "")) {
            tables.add(tableName);
        }
        tables.addAll(getTableNames(node.getLeft()));
        tables.addAll(getTableNames(node.getRight()));
        return tables;
    }

    /* depends on conditions type, we may need a new temp table with selection done first */
    private ArrayList<String> generateTempRelations(ArrayList<ExpressionTreeNode> subConditions) {
        ArrayList<String> tempTables = new ArrayList<>();
        HashMap<String, ArrayList<ExpressionTreeNode>> tableSelectionMap = new HashMap<>();

        if (subConditions == null || subConditions.size() == 0) return tempTables;

        for (ExpressionTreeNode subCondition : subConditions) {
            ArrayList<String> tableList = getTableNames(subCondition);
            // we want condition doing selection like course.homework = 100
            if (tableList.size() != 1) continue;
            String table = tableList.get(0);
            if (tableSelectionMap.containsKey(table)) {
                // may contain multiple conditions on same table
                ArrayList<ExpressionTreeNode> list = tableSelectionMap.get(table);
                list.add(subCondition);
            } else {
                ArrayList<ExpressionTreeNode> list = new ArrayList<>();
                list.add(subCondition);
                tableSelectionMap.put(table, list);
            }
        }
        for (String tableName : tableSelectionMap.keySet()) {
            ArrayList<ExpressionTreeNode> conditions = tableSelectionMap.get(tableName);
            createTempRelation(tableName, conditions);
            tempTables.add("temp" + tableName);
        }
        return tempTables;
    }

    private void createTempRelation(String table, ArrayList<ExpressionTreeNode> nodeList) {
        String relationName = "temp" + table;
        if (schemaManager.relationExists(relationName)) schemaManager.deleteRelation(relationName);
        // need an expression tree to check
        ExpressionTreeNode node;
        if (nodeList.size() > 1) node = mergeNodes(nodeList);
        else node = nodeList.get(0);

        Relation relation = schemaManager.getRelation(table);
        int relationBlocks = relation.getNumOfBlocks();
        int memBlocks = mainMemory.getMemorySize() - 1;
        int processedBlocks = 0;

        ArrayList<String> fieldNames = new ArrayList<>();
        for (String str : relation.getSchema().getFieldNames()) {
            fieldNames.add(table + "." + str);
        }
        Relation tmpRelation = schemaManager.createRelation(relationName,
                new Schema(fieldNames, relation.getSchema().getFieldTypes()));
        do {
            int blocksToMem = relationBlocks < memBlocks ? relationBlocks : memBlocks;
            //reads several blocks from the relation (the disk) and stores in the memory
            relation.getBlocks(processedBlocks, 0, blocksToMem);
            for (int i = 0; i < blocksToMem; i++) {
                Block block = mainMemory.getBlock(i);
                // this is to handle the holes after deletion
                if (block.getNumTuples() == 0) continue;
                for (Tuple tuple : block.getTuples()) {
                    Tuple tmpTuple = createTempTuple(tmpRelation, tuple);
                    if (node == null || ExpressionTree.checkCondition(tmpTuple, node))
                        Write.tuple(tmpTuple, tmpRelation, mainMemory, memBlocks);
                }
            }
            relationBlocks -= blocksToMem;
            processedBlocks += blocksToMem;
        } while (relationBlocks > 0);
    }

    private Tuple createTempTuple(Relation relation, Tuple tuple) {
        Tuple tmpTuple = relation.createTuple();
        if (tmpTuple.getNumOfFields() != tuple.getNumOfFields()) return null;
        for (int i = 0; i < tuple.getNumOfFields(); i++) {
            Field field = tuple.getField(i);
            if (field.type == FieldType.INT) tmpTuple.setField(i, field.integer);
            else tmpTuple.setField(i, field.str);
        }
        return tmpTuple;
    }

}
