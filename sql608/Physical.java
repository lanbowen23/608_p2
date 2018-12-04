package sql608;


import sql608.heap.TupleHeap;
import sql608.heap.TupleHeap2;
import sql608.heap.TupleWithDiskId;
import sql608.helper.*;
import storageManager.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

// physical implementation for sql
// use expressionTree structure to deal with the WHERE clause
// use heap to deal with the ORDER BY clause
// optimize the selection push down
// optimize join order

public class Physical {
    Parser parser;
    MainMemory mainMemory;
    Disk disk;
    SchemaManager schemaManager;

    public Physical() {
        parser = new Parser();
        mainMemory = new MainMemory();
        disk = new Disk();
        schemaManager = new SchemaManager(mainMemory, disk);
        disk.resetDiskIOs();
        disk.resetDiskTimer();
    }

    // clear all blocks of Main Memory
    private void clearMainMemory() {
        int numberOfBlocks = mainMemory.getMemorySize();
        for (int i = 0; i < numberOfBlocks; i++) {
            mainMemory.getBlock(i).clear();
        }
    }

    // field per block is 8, so tuple with fields
    // more than five will consume one block
    private void insertTupleToDisk(Tuple tuple, Relation relation, MainMemory mem, int memBlockInd) {
        Block block;
        // if relation is empty in the disk at first
        if (relation.getNumOfBlocks() == 0) {
            block = mem.getBlock(memBlockInd);
            block.clear();
            block.appendTuple(tuple);
            // reads several blocks from the memory and stores on the disk
            relation.setBlock(0, memBlockInd);
        } else {  // read in the relation already exist in the disk first
            relation.getBlock(relation.getNumOfBlocks() - 1, memBlockInd);
            block = mem.getBlock(memBlockInd);
            // if current mem block is full, meaning the disk block is also full
            // write to next block on the disk
            if (block.isFull()) {
                block.clear();
                block.appendTuple(tuple);
                relation.setBlock(relation.getNumOfBlocks(), memBlockInd);
            } else {
                block.appendTuple(tuple);
                relation.setBlock(relation.getNumOfBlocks() - 1, memBlockInd);
            }
        }
    }

    // compare Two tuples
    public int compare(Tuple o1, Tuple o2, String keyOne, String keyTwo) {
        Field fieldOne = o1.getField(keyOne);
        Field fieldTwo = o2.getField(keyTwo);
        if (fieldOne.type == FieldType.INT) {
            return fieldOne.integer - fieldTwo.integer;
        } else {
            return fieldOne.str.compareTo(fieldTwo.str);
        }
    }

    public void exec(String sql) {
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
        Three<String, ArrayList<String>, ArrayList<FieldType>> three = parser.parseCreate(sql);
        String tableName = three.first;
        Schema schema = new Schema(three.second, three.third);
        schemaManager.createRelation(tableName, schema);
    }

    private void dropQuery(String sql) {
        schemaManager.deleteRelation(parser.parseDrop(sql));
    }

    private void insertQuery(String sql) {
        if (!sql.trim().toLowerCase().contains("select")) {
            insertQueryWithValues(sql);
        } else {
            insertQueryWithSelect(sql);
        }
    }

    private void selectQuery(String sql) {
        double startTime = System.currentTimeMillis();
        long startDiskIO = disk.getDiskIOs();

        ParserContainer parserContainer = parser.parseSelect(sql);
        if (parserContainer.getTables() == null) {
            selectFromSingleTable(parserContainer);
        } else {
            selectFromMultipleTables(parserContainer);
        }

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
        String whereCondition = parserContainer.getWhereCondition();
        Relation relation = schemaManager.getRelation(tableName);
        int numOfMemoryBlocks = mainMemory.getMemorySize();
        int numOfRelationBlocks = relation.getNumOfBlocks();
        int curPofRelationBlocks = 0;
        // read into memory and invalidate
        while (numOfRelationBlocks > 0) {
            // remaining unprocessed relation blocks may be smaller than 10
            int numberOfBlocksToMemory = Math.min(numOfMemoryBlocks, numOfRelationBlocks);
            // read into memory
            relation.getBlocks(curPofRelationBlocks, 0, numberOfBlocksToMemory);
            // start processing block by block in main memory
            for (int i = 0; i < numberOfBlocksToMemory; i++) {
                Block block = mainMemory.getBlock(i);
                if (block.getNumTuples() == 0) continue;
                // returns all the tuples inside this block
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
        System.out.println("Number of tuples of " + tableName + ": " + relation.getNumOfTuples());
        // delete possible hole, it's too slow, will do it while select
        // twoPassSort(relation, relation.getSchema().getFieldName(0));
    }

    private void insertQueryWithValues(String sql) {
        clearMainMemory();
        Three<String, ArrayList<String>, ArrayList<Field>> three = parser.parseInsertOneTuple(sql, null);
        String relationName = three.first;
        Relation relation = schemaManager.getRelation(relationName);
        if (relation == null) { return; }
        three = parser.parseInsertOneTuple(sql, relation);
        // creates an empty tuple of schema
        Tuple tuple = relation.createTuple();
        ArrayList<String> filedNames = three.second;
        ArrayList<Field> fields = three.third;
        // loop over all fields within schema
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
        insertTupleToDisk(tuple, relation, mainMemory, 5);
        System.out.println("Number of blocks of " + relationName + ": " + relation.getNumOfBlocks());
        System.out.println("Number of tuples of " + relationName + ": " + relation.getNumOfTuples());
    }

    // TODO delete or not?
    private void insertQueryWithSelect(String sql) {
        Two<String, ParserContainer> pairOutput = parser.parseInsertForSelect(sql);
        String tableToBeInserted = pairOutput.first;
        ParserContainer parserContainer = pairOutput.second;
        Relation toRelation = schemaManager.getRelation(tableToBeInserted);
        String fromTableName = parserContainer.getTable();
        Relation fromRelation = schemaManager.getRelation(fromTableName);
        ArrayList<String> selectedAttributes = parserContainer.getAttributes();
        if (fromRelation == null || selectedAttributes.size() == 0) return;
        clearMainMemory();
        int relationNumOfBlocks = fromRelation.getNumOfBlocks();
        // number of tuples of this relation in disk
        ArrayList<String> selectedFieldNamesList;
        if (selectedAttributes.size() == 1 && selectedAttributes.get(0).equals("*")) {
            // no projection
            selectedFieldNamesList = fromRelation.getSchema().getFieldNames();
        } else {
            selectedFieldNamesList = selectedAttributes;
        }
        // if not distinct or order by condition, just print result without
        // storing
        if (!parserContainer.isDistinct() && !parserContainer.isOrder()) {
            for (int i = 0; i < relationNumOfBlocks; i++) {
                mainMemory.getBlock(0).clear();
                fromRelation.getBlock(i, 0);// read a block from disk to main
                // memory
                Block mainMemoryBlock = mainMemory.getBlock(0);
                if (mainMemoryBlock.getNumTuples() == 0) {
                    continue;
                }
                for (Tuple tuple : mainMemoryBlock.getTuples()) {
                    if (parserContainer.isWhere()) {
                        ExpressionTree expressionTree = new ExpressionTree(parserContainer.getWhereCondition(),
                                parserContainer.getTable());
                        if (!ExpressionTree.checkCondition(tuple, expressionTree.getRoot())) {
                            continue;
                        }
                    }
                    Tuple newTuple = toRelation.createTuple();// creates an
                    // empty tuple
                    // of the
                    // schema

                    for (int j = 0; j < selectedFieldNamesList.size(); j++) {
                        Field curField = tuple.getField(selectedFieldNamesList.get(j));
                        if (curField.type == FieldType.INT) {
                            newTuple.setField(selectedFieldNamesList.get(j), curField.integer);
                        } else {
                            newTuple.setField(selectedFieldNamesList.get(j), curField.str);
                        }
                    }
                    insertTupleToDisk(newTuple, toRelation, mainMemory, 5);
                }
            }
            return;
        }
        System.out.println("Number of blocks of " + tableToBeInserted + ": " + toRelation.getNumOfBlocks());
        System.out.println("Number of tuples of " + tableToBeInserted + ": " + toRelation.getNumOfTuples());
    }

    private void selectFromSingleTable(ParserContainer parserContainer) {
        String tableName = parserContainer.getTable();
        Relation relation = schemaManager.getRelation(tableName);
        ArrayList<String> attributes = parserContainer.getAttributes();
        // relation is empty
        if (relation == null || attributes.size() == 0) return;
        // number of tuples of this relation in disk
        int numOfRelationBlocks = relation.getNumOfBlocks();
        int numOfMemBlocks = mainMemory.getMemorySize();
        ArrayList<String> fieldNames;
        if (attributes.size() == 1 && attributes.get(0).equals("*")) {
            // SELECT * FROM course
            fieldNames = relation.getSchema().getFieldNames();
        } else {
            fieldNames = attributes;
        }
        // if no distinct or order by condition, just print result
        // TODO without storing??
        if (!parserContainer.isDistinct() && !parserContainer.isOrder()) {
            Show.tuples(parserContainer, relation, fieldNames, mainMemory);
            return;
        }
        // sorting if there is DISTINCT or ORDER BY
        // one pass algo
        ArrayList<Tuple> selectedTuples = new ArrayList<>();
        // relation can fit into main memory
        if (numOfRelationBlocks <= numOfMemBlocks) {
            for (int i = 0; i < numOfRelationBlocks; i++) {
                // read one block from disk to main memory
                relation.getBlock(i, 0);
                Block block = mainMemory.getBlock(0);
                if (block.getNumTuples() == 0) continue;
                // selected tuples won't exceed one block size
                for (Tuple tuple : block.getTuples()) {
                    if (parserContainer.isWhere()) {
                        ExpressionTree expressionTree = new ExpressionTree(parserContainer.getWhereCondition(),
                                parserContainer.getTable());
                        if (ExpressionTree.checkCondition(tuple, expressionTree.getRoot())) {
                            selectedTuples.add(tuple);
                        }
                    } else selectedTuples.add(tuple);
                }
            }
            // has DISTINCT
            if (parserContainer.isDistinct()) {
                selectedTuples = OnePass.duplicate(selectedTuples, fieldNames,mainMemory);
            }
            // has ORDER BY
            if (parserContainer.isOrder()) {
                ArrayList<String> sortFields = new ArrayList<>();
                sortFields.add(parserContainer.getOrderAttribute());
                selectedTuples = OnePass.onePassOrder(selectedTuples, sortFields, mainMemory);
            }
            Show.tuples(parserContainer, selectedTuples, fieldNames);
            return;
        }

        // Two pass single table
        // the relation cannot fit into main memory
        if (parserContainer.isOrder()) {
            // check order field
            ArrayList<String> sortFields = new ArrayList<>();
            String orderAttr = parserContainer.getOrderAttribute();
            sortFields.add(orderAttr);
            // TODO why?
            for (String sortField : fieldNames) {
                if (!sortField.equals(orderAttr)) sortFields.add(sortField);
            }
            twoPassSort(relation, sortFields);
        }
        else twoPassSort(relation, fieldNames);
        // able to do DISTINCT / eliminate duplication after sorting
        // output tuples
        Show.tuples(parserContainer, relation, fieldNames, mainMemory);
        return;
    }

    private void selectFromMultipleTables(ParserContainer parserContainer) {
        ExpressionTree expressionTree = new ExpressionTree(parserContainer.getWhereCondition(), "");

        // first consider push down selection in the expression tree
        // e.g. course.homework = 100 will be execute at first and be a new tempcourse back
        if (parserContainer.isWhere()) optimizedSelection(expressionTree, parserContainer);

        ArrayList<String> tableLists = parserContainer.getTables();
        ArrayList<String> attributes = parserContainer.getAttributes();

        // store table and its corresponding field names
        // arguments for the cross join method
        Map<String, ArrayList<String>> tableAttrMap = new HashMap<>();
        if (parserContainer.isDistinct()) {
            if (attributes.get(0).equals("*")) {
                for (String table : tableLists) {
                    tableAttrMap.put(table, schemaManager.getRelation(table).getSchema().getFieldNames());
                }
            } else {
                // process each attributes
                for (String attr : attributes) {
                    String[] records = attr.split("\\.");
                    if (!tableAttrMap.containsKey(records[0])) {
                        tableAttrMap.put(records[0], new ArrayList<>());
                    }
                    tableAttrMap.get(records[0]).add(records[1]);
                }
                // add rest of attrs
                for (Map.Entry<String, ArrayList<String>> entry : tableAttrMap.entrySet()) {
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
        if (parserContainer.isWhere())
        {
            ExpressionTree expressionTree2 = new ExpressionTree(parserContainer.getWhereCondition(), "");
            ArrayList<ExpressionTreeNode> subConditions = ExpressionTree.getSubExpression(expressionTree2).first;
            ArrayList<ExpressionTreeNode> filteredCondition = new ArrayList<>();
            ArrayList<ExpressionTreeNode> naturalJoinCondition = new ArrayList<>();
            for (ExpressionTreeNode node : subConditions) {
                // check conditions satisfy natural join or not
                if (isNaturalJoin(node)) naturalJoinCondition.add(node);
                else filteredCondition.add(node);
            }

            HashSet<String> naturalJoinedTableSet = new HashSet<>();
            ArrayList<String> newTableList = parserContainer.getTables();
            for (ExpressionTreeNode condition : naturalJoinCondition) {
                // these names will not contain temp
                ArrayList<String> tableNames = getTableNames(condition);
                if (tableNames != null && tableNames.size() == 2) {
                    String tableOne = tableNames.get(0);
                    String tableTwo = tableNames.get(1);
                    // Two table should not be the same
                    if (naturalJoinedTableSet.contains(tableOne) && naturalJoinedTableSet.contains(tableTwo)) {
                        filteredCondition.add(condition);
                        continue;
                    }
                    if (naturalJoinedTableSet.contains(tableOne)) {
                        for (String newTable : newTableList) {
                            String[] tables = newTable.split("temp|naturalJoin");
                            if (Arrays.asList(tables).contains(tableOne)) {
                                tableOne = newTable;
                            }
                        }
                    } else {
                        naturalJoinedTableSet.add(tableOne);
                    }
                    if (naturalJoinedTableSet.contains(tableTwo)) {
                        for (String newTable : newTableList) {
                            String[] tables = newTable.split("temp|naturalJoin");
                            if (Arrays.asList(tables).contains(tableTwo)) {
                                tableTwo = newTable;
                            }
                        }
                    } else {
                        naturalJoinedTableSet.add(tableTwo);
                    }
                    if (!parserContainer.getTables().contains(tableOne) &&
                            parserContainer.getTables().contains("temp" + tableOne)) {
                        tableOne = "temp" + tableOne;
                    }
                    if (!parserContainer.getTables().contains(tableTwo) &&
                            parserContainer.getTables().contains("temp" + tableTwo)) {
                        tableTwo = "temp" + tableTwo;
                    }
                    String tableName = naturalJoinTwoTables(tableOne, tableTwo, condition.getString(condition));
                    newTableList.remove(tableOne);
                    newTableList.remove(tableTwo);
                    newTableList.add(tableName);
                }
            }
            parserContainer.setTables(newTableList);

            joinedTableName = crossJoinTables(tableLists, parserContainer.isDistinct(), tableAttrMap);
            if(filteredCondition.size() == 0) {
                parserContainer.setWhere(false);
                parserContainer.setWhereCondition(null);
            } else {
                ExpressionTreeNode remainConditions = mergeNodes(filteredCondition);
                parserContainer.setWhereCondition(remainConditions.getString(remainConditions));
            }
        }
        else { // no WHERE
            joinedTableName = crossJoinTables(tableLists, parserContainer.isDistinct(), tableAttrMap);
        }

        parserContainer.setTable(joinedTableName);
        parserContainer.setTables(null);
        // because we have pushed distinct down, don't need to set distinct here
        // TODO what?
        if (parserContainer.isDistinct()) {
            parserContainer.setDistinct(false);
            for (String table : tableLists) {
                if (schemaManager.getRelation(table).getNumOfBlocks() > mainMemory.getMemorySize()) {
                    parserContainer.setDistinct(true);
                    break;
                }
            }
        }
        selectFromSingleTable(parserContainer);
        schemaManager.deleteRelation(joinedTableName);
    }

    // all tuples can fit into main memory
    private boolean validateOnePass(ArrayList<Tuple> selectedTuples) {
        int numberOfFieldsPerTuple = selectedTuples.get(0).getNumOfFields();
        int max_capacity = (8 / numberOfFieldsPerTuple) * mainMemory.getMemorySize();
        return max_capacity >= selectedTuples.size();
    }

    private ArrayList<Tuple> onePassDuplicateElimination(ArrayList<Tuple> selectedTuples,
                                                         ArrayList<String> selectedFieldNamesList) {
        // selected tuples < main memory
        if (!validateOnePass(selectedTuples)) return null;

        ArrayList<Tuple> result = new ArrayList<>();
        clearMainMemory();
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
    private ArrayList<Tuple> onePassOrder(ArrayList<Tuple> selectedTuples, ArrayList<String> sortFields) {
        if (!validateOnePass(selectedTuples)) return null;
        clearMainMemory();
        mainMemory.setTuples(0, selectedTuples);
        ArrayList<Tuple> result = new ArrayList<>();
        // set up a heap to get ordered tuples
        TupleHeap heap = buildHeap(sortFields);
        offerTuplesFromMemToHeap(heap);
        while (!heap.isEmpty()) result.add(heap.poll());
        return result;
    }

    private TupleHeap buildHeap(ArrayList<String> sortFields) {
        // By default ORDER BY sorts the data in ascending order
        // so I want a heap which returns minimum
        // for TupleHeap we need a comparator
        return new TupleHeap((o1, o2) -> {
            for (String sortField : sortFields) {
                Field field1 = o1.getField(sortField);
                Field field2 = o2.getField(sortField);
                // use built-in function to compare not equal case
                if (field1.type == FieldType.INT) {
                    if (field1.integer != field2.integer) {
                        return Integer.compare(field1.integer, field2.integer);
                    }
                } else if (field1.type == FieldType.STR20) {
                    if (!field1.str.equals(field2.str)) {
                        return field1.str.compareTo(field2.str);
                    }
                }
            }
            // only when all fields escape not equal tests
            // we can return 0 for they are equal
            return 0;
        });
    }

    private TupleHeap2 buildHeap2(ArrayList<String> sortFields) {
        return new TupleHeap2((o1, o2) -> {
            for (String sortField : sortFields) {
                // basically, this sort by the first sortField
                Field field1 = o1.tuple.getField(sortField);
                Field field2 = o2.tuple.getField(sortField);
                if (field1.type == FieldType.INT) {
                    if (field1.integer != field2.integer) {
                        return Integer.compare(field1.integer, field2.integer);
                    }
                } else if (field1.type == FieldType.STR20) {
                    if (!field1.str.equals(field2.str)) {
                        return field1.str.compareTo(field2.str);
                    }
                }
            }
            return 0;
        });
    }
    // push tuples into the heap

    private void offerTuplesFromMemToHeap(TupleHeap heap) {
        int memoryNumberOfBlocks = mainMemory.getMemorySize();
        for (int i = 0; i < memoryNumberOfBlocks; i++) {
            Block block = mainMemory.getBlock(i);
            // TODO if maybe redundant
            if (!block.isEmpty() && block.getNumTuples() > 0) {
                for (Tuple tuple : block.getTuples()) {
                    if (tuple.isNull()) continue;
                    heap.offer(tuple);
                }
            }
        }
    }

    private void sortTuplesInRelation(Relation relation, ArrayList<String> sortFields) {
        if (relation.getNumOfBlocks() <= mainMemory.getMemorySize()) {
            onePassSort(relation, sortFields);
        } else {
            twoPassSort(relation, sortFields);
        }
    }
    // Two pass merge sort algorithm

    // TODO how to Two pass sort the table
    private void twoPassSort(Relation relation, ArrayList<String> sortFields) {
        int numRealBlocks = relation.getNumOfBlocks();
        int numMemBlocks = mainMemory.getMemorySize();
        int numSublists = (numRealBlocks % numMemBlocks == 0) ?
                numRealBlocks / numMemBlocks : numRealBlocks / numMemBlocks + 1;
        // get sublist of relation, sort and write back to disk
        for (int i = 0; i < numSublists; i++) {
            clearMainMemory();
            // get one sublist of relation (10 blocks) from disk into main memory
            for (int j = 0; j < numMemBlocks; j++) {
                int curRelInd = i * numMemBlocks + j;
                if (curRelInd >= numRealBlocks) break;
                relation.getBlock(curRelInd, j);
            }
            // build a heap with comparator based on fields
            TupleHeap heap = buildHeap(sortFields);
            // push all tuples in the main memory into heap
            offerTuplesFromMemToHeap(heap);
            clearMainMemory();
            int memBlockIndex = 0;
            // pull tuples out from heap to main memory one by one
            while (!heap.isEmpty()) {
                if (mainMemory.getBlock(memBlockIndex).isFull()) memBlockIndex++;
                Block memBlock = mainMemory.getBlock(memBlockIndex);
                memBlock.appendTuple(heap.poll());
            }
            // output 10 blocks (sorted list) back into disk
            // following the end of the relation
            for (int j = 0; j < numMemBlocks; j++) {
                int diskBlockInd = numRealBlocks + i * numMemBlocks + j;
                relation.setBlock(diskBlockInd, j);
            }
        }

        // merge sorted sublists in the disk
        int[] numTuplesRemainInMemBlock = new int[numSublists];
        TupleHeap2 tupleHeapWithDiskId = buildHeap2(sortFields);
        // add tuples of the first block of the list into heap with list ID
        for (int i = 0; i < numSublists; i++) {
            int diskListId = numRealBlocks + i * numMemBlocks;
            mainMemory.getBlock(0).clear();
            relation.getBlock(diskListId, 0);
            Block memBlock = mainMemory.getBlock(0);
            numTuplesRemainInMemBlock[i] = memBlock.getNumTuples();
            if (memBlock.getNumTuples() <= 0) continue;
            for (Tuple tuple : memBlock.getTuples()) {
                if (tuple.isNull()) continue;
                tupleHeapWithDiskId.offer(new TupleWithDiskId(tuple, diskListId));
            }
        }
        // use heap to fill memory
        clearMainMemory();
        int memBlockId = 0;  // read in tuple and offer to heap
        int storeMemBlockId = 1;  // store the tuple poll from heap
        int curDiskBlockId = 0;  // write back from the start of disk of original relation
        boolean isLastDiskBlockFull = false;

        while (!tupleHeapWithDiskId.isEmpty()) {
            TupleWithDiskId tuple2 = tupleHeapWithDiskId.poll();
            int diskId = tuple2.diskId;
            int listId = (diskId - numRealBlocks) / numMemBlocks;
            numTuplesRemainInMemBlock[listId]--;
            // if we have polled all tuples of a block in the disk list,
            // we add the next block in the disk list
            // into heap if there is one.
            if (numTuplesRemainInMemBlock[listId] == 0) {
                // not end of the sublist
                if ((diskId - numRealBlocks) % numMemBlocks != numMemBlocks - 1
                        && diskId + 1 < numRealBlocks * 2)
                {
                    mainMemory.getBlock(memBlockId).clear();
                    relation.getBlock(diskId + 1, memBlockId);
                    Block curMemBlock = mainMemory.getBlock(memBlockId);
                    if (!curMemBlock.isEmpty() && curMemBlock.getNumTuples() > 0) {
                        // read next block into memory
                        numTuplesRemainInMemBlock[listId] = curMemBlock.getNumTuples();
                        for (Tuple tuple : curMemBlock.getTuples()) {
                            if (tuple.isNull()) {
                                numTuplesRemainInMemBlock[listId]--;
                                continue;
                            }
                            tupleHeapWithDiskId.offer(new TupleWithDiskId(tuple, diskId + 1));
                        }
                    }
                }
            }
            // use polled tuple fill a mem block and put it back to disk
            mainMemory.getBlock(storeMemBlockId).appendTuple(tuple2.tuple);
            isLastDiskBlockFull = false;
            if (mainMemory.getBlock(storeMemBlockId).isFull()) {
                relation.setBlock(curDiskBlockId, storeMemBlockId);
                curDiskBlockId++;
                mainMemory.getBlock(storeMemBlockId).clear();
                isLastDiskBlockFull = true;
            }
        }

        // delete helper sublist, start from the last used block
        if (isLastDiskBlockFull) {
            relation.deleteBlocks(curDiskBlockId);
        } else {
            relation.deleteBlocks(curDiskBlockId + 1);
        }
    }

    private void onePassSort(Relation relation, ArrayList<String> sortFields) {
        clearMainMemory();
        int numBlocks = relation.getNumOfBlocks();
        relation.getBlocks(0, 0, numBlocks);
        TupleHeap heap = buildHeap(sortFields);
        offerTuplesFromMemToHeap(heap);

        clearMainMemory();
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

    // TODO start the hardest part for multi-join
    // SELECT * FROM course, course2 WHERE course.sid = course2.sid AND course.exam > course2.exam AND course.homework = 100

    private void optimizedSelection(ExpressionTree expressionTree, ParserContainer parserContainer) {
        // retrieve the expression tree nodes separated by AND
        ArrayList<ExpressionTreeNode> subConditions = ExpressionTree.getSubExpression(expressionTree).first;
        ArrayList<ExpressionTreeNode> nodeWithMultiTable = new ArrayList<>();
        for (ExpressionTreeNode expressionTreeNode : subConditions) {
            // find those relation coupled with selection
            ArrayList<String> relevantTables = getTableNames(expressionTreeNode);
            if (relevantTables.size() > 1) nodeWithMultiTable.add(expressionTreeNode);
        }
        ExpressionTreeNode expressionTreeNode = mergeNodes(nodeWithMultiTable);
        String mergeCondition = expressionTreeNode.getString(expressionTreeNode).trim();

        ArrayList<String> tempRelations = generateTempRelations(subConditions);
        parserContainer.setWhereCondition(mergeCondition);
        for (String tempName : tempRelations) {
            String originalName = tempName.replace("temp", "");
            for (int i = 0; i < parserContainer.getTables().size(); i++) {
                if (parserContainer.getTables().get(i).equals(originalName)) {
                    parserContainer.getTables().set(i, tempName);
                }
            }
        }
    }

    // TODO
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
    // use to retrieve table names list in the Conditions

    private ArrayList<String> getTableNames(ExpressionTreeNode expressionTreeNode) {
        ArrayList<String> tables = new ArrayList<>();
        if (expressionTreeNode == null) return tables;
        String curNodeValue = expressionTreeNode.getValue();
        String tableName = "";
        if (!curNodeValue.contains(".")) {
            tableName = curNodeValue;
        } else if (curNodeValue.contains(".")) {
            // just in case course2.exam, don't need attr_name, just table name
            tableName = curNodeValue.split("\\.")[0];
        }
        if (!tables.contains(tableName) && !Objects.equals(tableName, "=")) {
            tables.add(tableName);
        }
        tables.addAll(getTableNames(expressionTreeNode.getLeft()));
        tables.addAll(getTableNames(expressionTreeNode.getRight()));
        return tables;
    }

    // depends on conditions type, we may do the selection at first
    // return temp table with selection done
    private ArrayList<String> generateTempRelations(ArrayList<ExpressionTreeNode> subConditions) {
        ArrayList<String> tempTables = new ArrayList<>();
        HashMap<String, ArrayList<ExpressionTreeNode>> mapRelationToCondition = new HashMap<>();

        if (subConditions == null || subConditions.size() == 0) return tempTables;

        for (ExpressionTreeNode subCondition : subConditions) {
            ArrayList<String> tableList = getTableNames(subCondition);
            // we want condition like course.homework = 100
            if (tableList.size() != 1) continue;
            String table = tableList.get(0);
            if (mapRelationToCondition.containsKey(table)) {
                // may contain multiple conditions on same table
                ArrayList<ExpressionTreeNode> list = mapRelationToCondition.get(table);
                list.add(subCondition);
            } else {
                ArrayList<ExpressionTreeNode> list = new ArrayList<>();
                list.add(subCondition);
                mapRelationToCondition.put(table, list);
            }
        }
        for (String tableName : mapRelationToCondition.keySet()) {
            ArrayList<ExpressionTreeNode> conditions = mapRelationToCondition.get(tableName);
            createTempRelation(tableName, conditions);
            tempTables.add("temp" + tableName);
        }
        return tempTables;
    }

    private void createTempRelation(String table, ArrayList<ExpressionTreeNode> nodeList) {
        String relationName = "temp" + table;
        if (schemaManager.relationExists(relationName)) schemaManager.deleteRelation(relationName);
        ExpressionTreeNode expressionTreeNode;
        if (nodeList.size() > 1) {
            expressionTreeNode = mergeNodes(nodeList);
        } else {
            expressionTreeNode = nodeList.get(0);
        }

        Relation relation = schemaManager.getRelation(table);
        int relationBlocks = relation.getNumOfBlocks();
        int memBlocks = mainMemory.getMemorySize() - 1;
        int processedBlocks = 0;

        ArrayList<String> fieldNames = new ArrayList<String>();
        for (String str : relation.getSchema().getFieldNames()) {
            fieldNames.add(table + "." + str);
        }
        Relation relation_reference = schemaManager.createRelation(relationName,
                new Schema(fieldNames, relation.getSchema().getFieldTypes()));
        do {
            int blocksToMem = relationBlocks < memBlocks ? relationBlocks : memBlocks;
            //reads several blocks from the relation (the disk) and stores in the memory
            relation.getBlocks(processedBlocks, 0, blocksToMem);
            for (int i = 0; i < blocksToMem; i++) {
                Block block_reference = mainMemory.getBlock(i);
                // this is to handle the holes after deletion
                if (block_reference.getNumTuples() == 0) continue;
                for (Tuple t : block_reference.getTuples()) {
                    Tuple tuple = createNewTuple(relation_reference, t);
                    if (expressionTreeNode == null || ExpressionTree.checkCondition(tuple, expressionTreeNode))
                        insertTupleToDisk(tuple, relation_reference, mainMemory, memBlocks);
                }
            }
            relationBlocks -= blocksToMem;
            processedBlocks += blocksToMem;
        } while (relationBlocks > 0);
    }

    private Tuple createNewTuple(Relation relation_reference, Tuple tuple) {
        Tuple newtuple = relation_reference.createTuple();
        if (newtuple.getNumOfFields() != tuple.getNumOfFields())
            return null;
        for (int i = 0; i < tuple.getNumOfFields(); i++) {
            Field f = tuple.getField(i);
            if (f.type == FieldType.INT)
                newtuple.setField(i, f.integer);
            else
                newtuple.setField(i, f.str);
        }
        return newtuple;
    }

    // natural join related methods
    // e.g. course.id = course2.id satisfy natural join

    // replace normal cross join then
    private boolean isNaturalJoin(ExpressionTreeNode node) {
        boolean twoTable = getTableNames(node).size() == 2;
        boolean equals = node.getValue().equals("=");
        if (!twoTable || !equals) return false;
        String leftKey = node.getLeft().getValue().split("\\.")[1];
        String rightKey = node.getRight().getValue().split("\\.")[1];
        return leftKey.equals(rightKey);
    }

    private String naturalJoinTwoTables(String tableOne, String tableTwo, String condition) {
        clearMainMemory();
        Relation relationOne = schemaManager.getRelation(tableOne);
        Relation relationTwo = schemaManager.getRelation(tableTwo);
        Schema schemaOne = schemaManager.getSchema(tableOne);
        Schema schemaTwo = schemaManager.getSchema(tableTwo);
        // course.id = course2.id
        String fieldOne = condition.split("=")[0].trim();
        if (!schemaOne.getFieldNames().contains(fieldOne) && fieldOne.contains(".")) {
            fieldOne = fieldOne.split("\\.")[1];
        }
        String fieldTwo = condition.split("=")[1].trim();
        if (!schemaTwo.getFieldNames().contains(fieldTwo) && fieldTwo.contains(".")) {
            fieldTwo = fieldTwo.split("\\.")[1];
        }

        Schema tempSchema = joinTwoSchema(schemaOne, schemaTwo, tableOne, tableTwo);
        String tempRelationName = tableOne + "naturalJoin" + tableTwo;
        Relation tempRelation = schemaManager.createRelation(tempRelationName, tempSchema);
        // before natural join, we need to sort the joining tables
        ArrayList<String> sortFieldsOne = new ArrayList<>();
        ArrayList<String> sortFieldsTwo = new ArrayList<>();
        sortFieldsOne.add(fieldOne);
        sortFieldsTwo.add(fieldTwo);
        twoPassSort(relationOne, sortFieldsOne);
        twoPassSort(relationTwo, sortFieldsTwo);
        // use first Two mem blocks to read in tuples from table one and Two
        RelationIterator relationIteratorOne = new RelationIterator(relationOne, mainMemory, 0);
        RelationIterator relationIteratorTwo = new RelationIterator(relationTwo, mainMemory, 1);
        boolean isBegin = true;
        Tuple tupleOne = null;
        Tuple tupleTwo = null;
        while ((isBegin || (tupleOne != null && tupleTwo != null))) {
            if (isBegin) {
                tupleOne = relationIteratorOne.next();
                tupleTwo = relationIteratorTwo.next();
                isBegin = false;
            }
            int compare = compare(tupleOne, tupleTwo, fieldOne, fieldTwo);
            if (compare < 0) tupleOne = relationIteratorOne.next();
            else if (compare > 0) tupleTwo = relationIteratorTwo.next();
            else { // find same tuples in Two tables
                List<Tuple> sameTupleListOne = new ArrayList<>();
                List<Tuple> sameTupleListTwo = new ArrayList<>();
                Tuple preTupleOne = tupleOne;
                Tuple preTupleTwo = tupleTwo;
                sameTupleListOne.add(preTupleOne);
                sameTupleListTwo.add(preTupleTwo);
                while (tupleOne != null) {
                    // move next which may still be same
                    tupleOne = relationIteratorOne.next();
                    if (tupleOne == null) break;
                    if (compare(preTupleOne, tupleOne, fieldOne, fieldOne) == 0) {
                        sameTupleListOne.add(tupleOne);
                    } else break;
                }
                while (tupleTwo != null) {
                    // move next which may still be same
                    tupleTwo = relationIteratorTwo.next();
                    if (tupleTwo == null) break;
                    if (compare(preTupleTwo, tupleTwo, fieldTwo, fieldTwo) == 0) {
                        sameTupleListTwo.add(tupleTwo);
                    } else break;
                }
                for (Tuple sameTupleOne : sameTupleListOne) {
                    for (Tuple sameTupleTwo : sameTupleListTwo) {
                        Tuple joinedTuple = joinTwoTuples(sameTupleOne, sameTupleTwo, tempRelationName);
                        if (joinedTuple == null) continue;
                        insertTupleToDisk(joinedTuple, tempRelation, mainMemory, mainMemory.getMemorySize() - 1);
                    }
                }
            }
        }
        return tempRelationName;
    }

    // TODO short
    private Schema joinTwoSchema(Schema schemaOne, Schema schemaTwo, String tableOne, String tableTwo) {
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

    private Tuple joinTwoTuples(Tuple tupleOne, Tuple tupleTwo, String relationName) {
        if (tupleOne.getSchema().getNumOfFields() + tupleTwo.getSchema().getNumOfFields() > 8) {
            return null;
        }
        if (tupleOne.isNull() || tupleTwo.isNull()) {
            return null;
        }
        Relation tempRelation = schemaManager.getRelation(relationName);
        Tuple newTuple = tempRelation.createTuple();
        int i = 0;
        Field field;
        while (i < tupleOne.getNumOfFields()) {
            field = tupleOne.getField(i);
            if (field.type == FieldType.INT) {
                newTuple.setField(i, field.integer);
            } else {
                newTuple.setField(i, field.str);
            }
            i++;
        }
        while (i < tupleOne.getNumOfFields() + tupleTwo.getNumOfFields()) {
            field = tupleTwo.getField(i - tupleOne.getNumOfFields());
            if (field.type == FieldType.INT) {
                newTuple.setField(i, field.integer);
            } else {
                newTuple.setField(i, field.str);
            }
            i++;
        }
        return newTuple;
    }

    // when cross join, consider small relation first
    private int[] getJoinOrder(ArrayList<String> tableLists) {
        int[] joinOrder = null;
        int minSize = Integer.MAX_VALUE;
        List<int[]> permutations = getAllPerms(tableLists.size());
        for (int[] perm : permutations) {
            int curSize = getTotalJoinSize(tableLists, perm);
            if (curSize < minSize) {
                minSize = curSize;
                joinOrder = perm;
            }
        }
        return joinOrder;
    }

    private List<int[]> getAllPerms(int size) {
        List<int[]> result = new ArrayList<>();
        boolean[] used = new boolean[size];
        permHelper(result, 0, new int[size], used);
        return result;
    }

    private void permHelper(List<int[]> result, int start, int[] arr, boolean[] used) {
        if (start == used.length) {
            result.add(arr.clone());
            return;
        }
        for (int i = 0; i < used.length; i++) {
            if (used[i]) continue;
            used[i] = true;
            arr[start] = i;
            permHelper(result, start + 1, arr, used);
            used[i] = false;
        }
    }

    private int getTotalJoinSize(ArrayList<String> tableLists, int[] perm) {
        int[] originSizeArr = new int[tableLists.size()];
        for (int i = 0; i < tableLists.size(); i++) {
            originSizeArr[i] = schemaManager.getRelation(tableLists.get(i)).getNumOfTuples();
        }
        int totalSize = 0;
        int runningSize = originSizeArr[perm[0]];
        for (int i = 1; i < perm.length - 1; i++) {
            runningSize *= originSizeArr[perm[i]];
            totalSize += runningSize;
        }
        return totalSize;
    }

    private String crossJoinTables(ArrayList<String> tableLists, boolean isDistinct,
                                   Map<String, ArrayList<String>> tableAttrMap) {
        clearMainMemory();
        if (tableLists.size() == 1 ) {
            return tableLists.get(0);
        }
        ArrayList<String> tempTables = new ArrayList<>();
        //find the best join order for tables to reduce internal size
        int[] joinOrder = getJoinOrder(tableLists);
        // sort tables based if distinct
        if (isDistinct) {
            for (String table : tableLists) {
                if (tableAttrMap.containsKey(table)) {
                    sortTuplesInRelation(schemaManager.getRelation(table), tableAttrMap.get(table));
                }
            }
        }
        int numTotalBlocks = 0;
        for (String table : tableLists) {
            numTotalBlocks += schemaManager.getRelation(table).getNumOfBlocks();
        }
        // if total size of tables is smaller than memory
        if (numTotalBlocks <= mainMemory.getMemorySize()) {
            Relation relationOne = schemaManager.getRelation(tableLists.get(0));
            Schema tmpSchema = relationOne.getSchema();
            String tmpName = tableLists.get(0);
            Map<String, ArrayList<Tuple>> tableMap = new HashMap<>();
            for (int i = 0; i < tableLists.size(); i++) {
                tableMap.put(tableLists.get(i), new ArrayList<>());
                storeTuplesToList(tableMap.get(tableLists.get(i)), schemaManager.getRelation(tableLists.get(i)));
            }
            List<String> relationsToDel = new ArrayList<>();
            for (int i = 1; i < tableLists.size(); i++) {
                Schema nextTmpSchema = joinTwoSchema(tmpSchema, schemaManager.getSchema(tableLists.get(i)), tmpName, tableLists.get(i));
                String nextTmpName = tmpName + "CrossJoin" + tableLists.get(i);
                schemaManager.createRelation(nextTmpName, nextTmpSchema);
                tableMap.put(nextTmpName, new ArrayList<>());
                for (Tuple tupleOne : tableMap.get(tmpName)) {
                    if (tupleOne == null || tupleOne.isNull()) {
                        continue;
                    }
                    for (Tuple tupleTwo : tableMap.get(tableLists.get(i))) {
                        if (tupleTwo == null || tupleTwo.isNull()) {
                            continue;
                        }
                        Tuple jointTuple = joinTwoTuples(tupleOne, tupleTwo, nextTmpName);
                        tableMap.get(nextTmpName).add(jointTuple);
                    }
                }
                tmpName = nextTmpName;
                tmpSchema = nextTmpSchema;
                relationsToDel.add(tmpName);
            }
            for (int i = 0; i < relationsToDel.size() - 1; i++) {
                schemaManager.deleteRelation(relationsToDel.get(i));
            }
            for (Tuple tuple : tableMap.get(tmpName)) {
                insertTupleToDisk(tuple, schemaManager.getRelation(tmpName), mainMemory, 5);
            }
            return tmpName;
        }

        String nameOfTempTable = tableLists.get(joinOrder[0]);
        for (int i = 1; i < tableLists.size(); i++) {
            nameOfTempTable = crossJoinTwoTables(nameOfTempTable, tableLists.get(joinOrder[i]), isDistinct, tableAttrMap);
            if (i != tableLists.size() - 1) {
                tempTables.add(nameOfTempTable);
            }
        }
        for (String tempTable : tempTables) {
            schemaManager.deleteRelation(tempTable);
        }
        return nameOfTempTable;
    }

    private String crossJoinTwoTables(String tableOne, String tableTwo, boolean isDistinct,
                                      Map<String, ArrayList<String>> tableAttrMap) {
        Relation relationOne = schemaManager.getRelation(tableOne);
        Relation relationTwo = schemaManager.getRelation(tableTwo);
        int mainMemorySize = mainMemory.getMemorySize();
        int tableOneSize = relationOne.getNumOfBlocks();
        int tableTwoSize = relationTwo.getNumOfBlocks();
        // one pass condition
        if (tableOneSize >= mainMemorySize && tableTwoSize >= mainMemorySize) {
            return twoPassJoinTwoTables(tableOne, tableTwo, isDistinct, tableAttrMap);
        }
        return onePassCrossJoinTwoTables(tableOne, tableTwo, isDistinct, tableAttrMap);
    }

    private String twoPassJoinTwoTables(String tableOne, String tableTwo, boolean isDistinct,
                                        Map<String, ArrayList<String>> tableAttrMap) {
        Relation relationOne = schemaManager.getRelation(tableOne);
        Relation relationTwo = schemaManager.getRelation(tableTwo);

        Schema tempSchema = joinTwoSchema(relationOne.getSchema(), relationTwo.getSchema(), tableOne, tableTwo);
        String tempRelationName = tableOne + "CrossJoin" + tableTwo;
        Relation tempRelation = schemaManager.createRelation(tempRelationName, tempSchema);
        int memListOnePos = 2;
        int memListTwoPos = 1;
        int memTempPos = 0;
        int memSize = mainMemory.getMemorySize();
        int numGroups = relationOne.getNumOfBlocks() / (memSize - 2);
        FieldTuple preFieldTupleOne = null;
        FieldTuple preFieldTupleTwo;
        for (int i = 0; i <= numGroups; i++) {
            if (i * numGroups >= relationOne.getNumOfBlocks()) {
                break;
            }
            clearMainMemory();
            relationOne.getBlocks(i * numGroups, memListOnePos,
                    Math.min(memSize - 2, relationOne.getNumOfBlocks() - i * numGroups));
            preFieldTupleTwo = null;
            for (int j = 0; j < relationTwo.getNumOfBlocks(); j++) {
                mainMemory.getBlock(memListTwoPos).clear();
                relationTwo.getBlock(j, memListTwoPos);
                if (mainMemory.getBlock(memListTwoPos).isEmpty()) {
                    continue;
                }
                if (j != 0) {
                    preFieldTupleOne = null;
                }
                for (int k = memListOnePos; k < memSize; k++) {
                    if (mainMemory.getBlock(k).isEmpty()) {
                        continue;
                    }
                    for (Tuple tupleOne : mainMemory.getBlock(k).getTuples()) {
                        if (tupleOne.isNull()) {
                            continue;
                        }
                        // skip duplicates
                        if (isDistinct && !tableOne.contains("CrossJoin")) {
                            FieldTuple curFieldTupleOne = new FieldTuple(tupleOne, tableAttrMap.get(tableOne));
                            if (preFieldTupleOne != null && preFieldTupleOne.equals(curFieldTupleOne)) {
                                continue;
                            }
                            preFieldTupleOne = curFieldTupleOne;
                        }
                        for (Tuple tupleTwo : mainMemory.getBlock(memListTwoPos).getTuples()) {
                            if (tupleTwo.isNull()) {
                                continue;
                            }
                            // skip duplicates
                            if (isDistinct && !tableTwo.contains("CrossJoin")) {
                                FieldTuple curFieldTupleTwo = new FieldTuple(tupleTwo,
                                        tableAttrMap.get(tableTwo));
                                if (preFieldTupleTwo != null && preFieldTupleTwo.equals(curFieldTupleTwo)) {
                                    continue;
                                }
                                preFieldTupleTwo = curFieldTupleTwo;
                            }
                            Tuple joinedTuple = joinTwoTuples(tupleOne, tupleTwo, tempRelationName);
                            if (joinedTuple != null) {
                                mainMemory.getBlock(memTempPos).clear();
                                insertTupleToDisk(joinedTuple, tempRelation, mainMemory, memTempPos);
                            }
                        }
                    }
                }
            }
        }
        clearMainMemory();
        return tempRelationName;
    }

    private String onePassCrossJoinTwoTables(String tableOne, String tableTwo, boolean isDistinct,
                                             Map<String, ArrayList<String>> tableAttrMap) {
        Relation relationOne = schemaManager.getRelation(tableOne);
        Relation relationTwo = schemaManager.getRelation(tableTwo);
        int mainMemorySize = mainMemory.getMemorySize();
        int tableOneSize = relationOne.getNumOfBlocks();
        int tableTwoSize = relationTwo.getNumOfBlocks();
        // one pass condition
        if (tableOneSize >= mainMemorySize && tableTwoSize >= mainMemorySize) {
            return null;
        }
        Relation tableToBeDumped, tableToBeProcessedByTuple;
        String tableToBeDumpedName, tableToBeProcessedByTupleName;
        if (tableOneSize < tableTwoSize) {
            tableToBeDumped = relationOne;
            tableToBeProcessedByTuple = relationTwo;
            tableToBeDumpedName = tableOne;
            tableToBeProcessedByTupleName = tableTwo;
        } else {
            tableToBeDumped = relationTwo;
            tableToBeProcessedByTuple = relationOne;
            tableToBeDumpedName = tableTwo;
            tableToBeProcessedByTupleName = tableOne;
        }
        // remove duplicates
        Set<FieldTuple> uniqueTupleSet;
        uniqueTupleSet = new HashSet<>();
        Schema schemaTableToBeDumped = tableToBeDumped.getSchema();
        Schema schemaTableToBeProcessedByTuple = tableToBeProcessedByTuple.getSchema();
        Schema tempSchema = joinTwoSchema(schemaTableToBeDumped, schemaTableToBeProcessedByTuple, tableToBeDumpedName,
                tableToBeProcessedByTupleName);
        String tempRelationName = tableToBeDumpedName + "CrossJoin" + tableToBeProcessedByTupleName;
        Relation tempRelation = schemaManager.createRelation(tempRelationName, tempSchema);
        clearMainMemory();
        tableToBeDumped.getBlocks(0, 0, tableToBeDumped.getNumOfBlocks());
        FieldTuple preLargeTuple = null;
        for (int i = 0; i < tableToBeProcessedByTuple.getNumOfBlocks(); i++) {
            // clear mem block because, if the block we get is empty, mem block
            // won't be cleared
            mainMemory.getBlock(mainMemorySize - 1).clear();
            tableToBeProcessedByTuple.getBlock(i, mainMemorySize - 1); // read a
            // block
            // to
            // block
            // 9
            Block blockLargerTable = mainMemory.getBlock(mainMemorySize - 1);
            ArrayList<Tuple> joinedTuples = new ArrayList<>();
            for (Tuple tupleLargerTable : blockLargerTable.getTuples()) { // process
                // by
                // each
                // tuple
                if (tupleLargerTable.isNull()) {
                    continue;
                }
                // skip duplicates
                if (isDistinct && !tableToBeProcessedByTupleName.contains("CrossJoin")) {
                    FieldTuple curCustomTuple = new FieldTuple(tupleLargerTable,
                            tableAttrMap.get(tableToBeProcessedByTupleName));
                    if (preLargeTuple != null && preLargeTuple.equals(curCustomTuple)) {
                        continue;
                    }
                    preLargeTuple = curCustomTuple;
                }
                uniqueTupleSet = new HashSet<>();
                // don't process last mem block
                for (int j = 0; j < mainMemory.getMemorySize() - 1; j++) {
                    Block blockSmallerTable = mainMemory.getBlock(j);
                    if (!blockSmallerTable.isEmpty()) { // Only process unempty
                        // blocks
                        for (Tuple tupleSmallerTable : blockSmallerTable.getTuples()) {
                            if (tupleSmallerTable.isNull()) {
                                continue;
                            }
                            // optimize join, push distinct downwards
                            if (isDistinct) {
                                FieldTuple fieldTuple = new FieldTuple(tupleSmallerTable,
                                        tupleSmallerTable.getSchema().getFieldNames());
                                if (uniqueTupleSet.contains(fieldTuple)) {
                                    continue;
                                }
                                uniqueTupleSet.add(fieldTuple);
                            }
                            Tuple joinedTuple = joinTwoTuples(tupleSmallerTable, tupleLargerTable, tempRelationName);
                            if (joinedTuple != null) {
                                joinedTuples.add(joinedTuple);
                            }
                        }
                    }
                }
            }
            mainMemory.getBlock(mainMemorySize - 1).clear();
            for (Tuple tuple : joinedTuples) {
                insertTupleToDisk(tuple, tempRelation, mainMemory, mainMemorySize - 1);
            }
            mainMemory.getBlock(mainMemorySize - 1).clear();
        }
        clearMainMemory();
        return tempRelationName;
    }

    private void storeTuplesToList(ArrayList<Tuple> list, Relation relation) {
        int numMemBlocks = mainMemory.getMemorySize();
        int numRelationBlocks = relation.getNumOfBlocks();
        int curDiskId = 0;
        do {
            int numBlocks = numMemBlocks > numRelationBlocks ? numRelationBlocks : numMemBlocks;
            relation.getBlocks(curDiskId, 0, numBlocks);
            for (int i = 0; i < numBlocks; i++) {
                Block block_reference = mainMemory.getBlock(i);
                // this is to handle the holes after deletion
                if (block_reference.getNumTuples() == 0) continue;
                list.addAll(block_reference.getTuples());
            }
            numRelationBlocks -= numBlocks;
            curDiskId += numBlocks;
        } while (numRelationBlocks > 0);
    }
}
