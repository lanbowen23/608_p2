package sql608;


import sql608.helper.*;
import storageManager.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;

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

    private void clearMainMemory() {
        int numberOfBlocks = mainMemory.getMemorySize();
        for (int i = 0; i < numberOfBlocks; i++) {
            mainMemory.getBlock(i).clear();
        }
    }

    private void appendTuple(Relation relation, MainMemory mem, int memory_block_index, Tuple tuple) {
        Block block;
        if (relation.getNumOfBlocks() == 0) {
            // relation in the disk is empty at first
            block = mem.getBlock(memory_block_index);
            block.clear(); // clear the block
            block.appendTuple(tuple); // append the tuple
            relation.setBlock(relation.getNumOfBlocks(), memory_block_index);
        } else {
            relation.getBlock(relation.getNumOfBlocks() - 1, memory_block_index);
            block = mem.getBlock(memory_block_index);

            if (block.isFull()) {
                // if current reading is a full block, write to next block on the disk
                block.clear(); // clear the block
                block.appendTuple(tuple); // append the tuple
                relation.setBlock(relation.getNumOfBlocks(), memory_block_index);
            } else {
                block.appendTuple(tuple); // append the tuple
                relation.setBlock(relation.getNumOfBlocks() - 1, memory_block_index);
            }
        }
    }

    private void outputTuples(ParserTree parserTree, Relation relation, ArrayList<String> fieldNames) {
        int numOfRelationBlocks = relation.getNumOfBlocks();
        int numOfRows = 0;
        String prev = "nullPrev";
        if (parserTree.getTable() != null
            && !parserTree.getTable().contains("CrossJoin")
            && !parserTree.getTable().contains("naturalJoin"))
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
                if (parserTree.isWhere()) {
                    ExpressionTree expressionTree =
                            new ExpressionTree(parserTree.getWhereCondition(), parserTree.getTable());
                    if (!ExpressionTree.checkCondition(tuple, expressionTree.getRoot())) continue;
                }

                StringBuilder sb = new StringBuilder();
                for (String field : fieldNames) {
                    String val = tuple.getField(field).toString();
                    if (val.equals("-2147483648") || val.equals("null")) val = "NULL";
                    sb.append(val).append("\t");
                }
                String cur = sb.toString();
                // handle distinct
                if (parserTree.isDistinct() && cur.equals(prev)) continue;
                prev = cur;
                System.out.println(cur);
                numOfRows++;
            }
        }
        System.out.println("---------------------------");
        System.out.println(numOfRows + " rows of results");
    }

    private void outputTuples(ParserTree parserTree, ArrayList<String> selectedFieldNamesList,
                              ArrayList<Tuple> selectedTuples) {
        String prev = "nullPrev";
        int numberOfRows = 0;
        if (parserTree.getTable() != null && !parserTree.getTable().contains("CrossJoin"))
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
            if (tuple.isNull()) {
                continue;
            }
            StringBuilder sb = new StringBuilder();
            for (String field : selectedFieldNamesList) {
                sb.append(tuple.getField(field)).append("\t");
            }
            String cur = sb.toString();
            if (parserTree.isDistinct() && cur.equals(prev)) {
                continue;
            }
            prev = cur;
            System.out.println(cur);
            numberOfRows++;
        }
        System.out.println("-------------------------------------------------------");
        System.out.println(numberOfRows + " rows of results");
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

    public void exec(String sql) {
        String action = sql.trim().toLowerCase().split("[\\s]+")[0];
        try {
            if (action.equals("create")) {
                this.createQuery(sql);
            } else if (action.equals("drop")) {
                this.dropQuery(sql);
            } else if (action.equals("offer")) {
                this.insertQuery(sql);
            } else if (action.equals("delete")) {
//                this.deleteQuery(sql);
            } else if (action.equals("select")) {
                this.selectQuery(sql);
            } else if ("source".equalsIgnoreCase(action)) {
                String file = sql.trim().toLowerCase().split("[\\s]+")[1];
                executeFile(file);
            } else {
                System.out.println("Invalid query, try again");
            }
        } catch (Exception e) {
            System.out.println("Invalid query, try again");
        }
        System.out.println("-------------------------------------------------------");
    }

    private void createQuery(String sql) throws Exception {
        three<String, ArrayList<String>, ArrayList<FieldType>> three = parser.parseCreate(sql);
        String tableName = three.first;
        Schema schema = new Schema(three.second, three.third);
        schemaManager.createRelation(tableName, schema);
    }

    private void dropQuery(String sql) {
        schemaManager.deleteRelation(parser.parseDrop(sql));
    }

    private void insertQuery(String sql) {
        if (!sql.trim().toLowerCase().contains("select")) {
            insertQueryForSingleTuple(sql);
        } else {
            insertQueryForSelect(sql);
        }
    }

    private void selectQuery(String sql) {
        double startTime = System.currentTimeMillis();
        long startDiskIO = disk.getDiskIOs();
        ParserTree parserTree = parser.parseSelect(sql);
        if (parserTree.getTables() == null) {
            selectQueryForSingleTable(parserTree);
        } else {
//            selectQueryForMultipleTables(parserTree);
        }
        double stopTime = System.currentTimeMillis();
        double timeSpent = (stopTime - startTime)/1000;
        long stopDiskIO = disk.getDiskIOs();
        long diskIOTaken = stopDiskIO - startDiskIO;
        System.out.println("Execution time: " + timeSpent + "seconds");
        System.out.println("Disk IO taken: " + diskIOTaken);
        System.out.println();
    }

    private void deleteQuery(String stmt) {
        ParserTree parserTree = parser.parseDelete(stmt);
        String tableName = parserTree.getTable();
        String whereCondition = parserTree.getWhereCondition();
        Relation relation = schemaManager.getRelation(tableName);
        int numberOfMemoryBlocks = mainMemory.getMemorySize();
        int numberOfRelationBlocks = relation.getNumOfBlocks();
        int currentIndexOfBlocks = 0;
        while (numberOfRelationBlocks > 0) {
            // Operate until all the blocks of this relation in the disk has
            // been checked
            int numberOfBlocksToMemory = Math.min(numberOfMemoryBlocks, numberOfRelationBlocks);
            relation.getBlocks(currentIndexOfBlocks, 0, numberOfBlocksToMemory);
            for (int i = 0; i < numberOfBlocksToMemory; i++) {
                Block block = mainMemory.getBlock(i);
                if (block.getNumTuples() == 0) // Empty block
                    continue;
                ArrayList<Tuple> tuples = block.getTuples();
                if (parserTree.isWhere()) {
                    for (int j = 0; j < tuples.size(); j++) {
                        ExpressionTree expreessionTree = new ExpressionTree(whereCondition, parserTree.getTable());
                        Tuple tuple = tuples.get(j);
                        if (expreessionTree.checkCondition(tuple, expreessionTree.getRoot()))
                            block.invalidateTuple(j);
                    }
                } else {
                    block.invalidateTuples();
                }
            }
            relation.setBlocks(currentIndexOfBlocks, 0, numberOfBlocksToMemory);
            numberOfRelationBlocks -= numberOfBlocksToMemory;
            currentIndexOfBlocks += numberOfBlocksToMemory;
        }
        System.out.println("Number of blocks of " + tableName + ": " + relation.getNumOfBlocks());
        System.out.println("Number of tuples of " + tableName + ": " + relation.getNumOfTuples());
        // delete possible hole, it's too slow, will do it while select
        // twoPassSort(relation, relation.getSchema().getFieldName(0));
    }

    private void insertQueryForSingleTuple(String sql) {
        clearMainMemory();
        three<String, ArrayList<String>, ArrayList<Field>> three = parser.parseInsertOneTuple(sql, null);
        String relationName = three.first;
        Relation relation = schemaManager.getRelation(relationName);
        three = parser.parseInsertOneTuple(sql, relation);
        if (relation == null) { return; }
        Tuple tuple = relation.createTuple(); // creates an empty tuple of the schema
        ArrayList<String> filedNames = three.second;
        ArrayList<Field> fields = three.third;
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
        appendTuple(relation, mainMemory, 5, tuple);
        System.out.println("Number of blocks of " + relationName + ": " + relation.getNumOfBlocks());
        System.out.println("Number of tuples of " + relationName + ": " + relation.getNumOfTuples());
    }

    private void insertQueryForSelect(String sql) {
        two<String, ParserTree> pairOutput = parser.parseInsertForSelect(sql);
        String tableToBeInserted = pairOutput.first;
        ParserTree parserTree = pairOutput.second;
        Relation toRelation = schemaManager.getRelation(tableToBeInserted);
        String fromTableName = parserTree.getTable();
        Relation fromRelation = schemaManager.getRelation(fromTableName);
        ArrayList<String> selectedAttributes = parserTree.getAttributes();
        ArrayList<Tuple> selectedTuples = new ArrayList<>();
        if (fromRelation == null || selectedAttributes.size() == 0) {
            // output is null
            return;
        }
        clearMainMemory();
        int relationNumOfBlocks = fromRelation.getNumOfBlocks();
        // number of tuples of this relation in disk
        int memoryNumOfBlocks = mainMemory.getMemorySize();// equals to 10
        ArrayList<String> selectedFieldNamesList = new ArrayList<>();
        if (selectedAttributes.size() == 1 && selectedAttributes.get(0).equals("*")) {
            // no projection
            selectedFieldNamesList = fromRelation.getSchema().getFieldNames();
        } else {
            selectedFieldNamesList = selectedAttributes;
        }
        // if not distinct or order by condition, just print result without
        // storing
        if (!parserTree.isDistinct() && !parserTree.isOrder()) {
            for (int i = 0; i < relationNumOfBlocks; i++) {
                mainMemory.getBlock(0).clear();
                fromRelation.getBlock(i, 0);// read a block from disk to main
                // memory
                Block mainMemoryBlock = mainMemory.getBlock(0);
                if (mainMemoryBlock.getNumTuples() == 0) {
                    continue;
                }
                for (Tuple tuple : mainMemoryBlock.getTuples()) {
                    if (parserTree.isWhere()) {
                        ExpressionTree expressionTree = new ExpressionTree(parserTree.getWhereCondition(),
                                parserTree.getTable());
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
                    appendTuple(toRelation, mainMemory, 5, newTuple);
                }
            }
            return;
        }
        System.out.println("Number of blocks of " + tableToBeInserted + ": " + toRelation.getNumOfBlocks());
        System.out.println("Number of tuples of " + tableToBeInserted + ": " + toRelation.getNumOfTuples());
    }

    private void selectQueryForSingleTable(ParserTree parserTree) {
        // only one table now
        String tableName = parserTree.getTable();
        Relation relation = schemaManager.getRelation(tableName);
        ArrayList<String> attributes = parserTree.getAttributes();

        // output is null
        if (relation == null || attributes.size() == 0) return;
        // number of tuples of this relation in disk
        int numOfRelationBlocks = relation.getNumOfBlocks();
        int memoryNumOfBlocks = mainMemory.getMemorySize();
        ArrayList<String> fieldNames;
        if (attributes.size() == 1 && attributes.get(0).equals("*")) {
            fieldNames = relation.getSchema().getFieldNames();
        } else {
            fieldNames = attributes;
        }
        // if not distinct or order by condition, just print result without storing
        // SELECT * FROM course
        if (!parserTree.isDistinct() && !parserTree.isOrder()) {
            outputTuples(parserTree, relation, fieldNames);
            return;
        }

        // one pass algo for single table
        ArrayList<Tuple> selectedTuples = new ArrayList<>();
        if (numOfRelationBlocks <= memoryNumOfBlocks) {
            for (int i = 0; i < numOfRelationBlocks; i++) {
                // read one block from disk to main memory
                relation.getBlock(i, 0);
                Block block = mainMemory.getBlock(0);
                if (block.getNumTuples() == 0) continue;
                for (Tuple tuple : block.getTuples()) {
                    if (parserTree.isWhere()) {
                        ExpressionTree expressionTree = new ExpressionTree(parserTree.getWhereCondition(),
                                parserTree.getTable());
                        if (ExpressionTree.checkCondition(tuple, expressionTree.getRoot())) {
                            selectedTuples.add(tuple);
                        }
                    } else selectedTuples.add(tuple);
                }
            }
            if (parserTree.isDistinct()) {
                // selected tuples won't exceed one block size
                selectedTuples = onePassDuplicateElimination(selectedTuples, fieldNames);
            }
            if (parserTree.isOrder()) {
                ArrayList<String> sortFields = new ArrayList<>();
                sortFields.add(parserTree.getOrderAttribute());
                selectedTuples = onePassOrder(selectedTuples, sortFields);
            }
            outputTuples(parserTree, fieldNames, selectedTuples);
            return;
        }

        // two pass single table
        if (parserTree.isOrder()) {
            ArrayList<String> sortFields = new ArrayList<>();
            String orderAttr = parserTree.getOrderAttribute();
            sortFields.add(orderAttr);
            for (String sortField : fieldNames) {
                if (!sortField.equals(orderAttr)) {
                    sortFields.add(sortField);
                }
            }
            twoPassSort(relation, sortFields);
        }
        else twoPassSort(relation, fieldNames);
        // output tuples
        outputTuples(parserTree, relation, fieldNames);
        return;
    }

    private ArrayList<Tuple> onePassDuplicateElimination(ArrayList<Tuple> selectedTuples,
                                                         ArrayList<String> selectedFieldNamesList) {
        // selected tuples < main memory
        if (!validateOnePass(selectedTuples)) return null;

        ArrayList<Tuple> result = new ArrayList<>();
        clearMainMemory();
        mainMemory.setTuples(0, selectedTuples);
        // use hash to eliminate duplicate
        HashSet<CustomedTuple> set = new HashSet<>();
        int memoryNumberOfBlocks = mainMemory.getMemorySize();
        for (int i = 0; i < memoryNumberOfBlocks; i++) {
            Block block = mainMemory.getBlock(i);
            if (!block.isEmpty()) {
                for (Tuple tuple : block.getTuples()) {
                    CustomedTuple uniqueTuple = new CustomedTuple(tuple, selectedFieldNamesList);
                    if (set.add(uniqueTuple)) {
                        result.add(tuple);
                    }
                }
            }
        }
        return result;
    }

    private boolean validateOnePass(ArrayList<Tuple> selectedTuples) {
        int numberOfFieldsPerTuple = selectedTuples.get(0).getNumOfFields();
        int max_capacity = (8 / numberOfFieldsPerTuple) * mainMemory.getMemorySize();
        return max_capacity >= selectedTuples.size();
    }

    private ArrayList<Tuple> onePassOrder(ArrayList<Tuple> selectedTuples, ArrayList<String> sortFields) {
        if (!validateOnePass(selectedTuples)) return null;
        clearMainMemory();
        mainMemory.setTuples(0, selectedTuples);
        ArrayList<Tuple> result = new ArrayList<>();
        // set up a heap to get ordered tuples
        TupleHeap heap = getMinHeap(sortFields);
        offerTuplesFromMemToHeap(heap);
        while (!heap.isEmpty()) result.add(heap.poll());
        return result;
    }

    private TupleHeap getMinHeap(ArrayList<String> sortFields) {
        // By default ORDER BY sorts the data in ascending order
        // so I want a Heap which returns minimum
        // for TupleHeap we need a comparator
        return new TupleHeap((o1, o2) -> {
            for (String sortField : sortFields) {
                Field field1 = o1.getField(sortField);
                Field field2 = o2.getField(sortField);
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
            // for equal cases
            return 0;
        });
    }

    private void offerTuplesFromMemToHeap(TupleHeap heap) {
        int memoryNumberOfBlocks = mainMemory.getMemorySize();
        for (int i = 0; i < memoryNumberOfBlocks; i++) {
            Block block = mainMemory.getBlock(i);
            if (!block.isEmpty() && block.getNumTuples() > 0) {
                for (Tuple tuple : block.getTuples()) {
                    if (tuple.isNull()) continue;
                    heap.offer(tuple);
                }
            }
        }
    }

    // two pass sort algo for single table
    private void twoPassSort(Relation relation, ArrayList<String> sortFields) {
        int numRealBlocks = relation.getNumOfBlocks();
        int numMemBlocks = mainMemory.getMemorySize();
        int numSublists = (numRealBlocks % numMemBlocks == 0) ? numRealBlocks / numMemBlocks
                : numRealBlocks / numMemBlocks + 1;
            for (int i = 0; i < relation.getNumOfBlocks(); i++) {
                relation.getBlock(i, 0);
                Block curMemBlock = mainMemory.getBlock(0);
            }
        for (int i = 0; i < numSublists; i++) {
            // get a sublist into main memory
            clearMainMemory();
            for (int j = 0; j < numMemBlocks; j++) {
                int offset = i * numMemBlocks + j;
                if (offset >= numRealBlocks) {
                    break;
                }
                relation.getBlock(offset, j);
            }

            // sort this sublist
            TupleHeap heap = getMinHeap(sortFields);
            // store all tuples in mem to heap
            offerTuplesFromMemToHeap(heap);
            int memBlockIndex = 0;
            // transfer sorted tuple list into main memory
            clearMainMemory();
            while (!heap.isEmpty()) {
                if (mainMemory.getBlock(memBlockIndex).isFull()) {
                    memBlockIndex++;
                }
                Block curMemBlock = mainMemory.getBlock(memBlockIndex);
                curMemBlock.appendTuple(heap.poll());
            }
            // put blocks back into disk, but at end of the relation
            for (int j = 0; j < numMemBlocks; j++) {
                int offset = numRealBlocks + i * numMemBlocks + j;
                if (offset >= numRealBlocks * 2) {
                    break;
                }
                relation.setBlock(offset, j);
            }
        }
        for (int i = 0; i < relation.getNumOfBlocks(); i++) {
            mainMemory.getBlock(0).clear();
            relation.getBlock(i, 0);
        }
        // merge sorted sublists
        int[] numTuplesRemainInBlock = new int[numSublists];
        TupleWithBlockIdHeap tupleWithBlockIdHeap = getMinTupleWithBlockIdHeap(sortFields);
        // add tuples of the first block into heap
        for (int i = 0; i < numSublists; i++) {
            int offset = numRealBlocks + i * numMemBlocks;
            mainMemory.getBlock(0).clear();
            relation.getBlock(offset, 0);
            Block curMemBlock = mainMemory.getBlock(0);
            numTuplesRemainInBlock[i] = curMemBlock.getNumTuples();
            if (curMemBlock.getNumTuples() <= 0) {
                continue;
            }
            for (Tuple tuple : curMemBlock.getTuples()) {
                if (tuple.isNull()) {
                    continue;
                }
                tupleWithBlockIdHeap.offer(new TupleWithBlockId(tuple, offset));
            }
        }
        // use heap to fill memory
        clearMainMemory();
        int curMemBlockId = 0;
        int curDiskBlockId = 0;
        int storeMemBlockId = 1;
        boolean isLastFull = false;

        while (!tupleWithBlockIdHeap.isEmpty()) {
            TupleWithBlockId tupleWithBlockId = tupleWithBlockIdHeap.poll();
            int blockId = tupleWithBlockId.blockId;
            int sublistId = (blockId - numRealBlocks) / numMemBlocks;
            numTuplesRemainInBlock[sublistId]--;
            // if we have polled all tuples of a block, we add the next block in
            // the sublist
            // into heap if there is one.
            if (numTuplesRemainInBlock[sublistId] == 0) {
                // not end of the sublist
                if ((blockId - numRealBlocks) % numMemBlocks != numMemBlocks - 1 && blockId + 1 < numRealBlocks * 2) {
                    mainMemory.getBlock(curMemBlockId).clear();
                    relation.getBlock(blockId + 1, curMemBlockId);
                    // not empty
                    Block curMemBlock = mainMemory.getBlock(curMemBlockId);
                    if (!curMemBlock.isEmpty() && curMemBlock.getNumTuples() > 0) {
                        numTuplesRemainInBlock[sublistId] = curMemBlock.getNumTuples();
                        for (Tuple tuple : curMemBlock.getTuples()) {
                            if (tuple.isNull()) {
                                numTuplesRemainInBlock[sublistId]--;
                                continue;
                            }
                            tupleWithBlockIdHeap.offer(new TupleWithBlockId(tuple, blockId + 1));
                        }
                    }
                }
            }
            // use polled tuple fill a mem block and put it back to disk
            mainMemory.getBlock(storeMemBlockId).appendTuple(tupleWithBlockId.tuple);
            isLastFull = false;
            if (mainMemory.getBlock(storeMemBlockId).isFull()) {
                relation.setBlock(curDiskBlockId, storeMemBlockId);
                curDiskBlockId++;
                mainMemory.getBlock(storeMemBlockId).clear();
                // relation.setBlock(curDiskBlockId, storeMemBlockId);
                isLastFull = true;
            }
        }
        // delete helper sublist, start from the last used block
        if (isLastFull) {
            relation.deleteBlocks(curDiskBlockId);
        } else {
            relation.deleteBlocks(curDiskBlockId + 1);
        }
    }

    private TupleWithBlockIdHeap getMinTupleWithBlockIdHeap(ArrayList<String> sortFields) {
        return new TupleWithBlockIdHeap(new Comparator<TupleWithBlockId>() {
            @Override
            public int compare(TupleWithBlockId o1, TupleWithBlockId o2) {
                for (String sortField : sortFields) {
                    Field field1 = o1.tuple.getField(sortField);
                    Field field2 = o2.tuple.getField(sortField);
                    if (field1.type == FieldType.INT) {
                        if (field1.integer != field2.integer) {
                            return ((Integer) field1.integer).compareTo(field2.integer);
                        }
                    } else if (field1.type == FieldType.STR20) {
                        if (!field1.str.equals(field2.str)) {
                            return field1.str.compareTo(field2.str);
                        }
                    }
                }
                return 0;
            }

        });
    }

//    private void selectQueryForMultipleTables(ParserTree parserTree) {
//        ExpressionTree expressionTree = new ExpressionTree(parserTree.getWhereCondition(), "");
//        if (parserTree.isWhere()) {
//            optimizedSelection(expressionTree, parserTree);
//        }
//        ArrayList<String> tableLists = parserTree.getTables();
//        ArrayList<String> attributes = parserTree.getAttributes();
//        long startJoinDiskIO = disk.getDiskIOs();
//        Map<String, ArrayList<String>> tableAttrMap = new HashMap<>();
//        if (parserTree.isDistinct()) {
//            if (attributes.get(0).equals("*")) {
//                for (String table : tableLists) {
//                    tableAttrMap.put(table, schemaManager.getRelation(table).getSchema().getFieldNames());
//                }
//            } else {
//                for (String tableAttr : attributes) {
//                    String[] records = tableAttr.split("\\.");
//                    if (!tableAttrMap.containsKey(records[0])) {
//                        tableAttrMap.put(records[0], new ArrayList<>());
//                    }
//                    tableAttrMap.get(records[0]).add(records[1]);
//                }
//                // add rest of attrs
//                for (Map.Entry<String, ArrayList<String>> entry : tableAttrMap.entrySet()) {
//                    for (String attr : schemaManager.getRelation(entry.getKey()).getSchema().getFieldNames()) {
//                        if (!entry.getValue().contains(attr)) {
//                            entry.getValue().add(attr);
//                        }
//                    }
//                }
//            }
//            if (logger.isDebugEnabled()) {
//                for (Map.Entry<String, ArrayList<String>> entry : tableAttrMap.entrySet()) {
//                    logger.debug("distinct table name " + entry.getKey());
//                    logger.debug("distinct table attr " + entry.getValue());
//                }
//            }
//        }
//        String joinedTableName = "";
//        if (parserTree.isWhere()) {
//            ExpressionTree expressionTree2 = new ExpressionTree(parserTree.getWhereCondition(), "");
//            ArrayList<ExpressionTreeNode> multipleTableConditionList = ExpressionTree.getSubExpressionTreeNode(expressionTree2).first;
//            ArrayList<ExpressionTreeNode> filterCondition = new ArrayList<>();
//            ArrayList<ExpressionTreeNode> naturalJoinCondition = new ArrayList<>();
//            for (ExpressionTreeNode expressionTreeNode : multipleTableConditionList) {
//                if (isNaturalJoin(expressionTreeNode)) {
//                    naturalJoinCondition.add(expressionTreeNode);
//                } else {
//                    filterCondition.add(expressionTreeNode);
//                }
//            }
//            HashSet<String> naturalJoinedTable = new HashSet<>();
//            ArrayList<String> newTableList = parserTree.getTables();
//            for (int i=0; i<naturalJoinCondition.size(); i++) {
//                ExpressionTreeNode condition = naturalJoinCondition.get(i);
//                ArrayList<String> tableNames = tableRelevantToTheCondition(condition);
//                if(tableNames != null && tableNames.size()==2) {
//                    String tableOne = tableNames.get(0);
//                    String tableTwo = tableNames.get(1);
//                    if (naturalJoinedTable.contains(tableOne) && naturalJoinedTable.contains(tableTwo)) {
//                        filterCondition.add(condition);
//                        continue;
//                    }
//                    if(naturalJoinedTable.contains(tableOne)) {
//                        for(int j=0; j < newTableList.size(); j++) {
//                            String[] tables = newTableList.get(j).split("temp|naturalJoin");
//                            if(Arrays.asList(tables).contains(tableOne)) {
//                                tableOne = newTableList.get(j);
//                            }
//                        }
//                    } else {
//                        naturalJoinedTable.add(tableOne);
//                    }
//                    if(naturalJoinedTable.contains(tableTwo)) {
//                        for(int j=0; j< newTableList.size(); j++) {
//                            String[] tables = newTableList.get(j).split("temp|naturalJoin");
//                            if(Arrays.asList(tables).contains(tableTwo)) {
//                                tableTwo = newTableList.get(j);
//                            }
//                        }
//                    } else {
//                        naturalJoinedTable.add(tableTwo);
//                    }
//                    if (!parserTree.getTables().contains(tableOne) && parserTree.getTables().contains("temp"+tableOne)) {
//                        tableOne = "temp" + tableOne;
//                    }
//                    if (!parserTree.getTables().contains(tableTwo) && parserTree.getTables().contains("temp"+tableTwo)) {
//                        tableTwo = "temp" + tableTwo;
//                    }
//                    String tableName = naturalJoinTwoTables(tableOne, tableTwo, condition.getString(condition));
//                    newTableList.remove(tableOne);
//                    newTableList.remove(tableTwo);
//                    newTableList.add(tableName);
//                }
//            }
//            parserTree.setTables(newTableList);
//            joinedTableName = crossJoinTables(tableLists, parserTree.isDistinct(), tableAttrMap);
//            if(filterCondition == null || filterCondition.size()==0) {
//                parserTree.setWhere(false);
//                parserTree.setWhereCondition(null);
//            } else {
//                ExpressionTreeNode remainConditions = mergeSubConditionsToSameTable(filterCondition, joinedTableName);
//                parserTree.setWhereCondition(remainConditions.getString(remainConditions));
//            }
//        } else {
//            joinedTableName = crossJoinTables(tableLists, parserTree.isDistinct(), tableAttrMap);
//        }
//        parserTree.setTable(joinedTableName);
//        parserTree.setTables(null);
//        // because we have pushed distinct down, don't need to set distinct here
//        if (parserTree.isDistinct()) {
//            parserTree.setDistinct(false);
//            for (String table : tableLists) {
//                if (schemaManager.getRelation(table).getNumOfBlocks() > mainMemory.getMemorySize()) {
//                    parserTree.setDistinct(true);
//                    break;
//                }
//            }
//        }
//        selectQueryForSingleTable(parserTree);
//        schemaManager.deleteRelation(joinedTableName);
//    }

}
