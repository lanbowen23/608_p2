package sql608.Heap;

import storageManager.*;

import java.util.ArrayList;

public class HeapFunc {
    public static TupleHeap build(ArrayList<String> sortFields) {
        // By default ORDER BY sorts the data in ascending order
        // so I want a Heap which returns minimum
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

    public static TupleHeap2 build2(ArrayList<String> sortFields) {
        return new TupleHeap2((o1, o2) -> {
            for (String sortField : sortFields) {
                // basically, this sort by the nodeName sortField
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
    // push tuple from main mem into the Heap

    public static void offer(TupleHeap heap, MainMemory mainMemory) {
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
}
