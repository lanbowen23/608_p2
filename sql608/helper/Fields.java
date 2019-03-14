package sql608.helper;

import storageManager.Field;
import storageManager.Tuple;

import java.util.ArrayList;

/*
used to Compare two tuples same on selected fields or not
transform the tuple's selected field names list to the corresponding Fields
*/
public class Fields {
    private ArrayList<Field> fields;
    private ArrayList<String> selectedFieldNames;

    public Fields(ArrayList<String> selectedFieldNamesList, Tuple tuple) {
        fields = new ArrayList<>();
        selectedFieldNames = selectedFieldNamesList;
        // store Field corresponding to field name
        for (String fieldName : selectedFieldNamesList) {
            Field field = tuple.getField(fieldName);
            fields.add(field);
        }
    }
}
