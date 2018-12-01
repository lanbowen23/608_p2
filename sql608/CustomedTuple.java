package sql608;

import storageManager.Field;
import storageManager.Tuple;

import java.util.ArrayList;

// transform string field names list to
// real corresponding tuple values list
public class CustomedTuple {
    private ArrayList<Field> fields;
    private ArrayList<String> selectedFieldNames;

    public CustomedTuple(Tuple tuple, ArrayList<String> selectedFieldNamesList) {
        fields = new ArrayList<>();
        selectedFieldNames = selectedFieldNamesList;
        for (String fieldName : selectedFieldNamesList) {
            Field field = tuple.getField(fieldName);
            fields.add(field);
        }
    }
}
