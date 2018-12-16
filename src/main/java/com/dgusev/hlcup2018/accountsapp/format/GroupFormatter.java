package com.dgusev.hlcup2018.accountsapp.format;

import com.dgusev.hlcup2018.accountsapp.model.Group;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class GroupFormatter {

    public String format(Group group, List<String> keys) {
        StringBuilder stringBuilder = new StringBuilder("{\"count\":").append(group.count);
        boolean hasNotNull = hasNotNullGroup(group.values, keys.size());
        for (int i = 0; i < group.values.size() && i < keys.size(); i++) {

                if (!hasNotNull) {
                    stringBuilder.append(",");
                    stringBuilder.append("\"").append(keys.get(i)).append("\":");
                    if (group.values.get(i) != null) {
                        stringBuilder.append("\"").append(group.values.get(i)).append("\"");
                    } else {
                        stringBuilder.append("null");
                    }
                } else {
                    if (group.values.get(i) != null) {
                        stringBuilder.append(",\"").append(keys.get(i)).append("\":\"").append(group.values.get(i)).append("\"");;
                    }
                }
        }
        stringBuilder.append("}");
        return stringBuilder.toString();
    }

    private boolean hasNotNullGroup(List<String> strings, int size) {
        for (int i =0; i< size; i++) {
            if (strings.get(i) != null) {
                return true;
            }
        }
        return false;

    }
}
