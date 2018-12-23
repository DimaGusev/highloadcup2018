package com.dgusev.hlcup2018.accountsapp.format;

import com.dgusev.hlcup2018.accountsapp.model.Group;
import io.netty.buffer.ByteBuf;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class GroupFormatter {

    private static final byte[] COUNT = "{\"count\":".getBytes();
    private static final byte[] NULL = "null".getBytes();

    public void format(Group group, List<String> keys, ByteBuf responseBuf) {
        responseBuf.writeBytes(COUNT);
        responseBuf.writeBytes(Integer.valueOf(group.count).toString().getBytes());
        boolean hasNotNull = hasNotNullGroup(group.values, keys.size());
        for (int i = 0; i < group.values.size() && i < keys.size(); i++) {
                if (!hasNotNull) {
                    responseBuf.writeByte(',');
                    responseBuf.writeByte('\"');
                    responseBuf.writeBytes(keys.get(i).getBytes());
                    responseBuf.writeByte('\"');
                    responseBuf.writeByte(':');
                    if (group.values.get(i) != null) {
                        responseBuf.writeByte('\"');
                        responseBuf.writeBytes(group.values.get(i).getBytes());
                        responseBuf.writeByte('\"');
                    } else {
                        responseBuf.writeBytes(NULL);
                    }
                } else {
                    if (group.values.get(i) != null) {
                        responseBuf.writeByte(',');
                        responseBuf.writeByte('\"');
                        responseBuf.writeBytes(keys.get(i).getBytes());
                        responseBuf.writeByte('\"');
                        responseBuf.writeByte(':');
                        responseBuf.writeByte('\"');
                        responseBuf.writeBytes(group.values.get(i).getBytes());
                        responseBuf.writeByte('\"');
                    }
                }
        }
        responseBuf.writeByte('}');
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
