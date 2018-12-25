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
        for (int i = 0; i < group.values.size() && i < keys.size(); i++) {
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
        responseBuf.writeByte('}');
    }
}
