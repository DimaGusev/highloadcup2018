package io.netty.channel.epoll;

import com.dgusev.hlcup2018.accountsapp.model.BadRequest;
import com.dgusev.hlcup2018.accountsapp.model.NotFoundRequest;
import com.dgusev.hlcup2018.accountsapp.parse.QueryParser;
import com.dgusev.hlcup2018.accountsapp.pool.ObjectPool;
import com.dgusev.hlcup2018.accountsapp.rest.AccountsController;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.epoll.Native;
import io.netty.util.ReferenceCountUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Map;

@Component
public class RequestHandler {

    public volatile static ByteBuffer[] attachments = new ByteBuffer[500000];

    public static final TIntObjectMap<TIntArrayList> history = new TIntObjectHashMap<>();

    private static final byte[] RESPONSE_201 = "HTTP/1.0 201 Created\r\nConnection: keep-alive\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}".getBytes();
    private static final byte[] RESPONSE_202 = "HTTP/1.0 202 Accepted\r\nConnection: keep-alive\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}".getBytes();
    private static final byte[] BAD_REQUEST = "HTTP/1.0 400 Bad Request\r\nConnection: keep-alive\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}".getBytes();
    private static final byte[] NOT_FOUND = "HTTP/1.0 404 Not Found\r\nConnection: keep-alive\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}".getBytes();
    private static final byte[] OK_START = "HTTP/1.0 200 OK\r\nConnection: keep-alive\r\nContent-Type: application/json\r\nContent-Length: ".getBytes();
    private static final byte[] HEADERS_TERMINATOR = "\r\n\r\n".getBytes();

    private static final byte[] FILTER = "/accounts/filter/".getBytes();
    private static final byte[] GROUP = "/accounts/group/".getBytes();
    private static final byte[] NEW = "/accounts/new/".getBytes();
    private static final byte[] LIKES = "/accounts/likes/".getBytes();

    private ThreadLocal<byte[]> responseArray = new ThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[10000];
        }
    };

    @Autowired
    private AccountsController accountsController;

    public void handleRead(SelectionKey selectionKey, LinuxSocket fd, byte[] buf, int length, ByteBuffer byteBuffer) throws IOException {
        RequestHandler.history.get(fd.intValue()).add(-90);
        SocketChannel socketChannel = null;
        if (selectionKey != null) {
            socketChannel = (SocketChannel) selectionKey.channel();
        }
        try {
            ByteBuffer fragmentByteBuf = null;
            if (selectionKey !=null) {
                fragmentByteBuf = (ByteBuffer) selectionKey.attachment();
            } else {
                fragmentByteBuf = attachments[fd.intValue()];
            }
            int position = 0;
            int nl = 0;
            int len = length;
            if (fragmentByteBuf != null) {
                RequestHandler.history.get(fd.intValue()).add(-80);
                position = fragmentByteBuf.position();
                fragmentByteBuf.put(buf, 0, length);
                int newLength = fragmentByteBuf.position();
                nl = newLength;
                fragmentByteBuf.flip();
                fragmentByteBuf.get(buf, 0, newLength);
                length = newLength;
                ObjectPool.releaseBuffer(fragmentByteBuf);
                if (selectionKey != null) {
                    selectionKey.attach(null);
                } else {
                    attachments[fd.intValue()] = null;
                    attachments = attachments;
                }
            }
            if (buf[0] != 'G' && buf[0] != 'P') {
                System.out.println("1Buf[0]=" + buf[0] + ",length=" + length + ",fragmented=" + (fragmentByteBuf != null) + ",position=" + position + ",newLength=" + nl + ",len=" + len + ",fd=" + fd);
            }
            if ((buf[0] == 'G' && (buf[length - 1] != '\n' || buf[length - 2] != '\r' || buf[length - 3] != '\n' || buf[length - 4] != '\r')) || (buf[0] == 'P' && isFragmentedPost(buf, length, fd))) {
                if (buf[0] == 'G') {
                    RequestHandler.history.get(fd.intValue()).add(-70);
                } else {
                    RequestHandler.history.get(fd.intValue()).add(-71);
                }
                ByteBuffer fragment = ObjectPool.acquireBuffer();
                fragment.put(buf, 0, length);
                if (selectionKey != null) {
                    selectionKey.attach(fragment);
                } else {
                    attachments[fd.intValue()] = fragment;
                    RequestHandler.history.get(fd.intValue()).add(-50);
                    attachments = attachments;
                }
                return;
            }
            if (buf[0] != 'G' && buf[0] != 'P') {
                System.out.println("2Buf[0]=" + buf[0] + ",length=" + length);
            }
            RequestHandler.history.get(fd.intValue()).add(-60);
            if (buf[0] == 'G') {
                int queryStart = indexOf(buf, 0, length, ' ') + 1;
                int queryFinish = indexOf(buf, queryStart, length, ' ');
                int endPathIndex = findPathEndIndex(buf, queryStart, queryFinish);
                int startParameters = endPathIndex + 1;
                //long t1 = System.nanoTime();
                if (equals(buf, queryStart, endPathIndex, FILTER)) {
                    Map<String, String> params = QueryParser.parse(buf, startParameters, queryFinish);
                    byte[] responseArr = responseArray.get();
                    int bodyLength = accountsController.accountsFilter(params, responseArr);
                    byteBuffer.put(OK_START);
                    encodeInt(bodyLength, byteBuffer);
                    byteBuffer.put(HEADERS_TERMINATOR);
                    byteBuffer.limit(byteBuffer.position() + bodyLength);
                    byteBuffer.put(responseArr, 0, bodyLength);
                    writeResponse(socketChannel, fd,  byteBuffer);
                } else if (equals(buf, queryStart, endPathIndex, GROUP)) {
                    Map<String, String> params = QueryParser.parse(buf, startParameters, queryFinish);
                    byte[] responseArr = responseArray.get();
                    int bodyLength = accountsController.group(params, responseArr);
                    byteBuffer.put(OK_START);
                    encodeInt(bodyLength, byteBuffer);
                    byteBuffer.put(HEADERS_TERMINATOR);
                    byteBuffer.limit(byteBuffer.position() + bodyLength);
                    byteBuffer.put(responseArr, 0, bodyLength);
                    writeResponse(socketChannel, fd, byteBuffer);
                } else if (contains(buf, queryStart, endPathIndex, "recommend")) {
                    int fin = indexOf(buf, queryStart + 10, queryFinish, '/');
                    int id = decodeInt(buf, queryStart + 10, fin - queryStart - 10);
                    byte[] responseArr = responseArray.get();
                    int bodyLength = accountsController.recommend(QueryParser.parse(buf, startParameters, queryFinish), id, responseArr);
                    byteBuffer.put(OK_START);
                    encodeInt(bodyLength, byteBuffer);
                    byteBuffer.put(HEADERS_TERMINATOR);
                    byteBuffer.limit(byteBuffer.position() + bodyLength);
                    byteBuffer.put(responseArr, 0, bodyLength);
                    writeResponse(socketChannel, fd, byteBuffer);
                } else if (contains(buf, queryStart, endPathIndex, "suggest")) {
                    int fin = indexOf(buf, queryStart + 10, queryFinish, '/');
                    int id = decodeInt(buf, queryStart + 10, fin - queryStart - 10);
                    byte[] responseArr = responseArray.get();
                    int bodyLength = accountsController.suggest(QueryParser.parse(buf, startParameters, queryFinish), id, responseArr);
                    byteBuffer.put(OK_START);
                    encodeInt(bodyLength, byteBuffer);
                    byteBuffer.put(HEADERS_TERMINATOR);
                    byteBuffer.limit(byteBuffer.position() + bodyLength);
                    byteBuffer.put(responseArr, 0, bodyLength);
                    writeResponse(socketChannel, fd, byteBuffer);
                } else  {
                    throw NotFoundRequest.INSTANCE;
                }
                //long t2 = System.nanoTime();
                //if (t2-t1 > 5000000) {
                    //System.out.println("Time=" +(t2-t1)+", query=" + new String(buf, queryStart, queryFinish));
                //}

            } else if (buf[0] == 'P') {
                int queryStart = indexOf(buf, 0, length, ' ') + 1;
                int queryFinish = indexOf(buf, queryStart, length, ' ');
                int endPathIndex = findPathEndIndex(buf, queryStart, queryFinish);
                int pointer = endPathIndex;
                while (!(buf[pointer] == '\n' && buf[pointer - 1] == '\r' && buf[pointer - 2] == '\n' && buf[pointer - 3] == '\r')) {
                    pointer++;
                }
                pointer++;
                int endIndex = length - 1;
                while (buf[endIndex] == '\r' || buf[endIndex] == '\n') {
                    endIndex--;
                }
                endIndex++;
                if (equals(buf, queryStart, endPathIndex, NEW)) {
                    accountsController.create(buf, pointer, endIndex);
                    writeResponse(socketChannel, fd, byteBuffer, RESPONSE_201);
                } else if (equals(buf, queryStart, endPathIndex, LIKES)) {
                    accountsController.like(buf, pointer, endIndex);
                    writeResponse(socketChannel, fd, byteBuffer, RESPONSE_202);
                } else {
                    int fin = indexOf(buf, queryStart + 10, queryFinish, '/');
                    int id = 0;
                    try {
                         id = decodeInt(buf, queryStart + 10, fin - queryStart - 10);
                    } catch (BadRequest ex) {
                        throw NotFoundRequest.INSTANCE;
                    }
                    accountsController.update(buf, pointer, endIndex, id);
                    writeResponse(socketChannel, fd, byteBuffer, RESPONSE_202);
                }
            } else {
                System.out.println(fd.intValue() + " Bad first byte " + (byte)buf[0] + " with length=" + length +",history=" + RequestHandler.history.get(fd.intValue()) + " |" + new String(buf, 0, length));
                byteBuffer.put(BAD_REQUEST);
                byteBuffer.flip();
                if (socketChannel != null) {
                    socketChannel.write(byteBuffer);
                } else {
                    fd.write(byteBuffer, byteBuffer.position(), byteBuffer.limit());
                }
            }
        } catch (BadRequest badRequest) {
            writeResponse(socketChannel, fd, byteBuffer, BAD_REQUEST);
        } catch (NumberFormatException nfex) {
            if (buf[0] == 'P') {
                System.out.println("1" + new String(buf, 0, length));
            }
            writeResponse(socketChannel, fd, byteBuffer, BAD_REQUEST);
        } catch (NotFoundRequest notFoundRequest) {
            writeResponse(socketChannel, fd, byteBuffer, NOT_FOUND);
        } catch (Exception ex) {
            ex.printStackTrace();
            writeResponse(socketChannel, fd, byteBuffer, BAD_REQUEST);
        }

    }


    private void writeResponse(SocketChannel socketChannel, LinuxSocket fd, ByteBuffer byteBuffer, byte[] response) throws IOException {
        byteBuffer.clear();
        byteBuffer.put(response);
        byteBuffer.flip();
        if (socketChannel != null) {
            socketChannel.write(byteBuffer);
        } else {
            fd.write(byteBuffer, byteBuffer.position(), byteBuffer.limit());
        }
    }

    private void writeResponse(SocketChannel socketChannel, LinuxSocket fd, ByteBuffer byteBuffer) throws IOException {
        byteBuffer.flip();
        if (socketChannel != null) {
            socketChannel.write(byteBuffer);
        } else {
            fd.write(byteBuffer, byteBuffer.position(), byteBuffer.limit());
        }
    }

    private int indexOf(byte[] arr, int from, int to, char val) {
        for (int i = from; i < to; i++) {
            if (arr[i] == (byte) val) {
                return i;
            }
        }
        return -1;
    }

    private static int findPathEndIndex(byte[] arr, int from, int to) {
        for (int i = from; i < to; i++) {
            byte c = arr[i];
            if (c == '?' || c == '#') {
                return i;
            }
        }
        return to;
    }

    private boolean equals(byte[] arr, int from, int to, byte[] value) {
        if (to - from != value.length) {
            return false;
        }
        for (int i = from; i < to; i++) {
            if (arr[i] != value[i - from]) {
                return false;
            }
        }

        return true;
    }

    private  boolean contains(byte [] buf, int from, int to, String part) {
        int position = from + 10;
        char first = part.charAt(0);
        int partSize = part.length();
        while (position < to) {
            if (buf[position] == first) {
                if (position + partSize > to) {
                    return false;
                } else {
                    int partOffset = 0;
                    while ((partOffset < partSize) && buf[position] == part.charAt(partOffset)) {
                        position++;
                        partOffset++;
                    }
                    if (partOffset == partSize) {
                        return true;
                    }
                }
            } else {
                position++;
            }
        }
        return false;
    }

    private static final int POW10[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};


    public static void encodeInt(int value, ByteBuffer responseBuf) {
        boolean printZero = false;
        for (int i = 9; i>=0; i--) {
            int digit = (int)(value/POW10[i]);
            if (digit == 0 && !printZero) {
                continue;
            }
            responseBuf.put((byte)(48 + digit));
            printZero=true;
            value -= (value/POW10[i]) * POW10[i];
        }
    }

    private int decodeInt(byte[] buf, int from, int length) {
        if (length > 10) {
            throw NotFoundRequest.INSTANCE;
        }
        int result = 0;
        for (int i = from; i < from + length; i++) {
            int value = buf[i] - 48;
            if (value <0 || value > 9) {
                throw NotFoundRequest.INSTANCE;
            }
            result+=POW10[length - (i - from) - 1]* value;
        }
        return result;
    }

    private boolean isFragmentedPost(byte[] buf, int length, LinuxSocket fd) {
        int contentLength = 0;
        try {
            contentLength = readContentLength(buf, length);
        } catch (Exception ex) {
            System.out.println("Cannot read content length: " + new String(buf, 0, length));
            if (ex instanceof NotFoundRequest) {
                throw  ex;
            } else {
                throw new RuntimeException(ex);
            }
        }
        if (contentLength == -1) {
            return true;
        }
        if (contentLength == 0) {
            return false;
        }
        int pointer = 0;
        while (!(buf[pointer] == '\n' && buf[pointer - 1] == '\r' && buf[pointer - 2] == '\n' && buf[pointer - 3] == '\r')) {
            pointer++;
        }
        pointer++;
        if (length - pointer - 2 > contentLength ) {
            System.out.println(fd.intValue() + "Error, more data than needed, actual=" + (length - pointer) +",history=" + history.get(fd.intValue()) + " ,header=" + contentLength + ": " + new String(buf, 0, length));
        }
        if (length - pointer - 2  < contentLength ) {
            return true;
        }
        return false;
    }

    private int readContentLength(byte[] buf, int length) {
        String contentLengthHeader = "Content-Length: ";
        int position = 0;
        char first = contentLengthHeader.charAt(0);
        int partSize = contentLengthHeader.length();
        while (position < length) {
            if (buf[position] == first) {
                if (position + partSize > length) {
                    return -1;
                } else {
                    int partOffset = 0;
                    while ((partOffset < partSize) && buf[position] == contentLengthHeader.charAt(partOffset)) {
                        position++;
                        partOffset++;
                    }
                    if (partOffset == partSize) {
                        return decodeInt(buf, position, indexOf(buf, position, length, '\r') - position);
                    }
                }
            } else {
                position++;
            }
        }
        return -1;
    }


}
