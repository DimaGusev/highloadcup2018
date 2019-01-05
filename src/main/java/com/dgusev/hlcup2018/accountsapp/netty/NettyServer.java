package com.dgusev.hlcup2018.accountsapp.netty;

import com.dgusev.hlcup2018.accountsapp.model.BadRequest;
import com.dgusev.hlcup2018.accountsapp.model.NotFoundRequest;
import com.dgusev.hlcup2018.accountsapp.parse.QueryParser;
import com.dgusev.hlcup2018.accountsapp.rest.AccountsController;
import com.dgusev.hlcup2018.accountsapp.service.AccountService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;

@Component
public class NettyServer {

    private static final byte[] EMPTY_OBJECT = "{}".getBytes();

    private static final ThreadLocal<byte[]> decodeBuffer = new ThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[5000];
        }
    };

    @Value("${server.port}")
    private Integer port;

    @Autowired
    private AccountsController accountsController;

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
                 EventLoopGroup workerGroup = new NioEventLoopGroup(4);
                  try {
                        ServerBootstrap b = new ServerBootstrap();
                       b.group(bossGroup, workerGroup)
                       .channel(NioServerSocketChannel.class)
                               .childHandler(new HttpSnoopServerInitializer());

                      Channel ch = b.bind(port).sync().channel();
                        ch.closeFuture().sync();
                 } finally {
                      bossGroup.shutdownGracefully();
                       workerGroup.shutdownGracefully();
                   }
    }

    private class HttpSnoopServerInitializer extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel socketChannel) throws Exception {
            ChannelPipeline p = socketChannel.pipeline();
                     p.addLast(new HttpServerCodec());
                     p.addLast(new HttpObjectAggregator(512*1024));
                    p.addLast(new AccountHttpHandler());
        }
    }

    private class AccountHttpHandler extends SimpleChannelInboundHandler<Object> {


        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            //System.out.println(cause.getClass() +" "+ cause.getMessage());
        }

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
            if (msg instanceof FullHttpRequest)
            {
                final FullHttpRequest request = (FullHttpRequest) msg;
                ByteBuf responseBuf =  channelHandlerContext.alloc().directBuffer();
                try {

                QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());


                    if (request.method() == HttpMethod.GET) {

                        if (queryStringDecoder.rawPath().equals("/accounts/filter/")) {
                            int start = findPathEndIndex(queryStringDecoder.uri());
                            accountsController.accountsFilter(QueryParser.parse(queryStringDecoder.uri(), start), responseBuf);
                            writeResponse(channelHandlerContext, request, HttpResponseStatus.OK, responseBuf);
                        } else if (queryStringDecoder.rawPath().equals("/accounts/group/")) {
                            int start = findPathEndIndex(queryStringDecoder.uri());
                            accountsController.group(QueryParser.parse(queryStringDecoder.uri(), start), responseBuf);
                            writeResponse(channelHandlerContext, request, HttpResponseStatus.OK, responseBuf);
                        } else if (queryStringDecoder.uri().contains("recommend")) {
                            int start = findPathEndIndex(queryStringDecoder.uri());
                            int fin = queryStringDecoder.uri().indexOf('/', 10);
                            int id = 0;
                            try {
                                id = Integer.parseInt(queryStringDecoder.uri().substring(10, fin));
                            } catch (NumberFormatException ex) {
                                throw new NotFoundRequest();
                            }
                            accountsController.recommend(QueryParser.parse(queryStringDecoder.uri(), start), id, responseBuf);
                            writeResponse(channelHandlerContext, request, HttpResponseStatus.OK, responseBuf);
                        } else if (queryStringDecoder.uri().contains("suggest")) {
                            int start = findPathEndIndex(queryStringDecoder.uri());
                            int fin = queryStringDecoder.uri().indexOf('/', 10);
                            int id = 0;
                            try {
                                id = Integer.parseInt(queryStringDecoder.uri().substring(10, fin));
                            } catch (NumberFormatException ex) {
                                throw new NotFoundRequest();
                            }
                            accountsController.suggest(QueryParser.parse(queryStringDecoder.uri(), start), id, responseBuf);
                            writeResponse(channelHandlerContext, request, HttpResponseStatus.OK, responseBuf);
                        } else {
                            throw new NotFoundRequest();
                        }
                    } else {
                        responseBuf.writeBytes(EMPTY_OBJECT);
                        if (queryStringDecoder.rawPath().equals("/accounts/new/")) {
                            byte[] buf = decodeBuffer.get();
                            int count = request.content().readableBytes();
                            request.content().readBytes(buf, 0, count);
                            accountsController.create(buf, count);
                            writeResponse(channelHandlerContext, request, HttpResponseStatus.CREATED, responseBuf);
                        } else if (queryStringDecoder.rawPath().equals("/accounts/likes/")) {
                            byte[] buf = decodeBuffer.get();
                            int count = request.content().readableBytes();
                            request.content().readBytes(buf, 0, count);
                            accountsController.like(buf, count);
                            writeResponse(channelHandlerContext, request, HttpResponseStatus.ACCEPTED, responseBuf);
                        } else {
                            int fin = queryStringDecoder.uri().indexOf('/', 10);
                            int id = 0;
                            try {
                                id = Integer.parseInt(queryStringDecoder.uri().substring(10, fin));
                            } catch (Exception ex) {
                                throw new NotFoundRequest();
                            }
                            byte[] buf = decodeBuffer.get();
                            int count = request.content().readableBytes();
                            request.content().readBytes(buf, 0, count);
                            accountsController.update(buf, count, id);
                            writeResponse(channelHandlerContext, request, HttpResponseStatus.ACCEPTED, responseBuf);
                        }
                    }
                } catch (BadRequest | NumberFormatException badRequest) {
                    writeResponse(channelHandlerContext, request, HttpResponseStatus.BAD_REQUEST, responseBuf);
                } catch (NotFoundRequest notFoundRequest) {
                    writeResponse(channelHandlerContext, request, HttpResponseStatus.NOT_FOUND, responseBuf);
                } catch (Exception ex) {
                    //System.out.println(request.content().toString(StandardCharsets.UTF_8));
                    //ex.printStackTrace();
                    writeResponse(channelHandlerContext, request, HttpResponseStatus.BAD_REQUEST, responseBuf);
                }
            } else {
                super.channelRead(channelHandlerContext, msg);
            }
        }
    }

    private void writeResponse(ChannelHandlerContext channelHandlerContext, FullHttpRequest fullHttpRequest, HttpResponseStatus httpResponseStatus, ByteBuf responseBuf) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    httpResponseStatus,
                    responseBuf
        );
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, responseBuf.writerIndex());
        response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json; charset=utf-8");
        if (HttpUtil.isKeepAlive(fullHttpRequest)) {
            response.headers().set(
                    HttpHeaders.Names.CONNECTION,
                    HttpHeaders.Values.KEEP_ALIVE
            );
        }
        channelHandlerContext.writeAndFlush(response);
    }

    private static int findPathEndIndex(String uri) {
        int len = uri.length();
        for (int i = 0; i < len; i++) {
            char c = uri.charAt(i);
            if (c == '?' || c == '#') {
                return i + 1;
            }
        }
        return len;
    }

}
