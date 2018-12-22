package com.dgusev.hlcup2018.accountsapp.netty;

import com.dgusev.hlcup2018.accountsapp.model.BadRequest;
import com.dgusev.hlcup2018.accountsapp.model.NotFoundRequest;
import com.dgusev.hlcup2018.accountsapp.rest.AccountsController;
import com.dgusev.hlcup2018.accountsapp.service.AccountService;
import io.netty.bootstrap.ServerBootstrap;
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
import java.util.Date;

@Component
public class NettyServer {

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

        }

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
            if (msg instanceof FullHttpRequest)
            {
                final FullHttpRequest request = (FullHttpRequest) msg;
                try {


                QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());


                    if (request.method() == HttpMethod.GET) {

                        if (queryStringDecoder.uri().startsWith("/accounts/filter")) {
                            String result = accountsController.accountsFilter(queryStringDecoder.parameters());
                            writeResponse(channelHandlerContext, request, HttpResponseStatus.OK, result);
                        } else if (queryStringDecoder.uri().startsWith("/accounts/group")) {
                            String result = accountsController.group(queryStringDecoder.parameters());
                            writeResponse(channelHandlerContext, request, HttpResponseStatus.OK, result);
                        } else if (queryStringDecoder.uri().contains("recommend")) {
                            int fin = queryStringDecoder.uri().indexOf('/', 10);
                            int id = Integer.parseInt(queryStringDecoder.uri().substring(10, fin));
                            String result = accountsController.recommend(queryStringDecoder.parameters(), id);
                            writeResponse(channelHandlerContext, request, HttpResponseStatus.OK, result);
                        } else if (queryStringDecoder.uri().contains("suggest")) {
                            int fin = queryStringDecoder.uri().indexOf('/', 10);
                            int id = Integer.parseInt(queryStringDecoder.uri().substring(10, fin));
                            String result = accountsController.suggest(queryStringDecoder.parameters(), id);
                            writeResponse(channelHandlerContext, request, HttpResponseStatus.OK, result);
                        } else {
                            throw new BadRequest();
                        }
                    } else {
                        if (queryStringDecoder.rawPath().equals("/accounts/new/")) {
                            accountsController.create(request.content().toString(StandardCharsets.UTF_8));
                            writeResponse(channelHandlerContext, request, HttpResponseStatus.CREATED, "{}");
                        } else if (queryStringDecoder.rawPath().equals("/accounts/likes/")) {
                            accountsController.like(request.content().toString(StandardCharsets.UTF_8));
                            writeResponse(channelHandlerContext, request, HttpResponseStatus.ACCEPTED, "{}");
                        } else {
                            int fin = queryStringDecoder.uri().indexOf('/', 10);
                            int id = 0;
                            try {
                                id = Integer.parseInt(queryStringDecoder.uri().substring(10, fin));
                            } catch (Exception ex) {
                                throw new NotFoundRequest();
                            }
                            accountsController.update(request.content().toString(StandardCharsets.UTF_8), id);
                            writeResponse(channelHandlerContext, request, HttpResponseStatus.ACCEPTED, "{}");
                        }
                    }
                } catch (BadRequest | NumberFormatException badRequest) {
                    writeResponse(channelHandlerContext, request, HttpResponseStatus.BAD_REQUEST, null);
                } catch (NotFoundRequest notFoundRequest) {
                    writeResponse(channelHandlerContext, request, HttpResponseStatus.NOT_FOUND, null);
                } catch (Exception ex) {
                    //System.out.println(request.content().toString(StandardCharsets.UTF_8));
                    //ex.printStackTrace();
                    writeResponse(channelHandlerContext, request, HttpResponseStatus.BAD_REQUEST, null);
                }
            } else {
                super.channelRead(channelHandlerContext, msg);
            }
        }
    }

    private void writeResponse(ChannelHandlerContext channelHandlerContext, FullHttpRequest fullHttpRequest, HttpResponseStatus httpResponseStatus, String body) {
        FullHttpResponse response = null;
        int bodyLength = 0;
        if (body != null) {
            byte[] b = body.getBytes();
            response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    httpResponseStatus,
                    Unpooled.copiedBuffer(b)
            );
            bodyLength = b.length;
            response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json; charset=utf-8");
        } else {
            response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    httpResponseStatus,
                    Unpooled.copiedBuffer("{}".getBytes())
            );
            bodyLength = 2;
        }
        if (HttpUtil.isKeepAlive(fullHttpRequest)) {
            response.headers().set(
                    HttpHeaders.Names.CONNECTION,
                    HttpHeaders.Values.KEEP_ALIVE
            );
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bodyLength);
            channelHandlerContext.writeAndFlush(response);
        } else {
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bodyLength);
            channelHandlerContext.writeAndFlush(response);//.addListener(ChannelFutureListener.CLOSE);
        }
    }

}
