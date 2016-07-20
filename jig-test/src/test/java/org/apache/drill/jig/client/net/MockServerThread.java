package org.apache.drill.jig.client.net;

import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import org.apache.drill.jig.drillpress.net.JigFrameDecoder;
import org.apache.drill.jig.drillpress.net.JigServerHandler;
import org.apache.drill.jig.protocol.MessageConstants;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class MockServerThread extends Thread
{
  public CountDownLatch readyLatch = new CountDownLatch( 1 );
  private int port = MessageConstants.DEFAULT_PORT;
  private PrintWriter out;
  
  public MockServerThread( PrintWriter out ) {
    super( "Mock Server" );
    this.out = out;
  }
  
  @Override
  public void run( )
  {
    try {
      runServer( );
    } catch (InterruptedException e) {
      System.err.println( "Interrupted" );
      e.printStackTrace();
    }
  }
  
  private void runServer( ) throws InterruptedException
  {
    final JigFrameDecoder frameDec = new JigFrameDecoder( );
    final JigServerHandler serverHandler = new JigServerHandler(
        new MockRequestProcessor( out ) );
    EventLoopGroup group = new NioEventLoopGroup();
    try {
      ServerBootstrap b = new ServerBootstrap();
      b.group(group)
        .channel(NioServerSocketChannel.class)
        .localAddress(new InetSocketAddress(port ))
        .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch)
                throws Exception {
              ch.pipeline().addLast(frameDec);
              ch.pipeline().addLast(serverHandler);
            }
        });
      ChannelFuture f = b.bind().sync();
      readyLatch.countDown();
      f.channel().closeFuture().sync();
    } finally {
      group.shutdownGracefully().sync();
    }
  }
}