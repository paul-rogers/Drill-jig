package org.apache.drill.jig.drillpress.net;

import org.apache.drill.jig.drillpress.DrillPressContext;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Future;

public class DrillPressServer
{
  public interface ReadyListener {
    void ready( );
  }
  
  public static class MiddlemanChannelInitializer extends ChannelInitializer<SocketChannel>
  {
    private RequestProcessorFactory procFactory;

    public MiddlemanChannelInitializer(RequestProcessorFactory procFactory) {
      this.procFactory = procFactory;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      ChannelPipeline p = ch.pipeline();
      p.addLast( new JigFrameDecoder( ) );
      p.addLast( new JigServerHandler( procFactory.newProcessor() ) );
    }    
  }
  
  private ReadyListener readyListener;
  private NioEventLoopGroup bossGroup;
  private NioEventLoopGroup workerGroup;
  private DrillPressContext context;
  
  public DrillPressServer( DrillPressContext context ) {
    this.context = context;
  }
  
  public void setReadyListener( ReadyListener listener ) {
    readyListener = listener;
  }

  public void start( ) throws InterruptedException {
    context.init();
    runNetty( );
    context.shutDown();
  }
  
  private void runNetty( ) throws InterruptedException {
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup();
      ServerBootstrap b = new ServerBootstrap()
       .group(bossGroup, workerGroup)
       .channel(NioServerSocketChannel.class)
       .childHandler(new MiddlemanChannelInitializer( context.getProcessor() ) )
       .option(ChannelOption.SO_BACKLOG, 128)
       .childOption(ChannelOption.SO_KEEPALIVE, true);

    try {
      // Bind and start to accept incoming connections.
      ChannelFuture f = b.bind(context.getDrillPressPort()).sync();
      
      if ( readyListener != null )
        readyListener.ready();
  
      // Wait until the server socket is closed.
      f.channel().closeFuture().sync();
    } finally {
      workerGroup.shutdownGracefully();
      bossGroup.shutdownGracefully();
    }
  }
  
  /**
   * Shuts down the server, blocking until shutdown is complete.
   */
  
  public void stop( ) {
    
    // Not sure if this is the right order...
    
    Future<?> bossFuture = bossGroup.shutdownGracefully();
    Future<?> workerFuture = workerGroup.shutdownGracefully();
    bossFuture.syncUninterruptibly();
    workerFuture.syncUninterruptibly();
  }
}
