package com.unionbigdata.kafka.loader.rest;



import com.unionbigdata.kafka.loader.common.LoaderContext;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;


/**
 * Created by kali on 2015/9/6.
 */
public class RestServer {

    private final LoaderContext context;
    private Server jettyServer;
    private Boolean isStarted = false;


    public RestServer(LoaderContext context){
        this.context = context;
    }

    public void start() throws Exception {
        if (!isStarted){
            int port = context.conf.getInt("loader.rest.port", 8099);
            jettyServer = new Server(port);

            ResourceConfig rc = new ResourceConfig()
                    .register(new ServiceBinder(context))
                    .register(LoaderOperation.class);

            ServletContextHandler root = new ServletContextHandler(jettyServer,"/*",ServletContextHandler.SESSIONS);
            root.addServlet(new ServletHolder(new ServletContainer(rc)),".*");
            jettyServer.start();
            isStarted = true;
        }
    }

    public void shutdown(){
        if (isStarted){
            jettyServer.destroy();
            isStarted = false;
        }
    }
}
