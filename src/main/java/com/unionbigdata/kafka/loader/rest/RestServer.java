package com.unionbigdata.kafka.loader.rest;


import com.unionbigdata.kafka.loader.core.LoaderContext;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by kali on 2015/9/6.
 */
public class RestServer {

   // private final LoaderContext context;
    private Server jettyServer = null;

   // public RestServer(LoaderContext context) {
//        this.context = context;
//    }

    public static class HelloTest{
        public String hello(){
            return "Hello Test.";
        }
    }

    public static void main(String[] args) throws Exception {
        URI baseURI = UriBuilder.fromUri("http://localhost/").port(8999).build();
        ResourceConfig rc = new ResourceConfig(LoaderOperation.class);
        Map<String,Object> pros = new HashMap<>();
        pros.put("loader.context", new Object());
        rc.addProperties(pros);
        //Server server = JettyHttpContainerFactory
    }

    private void init() {
//        ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.SESSIONS);
//        handler.setContextPath("/");
//
//        //int port = context.conf.getInt("loader.rest.port", 8099);
//        jettyServer = new Server(8080);
//        jettyServer.setHandler(handler);
//
//        ServletHolder jerseyServlet = handler.addServlet(
//                org.glassfish.jersey.servlet.ServletContainer.class,"/*");
//        jerseyServlet.setInitOrder(0);
//
//        // Tells the Jersey Servlet which REST service/class to load.
////        jerseyServlet.setInitParameter(
////                "com.sun.jersey.config.property.packages",
////                "com.unionbigdata.kafka.loader.rest");
//
//        jerseyServlet.setInitParameter("javax.ws.rs.Application",
//                com.unionbigdata.kafka.loader.rest.MyApplication.class.getCanonicalName());
    }

    public void start() throws Exception {
        init();
        jettyServer.start();
    }

    public void stop() {
        if (jettyServer != null) {
            try {
                jettyServer.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
