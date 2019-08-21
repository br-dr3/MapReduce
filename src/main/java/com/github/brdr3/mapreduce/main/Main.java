
package com.github.brdr3.mapreduce.main;

import com.github.brdr3.mapreduce.client.Client;
import com.github.brdr3.mapreduce.mapper.Mapper;
import com.github.brdr3.mapreduce.server.CoordinatorServer;
import com.github.brdr3.mapreduce.server.ReducerServer;

import java.util.logging.Logger;

public class Main {
    static Logger logger = Logger.getLogger("log4j.properties");
    public static void main(String[] args) {
        if (System.getProperty("coordinator") != null) {
            boolean b = Boolean.parseBoolean(System.getProperty("coordinator"));
            if(b) {
                CoordinatorServer cs = new CoordinatorServer();
                cs.start();
            }
        } else if(System.getProperty("reducer") != null) {
            boolean b = Boolean.parseBoolean(System.getProperty("reducer"));
            if(b) {
                ReducerServer rs = new ReducerServer();
                rs.start();
            }
        } else if(System.getProperty("client") != null) {
            boolean b = Boolean.parseBoolean(System.getProperty("client"));
            if(b) {
                Client c = new Client();
                c.start();
            }
        } else if (System.getProperty("mapper") != null) {
            boolean b = Boolean.parseBoolean(System.getProperty("mapper"));
            if(b) {
                int port = Integer.parseInt(System.getProperty("port"));
                Mapper m = new Mapper(port);
                m.start();
            }
        }
        else 
            throw new RuntimeException("Profile not specified");
    }
}
