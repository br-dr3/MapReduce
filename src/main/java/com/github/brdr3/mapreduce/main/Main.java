
package com.github.brdr3.mapreduce.main;

import com.github.brdr3.mapreduce.client.Client;
import com.github.brdr3.mapreduce.server.CoordinatorServer;
import com.github.brdr3.mapreduce.server.ReducerServer;

public class Main {
    public static void main(String[] args) {
        if (System.getProperty("coordinator") != null) {
            boolean b = Boolean.parseBoolean(System.getProperty("coordinator"));
            if(b) {
                int mappers = Integer.parseInt(System.getProperty("mappers"));
                CoordinatorServer cs = new CoordinatorServer(mappers);
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
        }
        else 
            throw new RuntimeException("Profile not specified");
    }
}
