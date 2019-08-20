
package com.github.brdr3.mapreduce.main;

import com.github.brdr3.mapreduce.client.Client;
import com.github.brdr3.mapreduce.server.CoordinatorServer;
import com.github.brdr3.mapreduce.server.ReducerServer;

import java.util.logging.Logger;

public class Main {
    static Logger logger = Logger.getLogger("log4j.properties");
    public static void main(String[] args) {

        //if (System.getProperty("coordinator") != null) {
        if ( args[0].equals("coordinator") ) {
            //boolean b = Boolean.parseBoolean(System.getProperty("coordinator"));
            //if(b) {
                int mappers = Integer.parseInt(args[1]);
                CoordinatorServer cs = new CoordinatorServer(mappers);
                cs.start();
            //}
        } //else if(System.getProperty("reducer") != null) {
        else if(args[0].equals("reducer") ) {
            //boolean b = Boolean.parseBoolean(System.getProperty("reducer"));
            //if(b) {
                ReducerServer rs = new ReducerServer();
                rs.start();
            //}
        }// else if(System.getProperty("client") != null) {
        else if(args[0].equals("client")) {
            //boolean b = Boolean.parseBoolean(System.getProperty("client"));
            //if(b) {
                Client c = new Client();
                c.start();
            //}
        }
        else 
            throw new RuntimeException("Profile not specified");
    }
}
