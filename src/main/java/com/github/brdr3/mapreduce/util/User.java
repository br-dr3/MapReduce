package com.github.brdr3.mapreduce.util;

import java.net.InetAddress;
import java.util.Objects;
import java.util.logging.Logger;


public class User {
    int id;
    private InetAddress address;
    private int port;
    static Logger logger = Logger.getLogger("log4j.properties");

    public User(int id, String address, int port) {
        this.id = id;
        
        try {
            this.address = InetAddress.getByName(address);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        this.port = port;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
    
    public InetAddress getAddress() {
        return address;
    }

    public void setAddress(InetAddress address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
    
    @Override
    public int hashCode() {
        int hash = 5;
        hash = 71 * hash + Objects.hashCode(this.id);
        hash = 71 * hash + Objects.hashCode(this.address);
        hash = 71 * hash + Objects.hashCode(this.port);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final User other = (User) obj;
        if (this.id != other.id) {
            return false;
        }
        if (this.port != other.port) {
            return false;
        }
        return Objects.equals(this.address, other.address);
    }
    
    @Override
    public String toString() {
        return "@"+this.address.getHostName()+":"+this.port;
    }

}