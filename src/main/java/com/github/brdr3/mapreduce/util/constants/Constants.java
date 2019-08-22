package com.github.brdr3.mapreduce.util.constants;

import com.github.brdr3.mapreduce.util.User;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Constants {

    public static User coordinatorServer = new User(-1, "192.168.43.79", 14010);
    public static User reducerServer = new User(-2, "192.168.43.195", 12000);
    
    public static String getRealInetAddress() {
        Enumeration<NetworkInterface> en = null;
        try {
            en = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException ex) {
            ex.printStackTrace();
        }
        
        while (en != null && en.hasMoreElements()) {
            NetworkInterface ni = en.nextElement();
            Enumeration<InetAddress> inetAddresses = ni.getInetAddresses();

            while (inetAddresses.hasMoreElements() ) {
                InetAddress ia = inetAddresses.nextElement();
                if (!ia.isLinkLocalAddress()) {
                    return ia.getHostAddress();
                }
            }
        }
        return null;
    }
}
