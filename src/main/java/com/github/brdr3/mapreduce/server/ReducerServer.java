package com.github.brdr3.mapreduce.server;

import com.github.brdr3.mapreduce.util.Message;
import com.github.brdr3.mapreduce.util.Message.MessageBuilder;
import com.github.brdr3.mapreduce.util.User;
import com.github.brdr3.mapreduce.util.constants.Constants;
import com.google.gson.Gson;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ReducerServer {
    private final User reducerServer = Constants.reducerServer;
    private final Thread sender;
    private final Thread receiver;
    private final Thread processor;
    private final ConcurrentLinkedQueue<Message> senderQueue;
    private final ConcurrentLinkedQueue<Message> processQueue;
    private final ConcurrentHashMap<User, LinkedList<Message>> history;
    
    
    public ReducerServer () {
        sender = new Thread() {
            @Override
            public void run() {
                send();
            }
        };
        
        receiver = new Thread() {
            @Override
            public void run() {
                receive();
            }
        };
        
        processor = new Thread() {
            @Override
            public void run() {
                process();
            }
        };
        
        senderQueue = new ConcurrentLinkedQueue<>();
        processQueue = new ConcurrentLinkedQueue<>();
        history = new ConcurrentHashMap<>();
    }
    
    public void send() {
        while (true) {
            sleep();
            Message urls = senderQueue.poll();
            if(urls != null) {
                sendMessage(urls);
            }
        }
    }
    
    public void start() {
        sender.start();
        processor.start();
        receiver.start();
    }
    
    public void sendMessage(Message m) {
        Gson gson = new Gson();
        String jsonMessage = gson.toJson(m);
        byte buffer[] = new byte[10000];
        DatagramSocket socket;
        DatagramPacket packet;
        buffer = jsonMessage.getBytes();
        packet = new DatagramPacket(buffer, buffer.length, m.getTo().getAddress(),
                                    m.getTo().getPort());
        try {
            socket = new DatagramSocket();
            socket.send(packet);
            socket.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    public void receive() {
        DatagramSocket socket;
        DatagramPacket packet;
        String jsonMessage;
        Message message;
        byte buffer[] = new byte[10000];
        Gson gson = new Gson();

        try {
            socket = new DatagramSocket(reducerServer.getPort());
            while (true) {
                sleep();
                packet = new DatagramPacket(buffer, buffer.length, reducerServer.getAddress(), reducerServer.getPort());

                socket.receive(packet);

                jsonMessage = new String(packet.getData()).trim();
                message = gson.fromJson(jsonMessage, Message.class);

                processQueue.add(message);
                cleanBuffer(buffer);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    public void process() {
        while (true) {
            sleep();
            Message m = processQueue.poll();
            if(m != null) {
                processMessage(m);
            }
        }
    }
    
    public void processMessage(Message m) {
        if(history.containsKey(m.getRequestor())) {
            LinkedList<Message> auxiliar = history.get(m.getRequestor());
            auxiliar.add(m);
            history.put(m.getRequestor(), auxiliar);
        } else {
            LinkedList<Message> auxiliar = new LinkedList<>();
            auxiliar.add(m);
            history.put(m.getRequestor(), auxiliar);
        }
        
        LinkedList<Message> mapperList = history.get(m.getRequestor());
        
        if(m.getEnd().equals(new Long(mapperList.size()))) {
            reduce(m.getRequestor());
            history.remove(m.getRequestor());
        }
    }

    public void reduce(User requestor) {
        HashMap<String, Set<String>> invertedLinks = new HashMap<>();
        LinkedList<Message> messages = history.get(requestor);
        
        for(Message m: messages) {
            HashMap<String, LinkedList<String>> messageMap = 
                    (HashMap<String, LinkedList<String>>) m.getContent();
            
            for(Entry<String, LinkedList<String>> e: messageMap.entrySet()) {
                for(String linkPointed: e.getValue()) {
                    if(invertedLinks.containsKey(linkPointed)) {
                        Set<String> auxiliar = invertedLinks.get(linkPointed);
                        auxiliar.add(e.getKey());
                        invertedLinks.put(linkPointed, auxiliar);
                    } else {
                        TreeSet<String> auxiliar = new TreeSet<>();
                        auxiliar.add(e.getKey());
                        invertedLinks.put(linkPointed, auxiliar);
                    }
                }
            }
        }
        
        Message messageToClient = new MessageBuilder()
                            .content(invertedLinks)
                            .to(requestor)
                            .from(reducerServer)
                            .build();
        
        senderQueue.add(messageToClient);
    }
    
    private void cleanBuffer(byte[] buffer) {
        for(int i = 0; i < buffer.length; i++) {
            buffer[i] = 0;
        }
    }

    private void sleep(){
        try {
            Thread.sleep(1);
        } catch ( InterruptedException e ){
            System.out.println(e);
        }
    }
}
