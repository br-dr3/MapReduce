package com.github.brdr3.mapreduce.server;

import com.github.brdr3.mapreduce.mapper.Mapper;
import com.github.brdr3.mapreduce.util.Message;
import com.github.brdr3.mapreduce.util.Message.MessageBuilder;
import com.github.brdr3.mapreduce.util.User;
import com.github.brdr3.mapreduce.util.constants.Constants;
import com.google.gson.Gson;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;

public class CoordinatorServer {
    
    private final User coordinatorServer = Constants.coordinatorServer;
    private final Thread sender;
    private final Thread receiver;
    private final Thread processor;
    
    private Mapper mapper[];
    
    private ConcurrentLinkedQueue<Message> senderQueue;
    private ConcurrentLinkedQueue<Message> processQueue;
    
    public CoordinatorServer(int mappers) {
        
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
        
        mapper = new Mapper[mappers];
        
        for(int i = 0; i < mappers; i++) {
            mapper[i] = new Mapper(new User(i+1, "localhost", 14020+10*i));
        }
        
        senderQueue = new ConcurrentLinkedQueue<>();
        processQueue = new ConcurrentLinkedQueue<>();
    }
    
    public void start() {
        sender.start();
        receiver.start();
        processor.start();
    }
    
    public void send() {
        while (true) {
            Message urls = senderQueue.poll();
            if(urls != null) {
                sendMessage(urls);
            }
        }
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
        byte buffer[] = new byte[65507];
        Gson gson = new Gson();

        try {
            socket = new DatagramSocket(coordinatorServer.getPort());
            while (true) {
                packet = new DatagramPacket(buffer, buffer.length, coordinatorServer.getAddress(), coordinatorServer.getPort());

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
            Message m = processQueue.poll();
            if(m != null) {
                processMessage(m);
            }
        }
    }
    
    public void processMessage(Message m) {
        LinkedList[] mapperLists;
        LinkedList<Message> messages = new LinkedList<>();
        
        if(m.getContent() instanceof LinkedList)
            mapperLists = divideList((LinkedList<String>) m.getContent());
        else
            return;
        
        for(int i = 0; i < mapperLists.length; i++) {
            if(mapperLists[i] != null) {
                Message messageToMapper = new MessageBuilder().from(coordinatorServer)
                                                       .id(new Long(i))
                                                       .to(mapper[i].getMapperUser())
                                                       .content(mapperLists[i])
                                                       .requestor(m.getRequestor())
                                                       .build();
                messages.add(messageToMapper);
            }
        }
        
        for(Message message: messages) {
            message.setEnd(new Long(messages.size()));
            senderQueue.add(message);
        }
    }
    
    private LinkedList[] divideList(LinkedList<String> urls) {
        LinkedList[] mapperList = new LinkedList [mapper.length];
        int size = mapperList.length;
        
        for(int i = 0; i < urls.size(); i++) {
            if(mapperList[i%size] == null) {
                mapperList[i%size] = new LinkedList<String>();
            }
            
            mapperList[i%size].add(urls.get(i));
        }
        
        return mapperList;
    }
    
    private void cleanBuffer(byte[] buffer) {
        for(int i = 0; i < buffer.length; i++) {
            buffer[i] = 0;
        }
    }
}
