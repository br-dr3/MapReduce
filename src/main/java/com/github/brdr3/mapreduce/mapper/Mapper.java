package com.github.brdr3.mapreduce.mapper;

import com.github.brdr3.mapreduce.util.Message;
import com.github.brdr3.mapreduce.util.Message.MessageBuilder;
import com.github.brdr3.mapreduce.util.User;
import com.github.brdr3.mapreduce.util.constants.Constants;
import com.google.gson.Gson;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

public class Mapper {
    private User mapperUser;
    private final Thread receiver;
    private final Thread sender;
    private final Thread processor;
    private final ConcurrentLinkedQueue<Message> processQueue;
    private final ConcurrentLinkedQueue<Message> senderQueue;
    
    public Mapper(User u) {
        mapperUser = u;
        
        receiver = new Thread() {
            @Override
            public void run() {
                receive();
            }
        };
        
        sender = new Thread() {
            @Override
            public void run() {
                send();
            }
        };
        
        processor = new Thread() {
            @Override
            public void run() {
                process();
            }
        };
        
        processQueue = new ConcurrentLinkedQueue<>();
        senderQueue = new ConcurrentLinkedQueue<>();
    }
    
    public void receive() {
        DatagramSocket socket;
        DatagramPacket packet;
        String jsonMessage;
        Message message;
        byte buffer[] = new byte[10000];
        Gson gson = new Gson();

        try {
            socket = new DatagramSocket(mapperUser.getPort());
            while (true) {
                packet = new DatagramPacket(buffer, buffer.length, 
                                            mapperUser.getAddress(), 
                                            mapperUser.getPort());

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
        byte buffer[] = new byte[65507];
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
    
    public User getMapperUser() {
        return mapperUser;
    }

    public void setMapperUser(User mapperUser) {
        this.mapperUser = mapperUser;
    }
    
    public void process() {
        while(true) {
            Message m = processQueue.poll();
            if(m != null) {
                processMessage(m);
            }
        }
    }
    
    public void processMessage(Message m) {
        LinkedList<String> listToProcess;
        if (m.getContent() instanceof LinkedList)
            listToProcess = (LinkedList<String>) m.getContent();
        else
            return;
        
        HashMap<String, LinkedList<String>> linksUrls = new HashMap<>();
        Document website;
        Elements links;
        
        for(String url: listToProcess) {
            try {
                website = Jsoup.connect(url).get();
                links = website.select("a[href]");
                
                LinkedList<String> linkList = 
                    links.stream()
                         .map(l -> l.attr("href"))
                         .collect(Collectors.toCollection(LinkedList :: new));
                
                linksUrls.put(url, linkList);
                
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        
        Message messageToReducer = new MessageBuilder()
                                        .id(m.getId())
                                        .end(m.getEnd())
                                        .to(Constants.reducerServer)
                                        .from(mapperUser)
                                        .content(linksUrls)
                                        .requestor(m.getRequestor())
                                        .build();
        senderQueue.add(messageToReducer);
    }
    
    private void cleanBuffer(byte[] buffer) {
        for(int i = 0; i < buffer.length; i++) {
            buffer[i] = 0;
        }
    }
}
