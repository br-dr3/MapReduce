package com.github.brdr3.mapreduce.client;

import com.github.brdr3.mapreduce.util.Message;
import com.github.brdr3.mapreduce.util.Message.MessageBuilder;
import com.github.brdr3.mapreduce.util.User;
import com.github.brdr3.mapreduce.util.constants.Constants;
import com.google.gson.Gson;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Client {
    private final User thisUser;
    private final Thread sender;
    private final Thread receiver;
    private final Thread interactor;
    private final Thread processor;
    
    private ConcurrentLinkedQueue<LinkedList<String>> senderQueue;
    private ConcurrentLinkedQueue<Message> processQueue;
    
    public Client() {
        
        try {
            thisUser = new User(0, "localhost", 14000);
        } catch (Exception ex) {
            throw new RuntimeException("It was not possible to create the User.");
        }
        
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
        
        interactor = new Thread() {
            @Override
            public void run() {
                interact();
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
    }
    
    public void start() {
        sender.start();
        receiver.start();
        interactor.start();
        processor.start();
    }
    
    public void send() {
        while (true) {
            LinkedList urls = senderQueue.poll();
            if(urls != null) {
                sendMessage(urls);
            }
        }
    }
    
    public void sendMessage(LinkedList<String> urls) {
        Gson gson = new Gson();
        Message m = new MessageBuilder().from(thisUser)
                                        .to(Constants.coordinatorServer)
                                        .content(urls)
                                        .requestor(thisUser)
                                        .build();
        
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
    
    public void receive() {
        DatagramSocket socket;
        DatagramPacket packet;
        String jsonMessage;
        Message message;
        byte buffer[] = new byte[65507];
        Gson gson = new Gson();

        try {
            socket = new DatagramSocket(thisUser.getPort());
            while (true) {
                packet = new DatagramPacket(buffer, buffer.length, thisUser.getAddress(), thisUser.getPort());

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
    
    public void interact() {
        String userEntry;
        Scanner x = new Scanner(System.in);
        while(true) {
            System.out.println("Digite os urls separados por espaço");
            userEntry = x.nextLine();
            LinkedList<String> urls = (LinkedList<String>) Arrays.asList(userEntry.split(" "));
            senderQueue.add(urls);
        }
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
        HashMap<String, Set<String>> pointedLinks = 
                (HashMap<String, Set<String>>) m.getContent();
        
        System.out.println(pointedLinks);
    }
    
    private void cleanBuffer(byte[] buffer) {
        for(int i = 0; i < buffer.length; i++) {
            buffer[i] = 0;
        }
    }
}
