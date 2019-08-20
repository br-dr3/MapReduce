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
import java.util.logging.Logger;

public class Client {
    private final User thisUser;
    private final Thread sender;
    private final Thread receiver;
    private final Thread interactor;
    private final Thread processor;
    
    private ConcurrentLinkedQueue<LinkedList<String>> senderQueue;
    private ConcurrentLinkedQueue<Message> processQueue;

    static Logger logger = Logger.getLogger("log4j.properties");

    public Client() {
        
        try {
            String addr = "localhost";
            int port = 14000;
            thisUser = new User(0, addr, port);
            logger.info("Cliente instanciada com sucesso addr: " + addr + " port: " + port);
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
        logger.info("CLiente start ");
        sender.start();
        receiver.start();
        interactor.start();
        processor.start();
    }
    
    public void send() {
        while (true) {
            LinkedList urls = senderQueue.poll();
            sleep();
            if(urls != null) {
                sendMessage(urls);
            }
        }
    }
    
    public void sendMessage(LinkedList<String> urls) {
        logger.info("CLiente enviando mensagem ");
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
            logger.info("CLiente enviando mensagem bufferSize:" + buffer.length + " to addrs: " + m.getTo().getAddress() + " to port: " + m.getTo().getPort());
            socket = new DatagramSocket();
            socket.send(packet);
            socket.close();
        } catch (Exception ex) {
            logger.warning("Cliente não conseguiu enviar mensagem " + ex );
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
                sleep();

                packet = new DatagramPacket(buffer, buffer.length, thisUser.getAddress(), thisUser.getPort());

                socket.receive(packet);
                logger.info("Cliente mensagem chegou e foi add na fila processQueue");
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
            sleep();
            System.out.println("Digite os urls separados por espaço");
            userEntry = x.nextLine();
            LinkedList<String> urls;
            if( userEntry.contains(" ") ){
                urls = (LinkedList<String>) Arrays.asList(userEntry.split(" "));
            }else{
                urls = new LinkedList<String>();
                urls.add(userEntry);
            }
            senderQueue.add(urls);
        }
    }
    
    public void process() {
        while(true) {
            sleep();
            Message m = processQueue.poll();
            if(m != null) {
                processMessage(m);
            }
        }
    }
    
    public void processMessage(Message m) {
        HashMap<String, Set<String>> pointedLinks = 
                (HashMap<String, Set<String>>) m.getContent();
        logger.info("Cliente processando mensgem ");
        System.out.println(pointedLinks);
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
