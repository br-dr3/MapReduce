package com.github.brdr3.mapreduce.server;

import com.github.brdr3.mapreduce.util.Message;
import com.github.brdr3.mapreduce.util.Message.MessageBuilder;
import com.github.brdr3.mapreduce.util.User;
import com.github.brdr3.mapreduce.util.constants.Constants;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

public class ReducerServer {
    private final User reducerServer = Constants.reducerServer;
    private final Thread sender;
    private final Thread receiver;
    private final Thread processor;
    private final ConcurrentLinkedQueue<Message> senderQueue;
    private final ConcurrentLinkedQueue<Message> processQueue;
    private final ConcurrentHashMap<User, LinkedList<Message>> history;
    static Logger logger = Logger.getLogger("log4j.properties");

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
                logger.info("ReducerServer@" + reducerServer.getAddress() + ":" + reducerServer.getPort() +" -> enviando mensagem");
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
        DatagramSocket socket;
        DatagramPacket packet;
        byte [] buffer = jsonMessage.getBytes();
        packet = new DatagramPacket(buffer, buffer.length, m.getTo().getAddress(),
                                    m.getTo().getPort());
        try {
            socket = new DatagramSocket();
            socket.send(packet);
            socket.close();
            logger.info("ReducerServer@" + reducerServer.getAddress() + ":" + reducerServer.getPort() +" -> mensagem enviada com sucesso");
        } catch (Exception ex) {
            logger.warning("ReducerServer@" + reducerServer.getAddress() + ":" + reducerServer.getPort() +"-> mensagem não enviada" + ex );
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
            socket = new DatagramSocket(reducerServer.getPort());
            while (true) {
                sleep();
                packet = new DatagramPacket(buffer, buffer.length, reducerServer.getAddress(), reducerServer.getPort());

                socket.receive(packet);
                logger.info("ReducerServer@" + reducerServer.getAddress() + ":" + reducerServer.getPort() +" -> mensagem recebida com sucesso");
                jsonMessage = new String(packet.getData()).trim();
                message = gson.fromJson(jsonMessage, Message.class);

                processQueue.add(message);
                logger.info("ReducerServer@" + reducerServer.getAddress() + ":" + reducerServer.getPort() +" -> mensagem adicionada na processQueue ");
                cleanBuffer(buffer);
            }
        } catch (Exception ex) {
            logger.warning("ReducerServer@" + reducerServer.getAddress() + ":" + reducerServer.getPort() +" -> falha no recive" + ex);
        }
    }
    
    public void process() {
        while (true) {
            sleep();
            Message m = processQueue.poll();
            if(m != null) {
                logger.info("ReducerServer@" + reducerServer.getAddress() + ":" + reducerServer.getPort() +" -> mensagem enviada para processamento ");
                processMessage(m);
            }
        }
    }
    
    public void processMessage(Message m) {

        if(history.containsKey(m.getRequestor())) {
            logger.info("ReducerServer@" + reducerServer.getAddress() + ":" + reducerServer.getPort() +" -> requestor <" + m.getRequestor() + "> já instanciado. Nova mensagem add ");
            LinkedList<Message> auxiliar = history.get(m.getRequestor());
            auxiliar.add(m);
            history.put(m.getRequestor(), auxiliar);
        } else {
            logger.info("ReducerServer@" + reducerServer.getAddress() + ":" + reducerServer.getPort() +" -> novo requestor <" + m.getRequestor() + ">. Nova mensagem add ");
            LinkedList<Message> auxiliar = new LinkedList<>();
            auxiliar.add(m);
            history.put(m.getRequestor(), auxiliar);
        }
        
        LinkedList<Message> mapperList = history.get(m.getRequestor());
        
        if(m.getEnd().equals(new Long(mapperList.size()))) {
            logger.info("ReducerServer@" + reducerServer.getAddress() + ":" + reducerServer.getPort() +" -> todas as mensagens para o requestor <" + m.getRequestor() + "> chegaram. Reduzindo");
            reduce(m.getRequestor());
            logger.info("ReducerServer@" + reducerServer.getAddress() + ":" + reducerServer.getPort() +" -> limpando requisição para o requestor <" + m.getRequestor() + ">.");
            history.remove(m.getRequestor());
        }
    }

    public void reduce(User requestor) {
        LinkedTreeMap<String, Set<String>> invertedLinks = new LinkedTreeMap<>();
        LinkedList<Message> messages = history.get(requestor);
        
        for(Message m: messages) {
            LinkedTreeMap<String, List<String>> messageMap =
                    (LinkedTreeMap<String, List<String>>) m.getContent();
            
            for(Entry<String, List<String>> e: messageMap.entrySet()) {
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
        logger.info("ReducerServer@" + reducerServer.getAddress() + ":" + reducerServer.getPort() +" -> add mensagem na fila sendQueue");
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
