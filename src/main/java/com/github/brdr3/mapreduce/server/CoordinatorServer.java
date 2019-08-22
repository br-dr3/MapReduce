package com.github.brdr3.mapreduce.server;

import com.github.brdr3.mapreduce.mapper.Mapper;
import com.github.brdr3.mapreduce.util.Message;
import com.github.brdr3.mapreduce.util.Message.MessageBuilder;
import com.github.brdr3.mapreduce.util.User;
import com.github.brdr3.mapreduce.util.constants.Constants;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;

import java.lang.reflect.Array;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;


public class CoordinatorServer {
    
    private final User coordinatorServer = Constants.coordinatorServer;
    private final Thread sender;
    private final Thread receiver;
    private final Thread processor;
    
    private ArrayList<User> mappers;
    
    private ConcurrentLinkedQueue<Message> senderQueue;
    private ConcurrentLinkedQueue<Message> processQueue;

    static Logger logger = Logger.getLogger("log4j.properties");

    public CoordinatorServer() {
        
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
        
        mappers = new ArrayList<>();        
        senderQueue = new ConcurrentLinkedQueue<>();
        processQueue = new ConcurrentLinkedQueue<>();

        logger.info("CoordinatorServer" + coordinatorServer + " -> instanciado com sucesso");
    }
    
    public void start() {
        sender.start();
        receiver.start();
        processor.start();
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
    
    public void sendMessage(Message m) {
        Gson gson = new Gson();
        DatagramSocket socket;
        DatagramPacket packet;

        String jsonMessage = gson.toJson(m);
        byte buffer[] = jsonMessage.getBytes();

        packet = new DatagramPacket(buffer, buffer.length, m.getTo().getAddress(),
                                    m.getTo().getPort());
        try {
            logger.info("CoordinatorServer" + coordinatorServer + " -> enviando mensagem User" + m.getTo() );
            socket = new DatagramSocket();
            socket.send(packet);
            socket.close();
            logger.info("CoordinatorServer" + coordinatorServer + " -> mensagem enviada User" + m.getTo() );
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
                sleep();

                packet = new DatagramPacket(buffer, buffer.length, coordinatorServer.getAddress(), coordinatorServer.getPort());

                socket.receive(packet);
                jsonMessage = new String(packet.getData()).trim();
                message = gson.fromJson(jsonMessage, Message.class);
                processQueue.add(message);
                cleanBuffer(buffer);
                logger.info("CoordinatorServer" + coordinatorServer + " -> mensagem recebida e adicionada na fila User" + message.getFrom() );
            }
        } catch (Exception ex) {
            logger.info("CoordinatorServer" + coordinatorServer + " -> erro ao receber mensagem." + ex );
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
        if(m.getContent() instanceof LinkedTreeMap) {
            Gson gson = new Gson();
            String content = gson.toJson(m.getContent());
            User u = gson.fromJson(content, User.class);
            mappers.add(u);
            logger.info("CoordinatorServer" + coordinatorServer + " -> novo mapper identificado. Mappper" + u );
            return;
        }



        LinkedList[] mapperLists;
        LinkedList<Message> messages = new LinkedList<>();

        if(m.getContent() instanceof List) {
            mapperLists = divideList((List) m.getContent());
            logger.info("CoordinatorServer" + coordinatorServer + " -> Mensagem do cliente foi dividida em " + mapperLists.length+ ". Cliente" + m.getFrom() );
        }
        else {
            logger.info("CoordinatorServer" + coordinatorServer + " -> Mensagem em formato desconhecido" );
            return;
        }
        for(int i = 0; i < mapperLists.length; i++) {

            if(mapperLists[i] != null) {
                Message messageToMapper = new MessageBuilder().from(coordinatorServer)
                                                       .id(new Long(i))
                                                       .to(mappers.get(i))
                                                       .content(mapperLists[i])
                                                       .requestor(m.getRequestor())
                                                       .build();
                messages.add(messageToMapper);
            }
        }
        
        for(Message message: messages) {
            message.setEnd(new Long(messages.size()));
            senderQueue.add(message);
            logger.info("CoordinatorServer" + coordinatorServer + " -> Mensagem colocada na lista de envio. Mapper" + message.getTo() );
        }
    }
    
    private LinkedList[] divideList(List<String> urls) {
        LinkedList[] mapperList = new LinkedList [mappers.size()];
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

    private void sleep(){
        try {
            Thread.sleep(100);
        } catch ( Exception e ){
            System.out.println(e);
        }
    }
}
