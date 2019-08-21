package com.github.brdr3.mapreduce.server;

import com.github.brdr3.mapreduce.mapper.Mapper;
import com.github.brdr3.mapreduce.util.Message;
import com.github.brdr3.mapreduce.util.Message.MessageBuilder;
import com.github.brdr3.mapreduce.util.User;
import com.github.brdr3.mapreduce.util.constants.Constants;
import com.google.gson.Gson;

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

        logger.info("CoordinatorServer instanciado com sucesso");
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
            logger.info("CoordinatorServer enviando mensagem buffersize: " + buffer.length + " toAddr: " + m.getTo().getAddress() + " toPort" + m.getTo().getPort());
            socket = new DatagramSocket();
            socket.send(packet);
            socket.close();
            logger.info("CoordinatorServer enviando mensagem sucesso buffersize: " + buffer.length + " toAddr: " + m.getTo().getAddress() + " toPort" + m.getTo().getPort());
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
                logger.info("Recebendo mensagem buffersize: " + buffer.length + " toAddr: " + coordinatorServer.getAddress() + " toPort " + coordinatorServer.getPort());
                packet = new DatagramPacket(buffer, buffer.length, coordinatorServer.getAddress(), coordinatorServer.getPort());

                socket.receive(packet);

                jsonMessage = new String(packet.getData()).trim();
                message = gson.fromJson(jsonMessage, Message.class);

                processQueue.add(message);
                cleanBuffer(buffer);
                logger.info("Sucesso buffersize: " + buffer.length + " toAddr: " + coordinatorServer.getAddress() + " toPort " + coordinatorServer.getPort());
            }
        } catch (Exception ex) {
            logger.warning("Erro " + ex + " buffersize: " + buffer.length + " toAddr: " + coordinatorServer.getAddress() + " toPort " + coordinatorServer.getPort());
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
        if(m.getContent() instanceof User) {
            mappers.add((User)m.getContent());
            return;
        }
        
        LinkedList[] mapperLists;
        LinkedList<Message> messages = new LinkedList<>();
        logger.info("Processando mensagem");

        if(m.getContent() instanceof List) {
            mapperLists = divideList((List) m.getContent());
        }
        else {
            logger.warning("CoordinatorServer Processando mensagem não conseguiu instanciar");
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
            logger.info("Mensagem enviando para fila id: " + message.getId());
            message.setEnd(new Long(messages.size()));
            senderQueue.add(message);
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
