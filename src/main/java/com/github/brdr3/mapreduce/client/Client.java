package com.github.brdr3.mapreduce.client;

import com.github.brdr3.mapreduce.util.Message;
import com.github.brdr3.mapreduce.util.Message.MessageBuilder;
import com.github.brdr3.mapreduce.util.User;
import com.github.brdr3.mapreduce.util.constants.Constants;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
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

    private ConcurrentLinkedQueue<ArrayList<String>> senderQueue;
    private ConcurrentLinkedQueue<Message> processQueue;

    static Logger logger = Logger.getLogger("log4j.properties");

    public Client() {

        try {
            String address = null;
            

            int port = 14000;
            thisUser = new User(0, address, port);
            logger.info("Cliente instanciada com sucesso addr: " + address + " port: " + port);
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
            ArrayList urls = senderQueue.poll();
            sleep();
            if (urls != null) {
                sendMessage(urls);
            }
        }
    }

    public void sendMessage(ArrayList<String> urls) {
        logger.info("Cliente enviando mensagem ");
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
            logger.info("Cliente enviando mensagem bufferSize:" + buffer.length + " to addrs: " + m.getTo().getAddress() + " to port: " + m.getTo().getPort());
            socket = new DatagramSocket();
            socket.send(packet);
            socket.close();
            logger.info("Mensagem enviada:" + m.toString());
        } catch (Exception ex) {
            logger.warning("Cliente não conseguiu enviar mensagem " + ex);
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
        while (true) {
            sleep();
            System.out.println("Digite os urls separados por espaço");
            userEntry = x.nextLine();
            ArrayList<String> urls;
            if (userEntry.contains(" ")) {
                urls = new ArrayList<String>(Arrays.asList(userEntry.split(" ")));
            } else {
                urls = new ArrayList<>();
                urls.add(userEntry);
            }
            senderQueue.add(urls);
        }
    }

    public void process() {
        while (true) {
            sleep();
            Message m = processQueue.poll();
            if (m != null) {
                processMessage(m);
            }
        }
    }

    public void processMessage(Message m) {
        LinkedTreeMap<String, Set<String>> pointedLinks
                = (LinkedTreeMap<String, Set<String>>) m.getContent();
        logger.info("Cliente processando mensgem ");
        System.out.println(pointedLinks);
    }

    private void cleanBuffer(byte[] buffer) {
        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = 0;
        }
    }

    private void sleep() {
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }
}
