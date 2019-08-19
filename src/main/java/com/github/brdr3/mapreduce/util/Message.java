package com.github.brdr3.mapreduce.util;

import com.google.gson.Gson;

public class Message {
    private Long id;
    private Long end; 
    private Object content;
    private User from;
    private User to;
    private User requestor;

    public User getRequestor() {
        return requestor;
    }

    public void setRequestor(User requestor) {
        this.requestor = requestor;
    }
    
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Object getContent() {
        return content;
    }

    public void setContent(Object content) {
        this.content = content;
    }

    public User getFrom() {
        return from;
    }

    public void setFrom(User from) {
        this.from = from;
    }

    public User getTo() {
        return to;
    }

    public void setTo(User to) {
        this.to = to;
    }
    
    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }
    
    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
    
    public static class MessageBuilder {
         
        Message m;
        
        public MessageBuilder() {
            m = new Message();
        }
        
        public MessageBuilder id(Long id) {
            m.setId(id);
            return this;
        }
        
        public MessageBuilder to(User to) {
            m.setTo(to);
            return this;
        }
        
        public MessageBuilder from(User from) {
            m.setFrom(from);
            return this;
        }
        
        public MessageBuilder content(Object content) {
            m.setContent(content);
            return this;
        }
        
        public MessageBuilder requestor(User requisitor) {
            m.setRequestor(requisitor);
            return this;
        }
        
        public MessageBuilder end(Long end) {
            m.setEnd(end);
            return this;
        }
        
        public Message build() {
            return m;
        }
    }
}
