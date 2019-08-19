package com.github.brdr3.mapreduce.util.constants;

import com.github.brdr3.mapreduce.util.User;

public class Constants {
    public static User coordinatorServer = new User(-1, "localhost", 14010);
    public static User reducerServer = new User(-2, "localhost", 12000);
}
