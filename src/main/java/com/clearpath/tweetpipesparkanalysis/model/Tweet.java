package com.clearpath.tweetpipesparkanalysis.model;

public class Tweet {

    private String text;

    public Tweet() {}

    public Tweet(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "text='" + text + '\'' +
                '}';
    }
}
