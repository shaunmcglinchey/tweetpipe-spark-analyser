package com.clearpath.tweetpipesparkanalysis.service;

import com.clearpath.tweetpipesparkanalysis.model.Tweet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.stereotype.Component;

@Component
@EnableBinding(Sink.class)
@Slf4j
public class TweetListener {

    @StreamListener(Sink.INPUT)
    public void handle(Tweet tweet) {
        log.info("Tweet received: {}", tweet);
    }
}
