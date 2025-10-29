package com.ctrip.framework.drc.console.vo.v2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengquanliang
 * 2025/5/20 11:14
 */
public class IncompatibleMessengerView {

    private List<IncompatibleMessengerDto> qmqMessengerDtos = new ArrayList<>();
    private List<IncompatibleMessengerDto> kafkaMessengerDtos = new ArrayList<>();

    public IncompatibleMessengerView(List<IncompatibleMessengerDto> qmqMessengerDtos, List<IncompatibleMessengerDto> kafkaMessengerDtos) {
        this.qmqMessengerDtos = qmqMessengerDtos;
        this.kafkaMessengerDtos = kafkaMessengerDtos;
    }

    public IncompatibleMessengerView() {
    }

    public List<IncompatibleMessengerDto> getQmqMessengerDtos() {
        return qmqMessengerDtos;
    }

    public void setQmqMessengerDtos(List<IncompatibleMessengerDto> qmqMessengerDtos) {
        this.qmqMessengerDtos = qmqMessengerDtos;
    }

    public List<IncompatibleMessengerDto> getKafkaMessengerDtos() {
        return kafkaMessengerDtos;
    }

    public void setKafkaMessengerDtos(List<IncompatibleMessengerDto> kafkaMessengerDtos) {
        this.kafkaMessengerDtos = kafkaMessengerDtos;
    }
}
