package org.kaaproject.kaa.common.channels.protocols.mqtt.listeners;


import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;

public interface DeliveryCompleteListener {
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken);
}
