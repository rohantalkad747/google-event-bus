package com.trident.load_balancer;

interface Connectable {
    void onConnect();
    void onDisconnect();
}
