package com.trident.load_balancer;

import java.net.Socket;

interface ModifiableNode extends Node {
    void setSocket(Socket socket);

    void updateConnections(int connections);
}
