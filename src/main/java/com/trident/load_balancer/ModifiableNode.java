package com.trident.load_balancer;

import java.net.URI;

interface ModifiableNode extends Node {
    void setURI(URI uri);

    void updateConnections(int connections);
}
