"use strict";
/*
Copyright (c) 2019, because-why-not.com Limited
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
Object.defineProperty(exports, "__esModule", { value: true });
exports.WebsocketNetworkServer = exports.Endpoint = void 0;
var WebSocket = require("ws");
var url = require("url");
var inet = require("./INetwork");
/**
 * Gathers all data related to a single websocket.
 *
 */
var Endpoint = /** @class */ (function () {
    function Endpoint() {
    }
    Endpoint.prototype.getConnectionInfo = function () {
        return this.remoteAddress + ":" + this.remotePort + " " + this.appPath;
    };
    Endpoint.prototype.getLocalConnectionInfo = function () {
        if (this.localAddress && this.localPort)
            return this.localAddress + ":" + this.localPort;
        return "unknown";
    };
    return Endpoint;
}());
exports.Endpoint = Endpoint;
var WebsocketNetworkServer = /** @class */ (function () {
    function WebsocketNetworkServer() {
        this.mPool = {};
    }
    WebsocketNetworkServer.SetLogLevel = function (verbose) {
        WebsocketNetworkServer.sVerboseLog = verbose;
    };
    WebsocketNetworkServer.logv = function (msg) {
        if (WebsocketNetworkServer.sVerboseLog) {
            console.log("(" + new Date().toISOString() + ")" + msg);
        }
    };
    WebsocketNetworkServer.prototype.onConnection = function (ep) {
        this.mPool[ep.appPath].add(ep);
    };
    /**Adds a new websocket server that will be used to receive incoming connections for
     * the given apps.
     *
     * @param websocketServer server used for the incoming connections
     * @param appConfig app the incoming connections are allowed to connect to
     * Apps can be given multiple times with different signaling servers to support different
     * ports and protocols.
     */
    WebsocketNetworkServer.prototype.addSocketServer = function (websocketServer, appConfigs) {
        var _this = this;
        for (var i = 0; i < appConfigs.length; i++) {
            var app = appConfigs[i];
            if ((app.path in this.mPool) == false) {
                console.log("Add new pool " + app.path);
                this.mPool[app.path] = new PeerPool(app);
            }
        }
        ;
        websocketServer.on('connection', function (socket, request) {
            var ep = new Endpoint();
            ep.ws = socket;
            ep.remoteAddress = request.socket.remoteAddress;
            ep.remotePort = request.socket.remotePort;
            ep.localAddress = request.socket.localAddress;
            ep.localPort = request.socket.localPort;
            ep.appPath = url.parse(request.url).pathname;
            if (ep.appPath in _this.mPool) {
                if (WebsocketNetworkServer.sVerboseLog)
                    console.log("New websocket connection:" + ep.getConnectionInfo());
                _this.onConnection(ep);
            }
            else {
                console.error("Websocket tried to connect to unknown app " + ep.appPath);
                socket.close();
            }
        });
    };
    WebsocketNetworkServer.sVerboseLog = true;
    return WebsocketNetworkServer;
}());
exports.WebsocketNetworkServer = WebsocketNetworkServer;
;
//Pool of client connects that are allowed to communicate to each other
var PeerPool = /** @class */ (function () {
    function PeerPool(config) {
        this.mConnections = new Array();
        this.mServers = {};
        this.mAddressSharing = false;
        this.maxAddressLength = 256;
        this.mAppConfig = config;
        if (this.mAppConfig.address_sharing) {
            this.mAddressSharing = this.mAppConfig.address_sharing;
        }
    }
    PeerPool.prototype.hasAddressSharing = function () {
        return this.mAddressSharing;
    };
    //add a new connection based on this websocket
    PeerPool.prototype.add = function (ep) {
        var peer = new SignalingPeer(this, ep);
        this.mConnections.push(peer);
    };
    //Returns the SignalingClientConnection that opened a server using the given address
    //or null if address not in use
    PeerPool.prototype.getServerConnection = function (address) {
        return this.mServers[address];
    };
    //Tests if the address is available for use. 
    //returns true in the following cases
    //the address is longer than the maxAddressLength and the server the address is not yet in use or address sharing is active
    PeerPool.prototype.isAddressAvailable = function (address) {
        if (address.length <= this.maxAddressLength // only allow addresses shorter than maxAddressLength
            && (this.mServers[address] == null || this.mAddressSharing)) {
            return true;
        }
        return false;
    };
    //Adds the server. No checking is performed here! logic should be solely in the connection class
    PeerPool.prototype.addServer = function (client, address) {
        if (this.mServers[address] == null) {
            this.mServers[address] = new Array();
        }
        this.mServers[address].push(client);
    };
    //Removes an address from the server. No checks performed
    PeerPool.prototype.removeServer = function (client, address) {
        //supports address sharing. remove the client from the server list that share the address
        var index = this.mServers[address].indexOf(client);
        if (index != -1) {
            this.mServers[address].splice(index, 1);
        }
        //delete the whole list if the last one left
        if (this.mServers[address].length == 0) {
            delete this.mServers[address];
            WebsocketNetworkServer.logv("Address " + address + " released.");
        }
    };
    //Removes a given connection from the pool
    PeerPool.prototype.removeConnection = function (client) {
        var index = this.mConnections.indexOf(client);
        if (index != -1) {
            this.mConnections.splice(index, 1);
        }
        else {
            console.warn("Tried to remove unknown SignalingClientConnection. Bug?" + client.GetLogPrefix());
        }
    };
    PeerPool.prototype.count = function () {
        return this.mConnections.length;
    };
    return PeerPool;
}());
var SignalingConnectionState;
(function (SignalingConnectionState) {
    SignalingConnectionState[SignalingConnectionState["Uninitialized"] = 0] = "Uninitialized";
    SignalingConnectionState[SignalingConnectionState["Connecting"] = 1] = "Connecting";
    SignalingConnectionState[SignalingConnectionState["Connected"] = 2] = "Connected";
    SignalingConnectionState[SignalingConnectionState["Disconnecting"] = 3] = "Disconnecting";
    SignalingConnectionState[SignalingConnectionState["Disconnected"] = 4] = "Disconnected"; //means the instance is destroyed and unusable
})(SignalingConnectionState || (SignalingConnectionState = {}));
;
///note: all methods starting with "internal" might leave the system in an inconsistent state
///e.g. peerA is connected to peerB means peerB is connected to peerA but internalRemoveConnection
///could cause peerA being disconnected from peerB but peerB still thinking to be connected to peerA!!!
var SignalingPeer = /** @class */ (function () {
    function SignalingPeer(pool, ep) {
        var _this = this;
        this.mState = SignalingConnectionState.Uninitialized;
        this.mConnections = {};
        //C# version uses short so 16384 is 50% of the positive numbers (maybe might make sense to change to ushort or int)
        this.mNextIncomingConnectionId = new inet.ConnectionId(16384);
        /// <summary>
        /// Assume 1 until message received
        /// </summary>
        this.mRemoteProtocolVersion = 1;
        this.mConnectionPool = pool;
        this.mEndPoint = ep;
        this.mPongReceived = true;
        this.mState = SignalingConnectionState.Connecting;
        WebsocketNetworkServer.logv("[" + this.mEndPoint.getConnectionInfo() + "]" +
            " connected on " + this.mEndPoint.getLocalConnectionInfo());
        this.mEndPoint.ws.on('message', function (message, flags) {
            _this.onMessage(message, flags);
        });
        this.mEndPoint.ws.on('error', function (error) {
            console.error(error);
        });
        this.mEndPoint.ws.on('close', function (code, message) { _this.onClose(code, message); });
        this.mEndPoint.ws.on('pong', function (data, flags) {
            _this.mPongReceived = true;
            _this.logInc("pong");
        });
        this.mState = SignalingConnectionState.Connected;
        this.mPingInterval = setInterval(function () { _this.doPing(); }, 30000);
    }
    SignalingPeer.prototype.GetLogPrefix = function () {
        //used to identify this peer for log messages / debugging
        return "[" + this.mEndPoint.getConnectionInfo() + "]";
    };
    SignalingPeer.prototype.doPing = function () {
        if (this.mState == SignalingConnectionState.Connected && this.mEndPoint.ws.readyState == WebSocket.OPEN) {
            if (this.mPongReceived == false) {
                this.NoPongTimeout();
                return;
            }
            this.mPongReceived = false;
            this.mEndPoint.ws.ping();
            this.logOut("ping");
        }
    };
    SignalingPeer.prototype.evtToString = function (evt) {
        var output = "[";
        output += "NetEventType: (";
        output += inet.NetEventType[evt.Type];
        output += "), id: (";
        output += evt.ConnectionId.id;
        if (evt.Info != null) {
            output += "), Data: (";
            output += evt.Info;
        }
        else if (evt.MessageData != null) {
            var chars = new Uint16Array(evt.MessageData.buffer, evt.MessageData.byteOffset, evt.MessageData.byteLength / 2);
            output += "), Data: (";
            var binaryString = "";
            for (var i = 0; i < chars.length; i++) {
                binaryString += String.fromCharCode(chars[i]);
            }
            output += binaryString;
        }
        output += ")]";
        return output;
    };
    SignalingPeer.prototype.onMessage = function (inmessage, flags) {
        try {
            var msg = inmessage;
            this.parseMessage(msg);
        }
        catch (err) {
            WebsocketNetworkServer.logv(this.GetLogPrefix() + " Invalid message received: " + inmessage + "  \n Error: " + err);
        }
    };
    SignalingPeer.prototype.sendToClient = function (evt) {
        //this method is also called during cleanup after a disconnect
        //check first if we are still connected
        //bugfix: apprently 2 sockets can be closed at exactly the same time without
        //onclosed being called immediately -> socket has to be checked if open
        if (this.mState == SignalingConnectionState.Connected
            && this.mEndPoint.ws.readyState == WebSocket.OPEN) {
            this.logOut(this.evtToString(evt));
            var msg = inet.NetworkEvent.toByteArray(evt);
            this.internalSend(msg);
        }
    };
    SignalingPeer.prototype.logOut = function (msg) {
        WebsocketNetworkServer.logv(this.GetLogPrefix() + "OUT: " + msg);
    };
    SignalingPeer.prototype.logInc = function (msg) {
        WebsocketNetworkServer.logv(this.GetLogPrefix() + "INC: " + msg);
    };
    SignalingPeer.prototype.sendVersion = function () {
        var msg = new Uint8Array(2);
        var ver = SignalingPeer.PROTOCOL_VERSION;
        msg[0] = inet.NetEventType.MetaVersion;
        msg[1] = ver;
        this.logOut("version " + ver);
        this.internalSend(msg);
    };
    SignalingPeer.prototype.sendHeartbeat = function () {
        var msg = new Uint8Array(1);
        msg[0] = inet.NetEventType.MetaHeartbeat;
        this.logOut("heartbeat");
        this.internalSend(msg);
    };
    SignalingPeer.prototype.internalSend = function (msg) {
        this.mEndPoint.ws.send(msg);
    };
    SignalingPeer.prototype.onClose = function (code, error) {
        WebsocketNetworkServer.logv(this.GetLogPrefix() + " CLOSED!");
        this.Cleanup();
    };
    SignalingPeer.prototype.NoPongTimeout = function () {
        WebsocketNetworkServer.logv(this.GetLogPrefix() + " TIMEOUT!");
        this.Cleanup();
    };
    //used for onClose or NoPongTimeout
    SignalingPeer.prototype.Cleanup = function () {
        //if the connection was cleaned up during a timeout it might get triggered again during closing.
        if (this.mState === SignalingConnectionState.Disconnecting || this.mState === SignalingConnectionState.Disconnected)
            return;
        this.mState = SignalingConnectionState.Disconnecting;
        WebsocketNetworkServer.logv(this.GetLogPrefix() + " disconnecting.");
        if (this.mPingInterval != null) {
            clearInterval(this.mPingInterval);
        }
        this.mConnectionPool.removeConnection(this);
        //disconnect all connections
        var test = this.mConnections; //workaround for not having a proper dictionary yet...
        for (var v in this.mConnections) {
            if (this.mConnections.hasOwnProperty(v))
                this.disconnect(new inet.ConnectionId(+v));
        }
        //make sure the server address is freed 
        if (this.mServerAddress != null) {
            this.stopServer();
        }
        this.mEndPoint.ws.terminate();
        WebsocketNetworkServer.logv(this.GetLogPrefix() + "removed"
            + " " + this.mConnectionPool.count()
            + " connections left in pool ");
        this.mState = SignalingConnectionState.Disconnected;
    };
    SignalingPeer.prototype.parseMessage = function (msg) {
        if (msg[0] == inet.NetEventType.MetaVersion) {
            var v = msg[1];
            this.logInc("protocol version " + v);
            this.mRemoteProtocolVersion = v;
            this.sendVersion();
        }
        else if (msg[0] == inet.NetEventType.MetaHeartbeat) {
            this.logInc("heartbeat");
            this.sendHeartbeat();
        }
        else {
            var evt = inet.NetworkEvent.fromByteArray(msg);
            this.logInc(this.evtToString(evt));
            this.handleIncomingEvent(evt);
        }
    };
    SignalingPeer.prototype.handleIncomingEvent = function (evt) {
        //update internal state based on the event
        if (evt.Type == inet.NetEventType.NewConnection) {
            //client wants to connect to another client
            var address = evt.Info;
            //the id this connection should be addressed with
            var newConnectionId = evt.ConnectionId;
            this.connect(address, newConnectionId);
        }
        else if (evt.Type == inet.NetEventType.ConnectionFailed) {
            //should never be received
        }
        else if (evt.Type == inet.NetEventType.Disconnected) {
            //peer tries to disconnect from another peer
            var otherPeerId = evt.ConnectionId;
            this.disconnect(otherPeerId);
        }
        else if (evt.Type == inet.NetEventType.ServerInitialized) {
            this.startServer(evt.Info);
        }
        else if (evt.Type == inet.NetEventType.ServerInitFailed) {
            //should never happen
        }
        else if (evt.Type == inet.NetEventType.ServerClosed) {
            //stop server request
            this.stopServer();
        }
        else if (evt.Type == inet.NetEventType.ReliableMessageReceived) {
            this.sendData(evt.ConnectionId, evt.MessageData, true);
        }
        else if (evt.Type == inet.NetEventType.UnreliableMessageReceived) {
            this.sendData(evt.ConnectionId, evt.MessageData, false);
        }
    };
    SignalingPeer.prototype.internalAddIncomingPeer = function (peer) {
        //another peer connected to this (while allowing incoming connections)
        //store the reference
        var id = this.nextConnectionId();
        this.mConnections[id.id] = peer;
        //event to this (the other peer gets the event via addOutgoing
        this.sendToClient(new inet.NetworkEvent(inet.NetEventType.NewConnection, id, null));
    };
    SignalingPeer.prototype.internalAddOutgoingPeer = function (peer, id) {
        //this peer successfully connected to another peer. id was generated on the 
        //client side
        this.mConnections[id.id] = peer;
        //event to this (the other peer gets the event via addOutgoing
        this.sendToClient(new inet.NetworkEvent(inet.NetEventType.NewConnection, id, null));
    };
    SignalingPeer.prototype.internalRemovePeer = function (id) {
        delete this.mConnections[id.id];
        this.sendToClient(new inet.NetworkEvent(inet.NetEventType.Disconnected, id, null));
    };
    //test this. might cause problems
    //the number is converted to string trough java script but we need get back the number
    //for creating the connection id
    SignalingPeer.prototype.findPeerConnectionId = function (otherPeer) {
        for (var peer in this.mConnections) {
            if (this.mConnections[peer] === otherPeer) {
                return new inet.ConnectionId(+peer);
            }
        }
    };
    SignalingPeer.prototype.nextConnectionId = function () {
        var result = this.mNextIncomingConnectionId;
        this.mNextIncomingConnectionId = new inet.ConnectionId(this.mNextIncomingConnectionId.id + 1);
        return result;
    };
    //public methods (not really needed but can be used for testing or server side deubgging)
    //this peer initializes a connection to a certain address. The connection id is set by the client
    //to allow tracking of the connection attempt
    SignalingPeer.prototype.connect = function (address, newConnectionId) {
        var serverConnections = this.mConnectionPool.getServerConnection(address);
        //
        if (serverConnections != null && serverConnections.length == 1) {
            //inform the server connection about the new peer
            //events will be send by these methods
            //shared addresses -> connect to everyone listening
            serverConnections[0].internalAddIncomingPeer(this);
            this.internalAddOutgoingPeer(serverConnections[0], newConnectionId);
        }
        else {
            //if address is not in use or it is in multi join mode -> connection fails
            this.sendToClient(new inet.NetworkEvent(inet.NetEventType.ConnectionFailed, newConnectionId, null));
        }
    };
    //join connection happens if another user joins a multi address. it will connect to every address
    //listening to that room
    SignalingPeer.prototype.connectJoin = function (address) {
        var serverConnections = this.mConnectionPool.getServerConnection(address);
        //in join mode every connection is incoming as everyone listens together
        if (serverConnections != null) {
            for (var _i = 0, serverConnections_1 = serverConnections; _i < serverConnections_1.length; _i++) {
                var v = serverConnections_1[_i];
                if (v != this) { //avoid connecting the peer to itself
                    v.internalAddIncomingPeer(this);
                    this.internalAddIncomingPeer(v);
                }
            }
        }
    };
    SignalingPeer.prototype.disconnect = function (connectionId) {
        var otherPeer = this.mConnections[connectionId.id];
        if (otherPeer != null) {
            var idOfOther = otherPeer.findPeerConnectionId(this);
            //find the connection id the other peer uses to talk to this one
            this.internalRemovePeer(connectionId);
            otherPeer.internalRemovePeer(idOfOther);
        }
        else {
            //the connectionid isn't connected 
            //invalid -> do nothing or log?
        }
    };
    SignalingPeer.prototype.startServer = function (address) {
        //what to do if it is already a server?
        if (this.mServerAddress != null)
            this.stopServer();
        if (this.mConnectionPool.isAddressAvailable(address)) {
            this.mServerAddress = address;
            this.mConnectionPool.addServer(this, address);
            this.sendToClient(new inet.NetworkEvent(inet.NetEventType.ServerInitialized, inet.ConnectionId.INVALID, address));
            if (this.mConnectionPool.hasAddressSharing()) {
                //address sharing is active. connect to every endpoint already listening on this address
                this.connectJoin(address);
            }
        }
        else {
            this.sendToClient(new inet.NetworkEvent(inet.NetEventType.ServerInitFailed, inet.ConnectionId.INVALID, address));
        }
    };
    SignalingPeer.prototype.stopServer = function () {
        if (this.mServerAddress != null) {
            this.mConnectionPool.removeServer(this, this.mServerAddress);
            this.sendToClient(new inet.NetworkEvent(inet.NetEventType.ServerClosed, inet.ConnectionId.INVALID, null));
            this.mServerAddress = null;
        }
        //do nothing if it wasnt a server
    };
    //delivers the message to the local peer
    SignalingPeer.prototype.forwardMessage = function (senderPeer, msg, reliable) {
        var id = this.findPeerConnectionId(senderPeer);
        if (reliable)
            this.sendToClient(new inet.NetworkEvent(inet.NetEventType.ReliableMessageReceived, id, msg));
        else
            this.sendToClient(new inet.NetworkEvent(inet.NetEventType.UnreliableMessageReceived, id, msg));
    };
    SignalingPeer.prototype.sendData = function (id, msg, reliable) {
        var peer = this.mConnections[id.id];
        if (peer != null)
            peer.forwardMessage(this, msg, reliable);
    };
    /// <summary>
    /// Version of the protocol implemented here
    /// </summary>
    SignalingPeer.PROTOCOL_VERSION = 2;
    /// <summary>
    /// Minimal protocol version that is still supported.
    /// V 1 servers won't understand heartbeat and version
    /// messages but would just log an unknown message and
    /// continue normally.
    /// </summary>
    SignalingPeer.PROTOCOL_VERSION_MIN = 1;
    return SignalingPeer;
}());
//# sourceMappingURL=WebsocketNetworkServer.js.map