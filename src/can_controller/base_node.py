#!/usr/bin/env python3
"""
Base Node Class - Foundation for all OBS nodes with communication capabilities
"""

import os
import sys
import json
import socket
import threading
import time
import logging
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass
from enum import Enum

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MessageType(Enum):
    """Message types for node communication"""
    COMMAND = "command"
    RESPONSE = "response"
    STATUS = "status"
    EMERGENCY = "emergency"
    HEARTBEAT = "heartbeat"
    DATA = "data"
    CONFIG_UPDATE = "config_update"

class Priority(Enum):
    """Message priority levels"""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4
    EMERGENCY = 5

@dataclass
class NodeMessage:
    """Standard message format for node communication"""
    message_id: str
    type: MessageType
    priority: Priority
    source: str
    destination: str
    payload: Dict[str, Any]
    timestamp: float
    ttl: Optional[float] = None
    requires_ack: bool = False
    ack_received: bool = False

class BaseNode:
    """
    Base class for all OBS nodes with communication capabilities
    
    Features:
    - Direct node-to-node communication
    - Master core communication
    - Emergency communication protocols
    - Heartbeat monitoring
    - Message queuing and routing
    """
    
    def __init__(self, node_name: str, config: Dict[str, Any]):
        self.node_name = node_name
        self.config = config  # Local node config (for testing/override)
        self.node_id = str(uuid.uuid4())
        self.status = "INITIALIZING"
        self.last_heartbeat = time.time()
        
        # Communication settings
        self.master_core_host = config.get("master_core_host", "localhost")
        self.master_core_port = config.get("master_core_port", 14550)
        self.node_port = config.get("node_port", 14551)
        
        # Direct communication settings
        self.direct_communication_enabled = config.get("direct_communication", True)
        self.emergency_nodes = config.get("emergency_nodes", [])
        
        # Configuration management (Enterprise pattern: Master Core > Local > Default)
        self.master_core_config: Dict[str, Any] = {}  # Centralized config from Master Core
        
        # Message handling
        self.message_handlers: Dict[str, Callable] = {}
        self.message_queue: List[NodeMessage] = []
        self.pending_acks: Dict[str, NodeMessage] = {}
        
        # Networking
        self.udp_socket = None
        self.listening = False
        self.listen_thread = None
        
        # Register default handlers
        self._register_default_handlers()
        
        logger.info(f"Node {self.node_name} initialized with ID {self.node_id}")
    
    def _register_default_handlers(self):
        """Register default message handlers"""
        self.message_handlers = {
            "heartbeat": self._handle_heartbeat,
            "status": self._handle_status,
            "emergency": self._handle_emergency,
            "ack": self._handle_ack,
            "config_update": self._handle_config_update
        }
    
    def start(self):
        """Start the node"""
        try:
            self.status = "STARTING"
            self._start_communication()
            self.status = "RUNNING"
            logger.info(f"Node {self.node_name} started successfully")
            return True
        except Exception as e:
            self.status = "ERROR"
            logger.error(f"Failed to start node {self.node_name}: {e}")
            return False
    
    def stop(self):
        """Stop the node"""
        try:
            self.status = "STOPPING"
            self.listening = False
            if self.udp_socket:
                self.udp_socket.close()
            if self.listen_thread:
                self.listen_thread.join(timeout=5)
            self.status = "STOPPED"
            logger.info(f"Node {self.node_name} stopped")
        except Exception as e:
            logger.error(f"Error stopping node {self.node_name}: {e}")
    
    def _start_communication(self):
        """Start UDP communication"""
        if self.direct_communication_enabled:
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_socket.bind(('0.0.0.0', self.node_port))
            self.udp_socket.settimeout(1.0)
            
            self.listening = True
            self.listen_thread = threading.Thread(target=self._listen_for_messages)
            self.listen_thread.daemon = True
            self.listen_thread.start()
            
            logger.info(f"Node {self.node_name} listening on port {self.node_port}")
    
    def _listen_for_messages(self):
        """Listen for incoming messages"""
        while self.listening:
            try:
                data, addr = self.udp_socket.recvfrom(4096)
                message_data = json.loads(data.decode('utf-8'))
                logger.info(f"🔧 [{self.node_name}] Received message from {addr}, type: {message_data.get('type')}, source: {message_data.get('source')}, destination: {message_data.get('destination')}")
                message = self._deserialize_message(message_data)
                self._process_message(message, addr)
            except socket.timeout:
                continue
            except (ConnectionResetError, OSError) as e:
                # Windows-specific: UDP can raise ConnectionResetError when remote socket closes
                # This is often benign for UDP (connectionless protocol) - log as debug/warning
                error_code = getattr(e, 'winerror', None) or getattr(e, 'errno', None)
                if error_code == 10054:  # WinError 10054: Connection reset by peer
                    # addr may not be defined if error occurs before recvfrom completes
                    logger.debug(f"UDP connection reset by remote host (Windows behavior, usually benign)")
                else:
                    logger.warning(f"UDP socket error: {e}")
            except Exception as e:
                logger.error(f"❌ [{self.node_name}] Error receiving message: {e}")
                logger.error(f"Error type: {type(e).__name__}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
    
    def send_to_master_core(self, message_type: MessageType, payload: Dict[str, Any], 
                          priority: Priority = Priority.NORMAL, requires_ack: bool = False):
        """Send message to master core"""
        message = NodeMessage(
            message_id=str(uuid.uuid4()),
            type=message_type,
            priority=priority,
            source=self.node_name,
            destination="master_core",
            payload=payload,
            timestamp=time.time(),
            requires_ack=requires_ack
        )
        
        return self._send_message(message, (self.master_core_host, self.master_core_port))
    
    def send_to_node(self, target_node: str, message_type: MessageType, payload: Dict[str, Any],
                    priority: Priority = Priority.NORMAL, requires_ack: bool = False):
        """Send message directly to another node"""
        message = NodeMessage(
            message_id=str(uuid.uuid4()),
            type=message_type,
            priority=priority,
            source=self.node_name,
            destination=target_node,
            payload=payload,
            timestamp=time.time(),
            requires_ack=requires_ack
        )
        
        # Get target node address from config or discovery
        target_addr = self._get_node_address(target_node)
        if target_addr:
            return self._send_message(message, target_addr)
        else:
            logger.error(f"Could not find address for node {target_node}")
            return False
    
    def send_emergency(self, target_nodes: List[str], payload: Dict[str, Any]):
        """Send emergency message to multiple nodes"""
        message = NodeMessage(
            message_id=str(uuid.uuid4()),
            type=MessageType.EMERGENCY,
            priority=Priority.EMERGENCY,
            source=self.node_name,
            destination="multiple",
            payload=payload,
            timestamp=time.time(),
            requires_ack=True
        )
        
        success_count = 0
        for target_node in target_nodes:
            target_addr = self._get_node_address(target_node)
            if target_addr:
                if self._send_message(message, target_addr):
                    success_count += 1
        
        logger.info(f"Emergency message sent to {success_count}/{len(target_nodes)} nodes")
        return success_count > 0
    
    def _send_message(self, message: NodeMessage, addr: tuple) -> bool:
        """Send message to specific address"""
        try:
            message_data = self._serialize_message(message)
            data = json.dumps(message_data).encode('utf-8')
            
            if not self.udp_socket:
                # Create temporary socket for sending
                temp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                temp_socket.sendto(data, addr)
                temp_socket.close()
            else:
                self.udp_socket.sendto(data, addr)
            
            # Track pending acknowledgments
            if message.requires_ack:
                self.pending_acks[message.message_id] = message
            
            logger.debug(f"Message {message.message_id} sent to {addr}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send message {message.message_id}: {e}")
            return False
    
    def _serialize_message(self, message: NodeMessage) -> Dict[str, Any]:
        """Serialize message for transmission"""
        return {
            "message_id": message.message_id,
            "type": message.type.value,
            "priority": message.priority.value,
            "source": message.source,
            "destination": message.destination,
            "payload": message.payload,
            "timestamp": message.timestamp,
            "ttl": message.ttl,
            "requires_ack": message.requires_ack
        }
    
    def _deserialize_message(self, data: Dict[str, Any]) -> NodeMessage:
        """Deserialize received message"""
        try:
            logger.debug(f"DEBUG: _deserialize_message called with data keys: {list(data.keys())}")
            logger.debug(f"DEBUG: _deserialize_message data: {data}")
            
            return NodeMessage(
                message_id=data["message_id"],
                type=MessageType(data["type"]),
                priority=Priority(data["priority"]),
                source=data["source"],
                destination=data["destination"],
                payload=data["payload"],
                timestamp=data["timestamp"],
                ttl=data.get("ttl"),
                requires_ack=data.get("requires_ack", False)
            )
        except KeyError as e:
            logger.error(f"ERROR: Missing key in message data: {e}")
            logger.error(f"ERROR: Available keys: {list(data.keys())}")
            logger.error(f"ERROR: Full data: {data}")
            raise
        except Exception as e:
            logger.error(f"ERROR: Failed to deserialize message: {e}")
            logger.error(f"ERROR: Data: {data}")
            raise
    
    def _process_message(self, message: NodeMessage, addr: tuple):
        """Process incoming message"""
        # Check TTL
        if message.ttl and time.time() > message.ttl:
            logger.warning(f"Message {message.message_id} expired")
            return
        
        # Handle acknowledgments
        if message.type == MessageType.RESPONSE and message.message_id in self.pending_acks:
            self.pending_acks[message.message_id].ack_received = True
            del self.pending_acks[message.message_id]
            return
        
        # Route to appropriate handler
        handler_key = message.type.value
        logger.info(f"🔧 [{self.node_name}] Processing message type: '{handler_key}', source: {message.source}, destination: {message.destination}")
        logger.info(f"🔧 [{self.node_name}] Available handlers: {list(self.message_handlers.keys())}")
        logger.info(f"🔧 [{self.node_name}] Handler exists for '{handler_key}': {handler_key in self.message_handlers}")
        
        if handler_key in self.message_handlers:
            try:
                logger.info(f"🔧 [{self.node_name}] Calling handler for '{handler_key}'")
                self.message_handlers[handler_key](message, addr)
                logger.info(f"✅ [{self.node_name}] Handler '{handler_key}' completed")
            except Exception as e:
                logger.error(f"❌ [{self.node_name}] Error handling {handler_key} message: {e}")
                import traceback
                logger.error(traceback.format_exc())
        else:
            logger.warning(f"⚠️ [{self.node_name}] No handler for message type: '{handler_key}'")
        
        # Send acknowledgment if required
        if message.requires_ack:
            ack_message = NodeMessage(
                message_id=str(uuid.uuid4()),
                type=MessageType.RESPONSE,
                priority=Priority.NORMAL,
                source=self.node_name,
                destination=message.source,
                payload={"ack_for": message.message_id, "status": "received"},
                timestamp=time.time()
            )
            self._send_message(ack_message, addr)
    
    def _handle_heartbeat(self, message: NodeMessage, addr: tuple):
        """Handle heartbeat messages"""
        self.last_heartbeat = time.time()
        logger.debug(f"Heartbeat received from {message.source}")
    
    def _handle_status(self, message: NodeMessage, addr: tuple):
        """Handle status requests"""
        status_response = {
            "node_name": self.node_name,
            "status": self.status,
            "last_heartbeat": self.last_heartbeat,
            "uptime": time.time() - self.last_heartbeat
        }
        
        response = NodeMessage(
            message_id=str(uuid.uuid4()),
            type=MessageType.RESPONSE,
            priority=Priority.NORMAL,
            source=self.node_name,
            destination=message.source,
            payload=status_response,
            timestamp=time.time()
        )
        self._send_message(response, addr)
    
    def _handle_emergency(self, message: NodeMessage, addr: tuple):
        """Handle emergency messages"""
        logger.critical(f"EMERGENCY MESSAGE from {message.source}: {message.payload}")
        # Override in subclasses for specific emergency handling
    
    def _handle_ack(self, message: NodeMessage, addr: tuple):
        """Handle acknowledgment messages"""
        logger.debug(f"Acknowledgment received for message {message.payload.get('ack_for')}")
    
    def _handle_config_update(self, message: NodeMessage, addr: tuple):
        """Handle configuration update from Master Core
        
        Enterprise pattern: Master Core config takes precedence over local config
        """
        logger.info(f"🔧 [{self.node_name}] Received config_update message from {message.source}")
        config_updates = message.payload.get("config", {})
        logger.info(f"🔧 [{self.node_name}] Config update payload: {config_updates}")
        
        if config_updates:
            old_config = dict(self.master_core_config)
            self.master_core_config.update(config_updates)
            logger.info(f"✅ [{self.node_name}] Configuration updated from Master Core")
            logger.info(f"   Old config: {old_config}")
            logger.info(f"   New config: {self.master_core_config}")
            logger.info(f"   Updated keys: {list(config_updates.keys())}")
            
            # Notify subclasses that config was updated (for dynamic reconfiguration)
            if hasattr(self, 'on_config_updated'):
                logger.info(f"🔧 [{self.node_name}] Calling on_config_updated() callback")
                self.on_config_updated(config_updates)
        else:
            logger.warning(f"⚠️ [{self.node_name}] Config update message received but payload.config is empty or missing")
    
    def get_config_value(self, key: str, default: Any = None) -> Any:
        """Get configuration value with hierarchy: Master Core > Local > Default
        
        Enterprise configuration pattern:
        1. Master Core config (authoritative, centralized)
        2. Local node config (override for testing/development)
        3. Default value (fallback)
        
        Args:
            key: Configuration key to retrieve
            default: Default value if not found in any config source
            
        Returns:
            Configuration value from highest priority source
        """
        # Priority 1: Master Core config (authoritative)
        if key in self.master_core_config:
            value = self.master_core_config[key]
            logger.info(f"🔧 [{self.node_name}] get_config_value('{key}') = {value} (from Master Core)")
            return value
        
        # Priority 2: Local node config (override for testing)
        if key in self.config:
            value = self.config[key]
            logger.info(f"🔧 [{self.node_name}] get_config_value('{key}') = {value} (from local config)")
            return value
        
        # Priority 3: Default value
        logger.info(f"🔧 [{self.node_name}] get_config_value('{key}') = {default} (using default)")
        return default
    
    def request_config_from_master(self) -> bool:
        """Request configuration from Master Core
        
        Called on startup or when config needs to be refreshed
        """
        try:
            logger.info(f"🔧 [{self.node_name}] Requesting configuration from Master Core at {self.master_core_host}:{self.master_core_port}")
            payload = {"request_config": True, "node_name": self.node_name}
            result = self.send_to_master_core(MessageType.COMMAND, payload, Priority.NORMAL)
            logger.info(f"🔧 [{self.node_name}] Config request sent, result: {result}")
            return result
        except Exception as e:
            logger.error(f"❌ [{self.node_name}] Failed to request config from Master Core: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _get_node_address(self, node_name: str) -> Optional[tuple]:
        """Get address for a specific node"""
        # This would typically use service discovery or configuration
        node_config = self.config.get("known_nodes", {}).get(node_name)
        if node_config:
            return (node_config["host"], node_config["port"])
        return None
    
    def register_handler(self, message_type: str, handler: Callable):
        """Register custom message handler"""
        self.message_handlers[message_type] = handler
    
    def get_status(self) -> Dict[str, Any]:
        """Get current node status"""
        return {
            "node_name": self.node_name,
            "node_id": self.node_id,
            "status": self.status,
            "last_heartbeat": self.last_heartbeat,
            "pending_messages": len(self.message_queue),
            "pending_acks": len(self.pending_acks)
        }
    
    def send_heartbeat(self):
        """Send heartbeat to master core"""
        heartbeat_data = {
            "node_id": self.node_id,
            "status": self.status,
            "timestamp": time.time()
        }
        return self.send_to_master_core(MessageType.HEARTBEAT, heartbeat_data)
    
    def query_db(self, collection: str, query_filter: Dict[str, Any] = None, 
                 sort: List[tuple] = None, limit: int = 100, skip: int = 0,
                 callback: Optional[Callable] = None) -> bool:
        """Query database via DB Client node
        
        This is a convenience method that sends a query to the DB client node.
        The response will be handled by registered message handlers or the optional callback.
        
        Args:
            collection: Collection name to query
            query_filter: MongoDB filter dictionary (default: {})
            sort: List of (field, direction) tuples for sorting (e.g., [("timestamp", -1)])
            limit: Maximum number of documents to return (default: 100)
            skip: Number of documents to skip (default: 0)
            callback: Optional callback function(message, addr) to handle response
        
        Returns:
            bool: True if message was sent successfully
        
        Example:
            # Query with filter and sort
            node.query_db(
                collection="System",
                query_filter={"status": "ONLINE"},
                sort=[("startuptime", -1)],
                limit=10
            )
            
            # Register handler to process response
            def handle_db_response(message, addr):
                if message.payload.get("status") == "success":
                    results = message.payload.get("query_results", [])
                    print(f"Got {len(results)} documents")
            
            node.register_handler("response", handle_db_response)
        """
        # Get DB client address from config
        db_client_addr = self._get_node_address("db_client")
        if not db_client_addr:
            logger.error("DB client node address not found in configuration")
            return False
        
        # Build query payload
        payload = {
            "command": "query_data",
            "collection": collection,
            "query": query_filter or {},
            "limit": limit,
            "skip": skip
        }
        
        if sort:
            payload["sort"] = sort
        
        # Generate message ID
        message_id = str(uuid.uuid4())
        
        # Store callback if provided (for async response handling)
        if callback:
            # Store callback with message_id for later lookup
            if not hasattr(self, '_db_query_callbacks'):
                self._db_query_callbacks = {}
            self._db_query_callbacks[message_id] = callback
            
            # Register temporary response handler
            original_handler = self.message_handlers.get("response")
            
            def response_handler(msg: NodeMessage, addr: tuple):
                # Check if this response is for our query
                # Note: DB client responds with its own message_id, but we track by checking source and payload
                if msg.source == "db_client" and msg.destination == self.node_name:
                    # Check if this matches our query (by checking if we have a callback waiting)
                    # We'll call callback for any response from db_client if callback exists
                    if hasattr(self, '_db_query_callbacks') and message_id in self._db_query_callbacks:
                        callback(msg, addr)
                        # Remove callback after use
                        self._db_query_callbacks.pop(message_id, None)
                        # Restore original handler if it exists
                        if original_handler:
                            self.message_handlers["response"] = original_handler
                elif original_handler:
                    original_handler(msg, addr)
            
            self.message_handlers["response"] = response_handler
        
        # Send query message to DB client
        message = NodeMessage(
            message_id=message_id,
            type=MessageType.COMMAND,
            priority=Priority.NORMAL,
            source=self.node_name,
            destination="db_client",
            payload=payload,
            timestamp=time.time(),
            requires_ack=False
        )
        
        return self._send_message(message, db_client_addr)

