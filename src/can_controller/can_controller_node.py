#!/usr/bin/env python3
"""
CAN Controller Node - Specialized node for CAN bus communication
Extends BaseNode with CAN-specific functionality
"""

import can
import threading
import time
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
import json
import os
import sys

# Add parent directory to path for base_node import
sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent / "src" / "onboard_core"))

from base_node import BaseNode, MessageType, Priority, NodeMessage

logger = logging.getLogger(__name__)

class CANControllerNode(BaseNode):
    """
    CAN Controller Node with direct communication capabilities
    
    Features:
    - CAN bus communication
    - Direct node-to-node messaging
    - Emergency stop capabilities
    - Real-time data streaming
    - CAN file playback for testing
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("can_controller", config)
        
        # CAN-specific configuration
        self.can_interface = config.get("can_interface", "socketcan")
        self.can_channel = config.get("can_channel", "vcan0")
        self.can_bitrate = config.get("can_bitrate", 250000)
        
        # CAN bus
        self.can_bus = None
        self.can_listener = None
        self.can_thread = None
        self.can_running = False
        
        # Data streaming
        self.data_subscribers = []  # Nodes subscribed to CAN data
        self.emergency_stop_enabled = True
        
        # CAN file playback
        self.playback_enabled = config.get("playback_enabled", True)
        self.playback_thread = None
        self.playback_running = False
        
        # Register CAN-specific handlers
        self.register_handler("can_command", self._handle_can_command)
        self.register_handler("subscribe_data", self._handle_subscribe_data)
        self.register_handler("emergency_stop", self._handle_emergency_stop)
        self.register_handler("play_can_file", self._handle_play_can_file)
        
        logger.info(f"CAN Controller Node initialized for {self.can_interface}:{self.can_channel}")
    
    def start(self):
        """Start the CAN controller node"""
        if not super().start():
            return False
        
        # Start CAN bus
        if self._start_can_bus():
            self.status = "RUNNING"
            logger.info("CAN Controller Node started successfully")
            return True
        else:
            self.status = "ERROR"
            return False
    
    def stop(self):
        """Stop the CAN controller node"""
        self._stop_can_bus()
        self._stop_playback()
        super().stop()
    
    def _start_can_bus(self) -> bool:
        """Start CAN bus communication"""
        try:
            self.can_bus = can.interface.Bus(
                interface=self.can_interface,
                channel=self.can_channel,
                bitrate=self.can_bitrate
            )
            
            # Start CAN message listener
            self.can_running = True
            self.can_thread = threading.Thread(target=self._can_message_loop)
            self.can_thread.daemon = True
            self.can_thread.start()
            
            logger.info(f"CAN bus started on {self.can_interface}:{self.can_channel}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start CAN bus: {e}")
            return False
    
    def _stop_can_bus(self):
        """Stop CAN bus communication"""
        self.can_running = False
        if self.can_thread:
            self.can_thread.join(timeout=5)
        
        if self.can_bus:
            self.can_bus.shutdown()
            logger.info("CAN bus stopped")
    
    def _can_message_loop(self):
        """Main CAN message processing loop"""
        while self.can_running:
            try:
                message = self.can_bus.recv(timeout=1.0)
                if message:
                    self._process_can_message(message)
            except Exception as e:
                logger.error(f"Error processing CAN message: {e}")
    
    def _process_can_message(self, can_message: can.Message):
        """Process incoming CAN message"""
        # Convert CAN message to our format
        message_data = {
            "arbitration_id": can_message.arbitration_id,
            "data": list(can_message.data),
            "timestamp": can_message.timestamp,
            "is_extended_id": can_message.is_extended_id,
            "is_remote_frame": can_message.is_remote_frame
        }
        
        # Send to subscribers
        self._broadcast_to_subscribers(message_data)
        
        # Send to master core
        self.send_to_master_core(
            MessageType.DATA,
            {"can_message": message_data},
            Priority.NORMAL
        )
    
    def _broadcast_to_subscribers(self, data: Dict[str, Any]):
        """Broadcast data to subscribed nodes"""
        for subscriber in self.data_subscribers:
            self.send_to_node(
                subscriber,
                MessageType.DATA,
                {"can_data": data},
                Priority.NORMAL
            )
    
    def _handle_can_command(self, message: NodeMessage, addr: tuple):
        """Handle CAN-specific commands"""
        command = message.payload.get("command")
        
        if command == "start_monitoring":
            self._start_can_bus()
        elif command == "stop_monitoring":
            self._stop_can_bus()
        elif command == "send_message":
            self._send_can_message(message.payload.get("can_data"))
    
    def _handle_subscribe_data(self, message: NodeMessage, addr: tuple):
        """Handle data subscription requests"""
        subscriber = message.payload.get("subscriber")
        if subscriber and subscriber not in self.data_subscribers:
            self.data_subscribers.append(subscriber)
            logger.info(f"Node {subscriber} subscribed to CAN data")
    
    def _handle_emergency_stop(self, message: NodeMessage, addr: tuple):
        """Handle emergency stop commands"""
        logger.critical(f"EMERGENCY STOP from {message.source}")
        
        # Send emergency stop on CAN bus
        if self.emergency_stop_enabled:
            self._send_emergency_stop_can()
        
        # Broadcast emergency to all emergency nodes
        emergency_payload = {
            "type": "emergency_stop",
            "source": message.source,
            "timestamp": time.time()
        }
        
        self.send_emergency(self.emergency_nodes, emergency_payload)
    
    def _handle_play_can_file(self, message: NodeMessage, addr: tuple):
        """Handle CAN file playback requests"""
        file_path = message.payload.get("file_path")
        if file_path and self.playback_enabled:
            self._start_playback(file_path)
    
    def _start_playback(self, file_path: str):
        """Start CAN file playback"""
        if self.playback_running:
            logger.warning("Playback already running")
            return
        
        self.playback_running = True
        self.playback_thread = threading.Thread(
            target=self._playback_worker,
            args=(file_path,)
        )
        self.playback_thread.daemon = True
        self.playback_thread.start()
        logger.info(f"Started CAN file playback: {file_path}")
    
    def _stop_playback(self):
        """Stop CAN file playback"""
        self.playback_running = False
        if self.playback_thread:
            self.playback_thread.join(timeout=5)
        logger.info("CAN file playback stopped")
    
    def _playback_worker(self, file_path: str):
        """Worker thread for CAN file playback"""
        try:
            from can_file_parser import CANFileParser
            
            parser = CANFileParser()
            messages = parser.parse_log_file(file_path)
            
            logger.info(f"Playing back {len(messages)} CAN messages")
            
            for message in messages:
                if not self.playback_running:
                    break
                
                # Send message on CAN bus
                can_msg = can.Message(
                    arbitration_id=message['can_id'],
                    data=message['data'],
                    is_extended_id=False
                )
                
                if self.can_bus:
                    self.can_bus.send(can_msg)
                
                # Wait for next message timing
                time.sleep(0.1)  # Adjust timing as needed
            
            logger.info("CAN file playback completed")
            
        except Exception as e:
            logger.error(f"Error during CAN file playback: {e}")
        finally:
            self.playback_running = False
    
    def _send_can_message(self, can_data: Dict[str, Any]):
        """Send message on CAN bus"""
        try:
            message = can.Message(
                arbitration_id=can_data["arbitration_id"],
                data=can_data["data"],
                is_extended_id=can_data.get("is_extended_id", False)
            )
            
            self.can_bus.send(message)
            logger.debug(f"CAN message sent: {can_data['arbitration_id']}")
            
        except Exception as e:
            logger.error(f"Failed to send CAN message: {e}")
    
    def _send_emergency_stop_can(self):
        """Send emergency stop message on CAN bus"""
        # This would be a specific CAN message for emergency stop
        emergency_message = can.Message(
            arbitration_id=0x1FF,  # Emergency stop PGN
            data=[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            is_extended_id=False
        )
        
        try:
            self.can_bus.send(emergency_message)
            logger.critical("Emergency stop sent on CAN bus")
        except Exception as e:
            logger.error(f"Failed to send emergency stop on CAN: {e}")
    
    def get_can_status(self) -> Dict[str, Any]:
        """Get CAN-specific status"""
        base_status = self.get_status()
        base_status.update({
            "can_interface": self.can_interface,
            "can_channel": self.can_channel,
            "can_running": self.can_running,
            "subscribers": len(self.data_subscribers),
            "emergency_stop_enabled": self.emergency_stop_enabled,
            "playback_running": self.playback_running
        })
        return base_status

def main():
    """Main entry point for CAN Controller Node"""
    import argparse
    
    parser = argparse.ArgumentParser(description="CAN Controller Node")
    parser.add_argument("--config", default="config.json", help="Configuration file")
    parser.add_argument("--daemon", action="store_true", help="Run as daemon")
    
    args = parser.parse_args()
    
    # Load configuration
    config_path = Path(args.config)
    if config_path.exists():
        with open(config_path, 'r') as f:
            config = json.load(f)
    else:
        # Default configuration
        config = {
            "can_interface": "socketcan",
            "can_channel": "vcan0",
            "can_bitrate": 250000,
            "node_port": 14551,
            "master_core_host": "localhost",
            "master_core_port": 14550,
            "direct_communication": True,
            "emergency_nodes": ["engine", "steering", "autopilot"],
            "playback_enabled": True
        }
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and start node
    node = CANControllerNode(config)
    
    if args.daemon:
        # Run as daemon
        import daemon
        from daemon.pidfile import PIDLockFile
        
        pidfile = PIDLockFile('/tmp/can_controller_node.pid')
        
        with daemon.DaemonContext(pidfile=pidfile):
            if node.start():
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    pass
            node.stop()
    else:
        # Run in foreground
        if node.start():
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass
        node.stop()

if __name__ == "__main__":
    main()
