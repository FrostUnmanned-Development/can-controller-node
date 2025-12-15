#!/usr/bin/env python3
"""
CAN Controller Node - Specialized node for CAN bus communication
Extends BaseNode with CAN-specific functionality including NMEA2000 PGN parsing
"""

import can
import threading
import time
import logging
from datetime import datetime, timedelta
import struct
from pathlib import Path
from typing import Dict, Any, Optional, List
import json
import argparse
import os
import sys
import platform
import tempfile
import signal
import uuid

# Import BaseNode from local module (with fallback for direct execution)
try:
    from .base_node import BaseNode, MessageType, Priority, NodeMessage
except ImportError:
    # Fallback for direct execution (not as package)
    sys.path.insert(0, str(Path(__file__).parent))
    from base_node import BaseNode, MessageType, Priority, NodeMessage
from nmea2000.decoder import NMEA2000Decoder

# Import constants for data categorization
sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent / "src" / "data_storage"))
from constants import DataCategories

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
        
        # NMEA2000 Decoder
        self.decoder = NMEA2000Decoder()
        
        # Data processing
        self.data_subscribers = []  # Nodes subscribed to CAN data
        self.emergency_stop_enabled = True
        # Use get_config_value() for enterprise config hierarchy (Master Core > Local > Default)
        self.data_ttl_days = self.get_config_value("data_ttl_days", 7)
        
        # CAN file playback
        self.playback_enabled = config.get("playback_enabled", True)
        self.playback_thread = None
        self.playback_running = False
        
        # Background heartbeat task
        self.heartbeat_task_running = False
        self.heartbeat_thread = None
        
        # Register CAN-specific handlers
        # Note: BaseNode routes by message type, so we register "command" handler
        # that dispatches based on payload.command
        self.register_handler("command", self._handle_can_command)
        self.register_handler("subscribe_data", self._handle_subscribe_data)
        self.register_handler("emergency_stop", self._handle_emergency_stop)
        self.register_handler("play_can_file", self._handle_play_can_file)
        
        logger.info(f"CAN Controller Node initialized for {self.can_interface}:{self.can_channel}")
    
    def on_config_updated(self, config_updates: Dict[str, Any]):
        """Handle configuration updates from Master Core
        
        Called automatically when Master Core sends config updates.
        Updates TTL settings dynamically and CAN interface settings.
        """
        if "data_ttl_days" in config_updates:
            old_ttl = self.data_ttl_days
            self.data_ttl_days = self.get_config_value("data_ttl_days", 7)
            logger.info(f"TTL configuration updated: {old_ttl} -> {self.data_ttl_days} days")
        
        # Handle CAN interface/channel/bitrate updates
        can_config_changed = False
        if "can_interface" in config_updates:
            old_interface = self.can_interface
            self.can_interface = config_updates["can_interface"]
            logger.info(f"CAN interface configuration updated: {old_interface} -> {self.can_interface}")
            can_config_changed = True
        
        if "can_channel" in config_updates:
            old_channel = self.can_channel
            self.can_channel = config_updates["can_channel"]
            logger.info(f"CAN channel configuration updated: {old_channel} -> {self.can_channel}")
            can_config_changed = True
        
        if "can_bitrate" in config_updates:
            old_bitrate = self.can_bitrate
            self.can_bitrate = config_updates["can_bitrate"]
            logger.info(f"CAN bitrate configuration updated: {old_bitrate} -> {self.can_bitrate}")
            can_config_changed = True
        
        # Restart CAN bus if interface/channel/bitrate changed
        if can_config_changed:
            logger.info("🔄 [can_controller] Restarting CAN bus with new configuration...")
            self._stop_can_bus()
            time.sleep(0.5)  # Brief delay to ensure clean shutdown
            if self._start_can_bus():
                logger.info("✅ [can_controller] CAN bus restarted successfully with new configuration")
            else:
                logger.error("❌ [can_controller] Failed to restart CAN bus with new configuration")
    
    def start(self):
        """Start the CAN controller node"""
        if not super().start():
            return False
        
        logger.info(f"🔧 [can_controller] Starting configuration setup...")
        logger.info(f"🔧 [can_controller] Current master_core_config: {self.master_core_config}")
        logger.info(f"🔧 [can_controller] Current local config data_ttl_days: {self.config.get('data_ttl_days', 'NOT SET')}")
        
        # Request configuration from Master Core (enterprise pattern)
        logger.info(f"🔧 [can_controller] Requesting config from Master Core...")
        self.request_config_from_master()
        
        # Give Master Core a moment to respond (config is async)
        time.sleep(0.5)  # Small delay for config response
        
        # Update data_ttl_days from config hierarchy
        logger.info(f"🔧 [can_controller] Getting data_ttl_days from config hierarchy...")
        logger.info(f"🔧 [can_controller] master_core_config after request: {self.master_core_config}")
        self.data_ttl_days = self.get_config_value("data_ttl_days", 7)
        
        config_source = "Master Core" if "data_ttl_days" in self.master_core_config else "local config"
        logger.info(f"✅ [can_controller] CAN Controller Node using TTL: {self.data_ttl_days} days (from {config_source})")
        logger.info(f"🔧 [can_controller] TTL in seconds: {self.data_ttl_days * 86400}")
        
        # Start CAN bus
        if self._start_can_bus():
            # Start background heartbeat task
            self._start_heartbeat_task()
            self.status = "RUNNING"
            logger.info("✅ [can_controller] CAN Controller Node started successfully")
            return True
        else:
            self.status = "ERROR"
            logger.error("❌ [can_controller] Failed to start CAN bus")
            return False
    
    def stop(self):
        """Stop the CAN controller node"""
        self._stop_heartbeat_task()
        self._stop_can_bus()
        self._stop_playback()
        super().stop()
    
    def run_daemon(self):
        """Main daemon loop for CAN controller"""
        logger.info("CAN Controller Daemon started successfully")
        try:
            while self.listening:
                try:
                    # Listen for CAN messages
                    message = self.can_bus.recv(timeout=1.0)
                    if message:
                        self._process_can_message(message)
                except Exception as e:
                    logger.error(f"Error receiving CAN message: {e}")
                    time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutdown signal received")
        finally:
            self.stop()
            logger.info("CAN Controller Daemon stopped")
    
    def _detect_available_can_interfaces(self) -> List[Dict[str, str]]:
        """Auto-detect available CAN interfaces (last resort fallback)
        
        Reads interface options from config file (can_interface_1-4) and tests each one.
        Falls back to platform-specific defaults if config interfaces fail.
        
        Returns list of detected interfaces with format:
        [{"interface": "kvaser", "channel": "0"}, ...]
        """
        import platform
        detected = []
        
        # First, try interfaces from config file (can_interface_1-4)
        config_interfaces = []
        for i in range(1, 5):  # Check can_interface_1 through can_interface_4
            interface_key = f"can_interface_{i}"
            channel_key = f"can_channel_{i}"
            interface = self.get_config_value(interface_key)
            channel = self.get_config_value(channel_key, "0")
            
            if interface:
                config_interfaces.append({"interface": interface, "channel": channel})
        
        # Also check primary can_interface/can_channel if present
        primary_interface = self.get_config_value("can_interface")
        primary_channel = self.get_config_value("can_channel", "0")
        if primary_interface:
            config_interfaces.insert(0, {"interface": primary_interface, "channel": primary_channel})
        
        # Fallback to platform-specific defaults if no config interfaces found
        if not config_interfaces:
            if platform.system() == 'Windows':
                config_interfaces = [
                    {"interface": "kvaser", "channel": "0"},
                    {"interface": "pcan", "channel": "0"},
                    {"interface": "vector", "channel": "0"},
                    {"interface": "slcan", "channel": "0"},
                    {"interface": "usb2can", "channel": "0"}
                ]
            else:  # Linux
                config_interfaces = [
                    {"interface": "socketcan", "channel": "vcan0"},
                    {"interface": "slcan", "channel": "0"}
                ]
        
        logger.info(f"🔍 [can_controller] Auto-detecting available CAN interfaces from config...")
        logger.info(f"   Config interfaces to test: {[f\"{d['interface']}:{d['channel']}\" for d in config_interfaces]}")
        
        for interface_config in config_interfaces:
            interface = interface_config["interface"]
            channel = interface_config["channel"]
            try:
                # Try to create a bus instance to test if interface is available
                test_bus = can.interface.Bus(interface=interface, channel=channel, bitrate=250000, receive_own_messages=False)
                test_bus.shutdown()
                detected.append({"interface": interface, "channel": channel})
                logger.info(f"   ✅ Detected: {interface} (channel: {channel})")
            except Exception as e:
                # Interface not available or error - skip silently
                logger.debug(f"   ⏭️  Skipped {interface}:{channel} (not available)")
        
        if detected:
            logger.info(f"✅ [can_controller] Auto-detected {len(detected)} CAN interface(s): {[d['interface'] for d in detected]}")
        else:
            logger.warning(f"⚠️ [can_controller] No CAN interfaces auto-detected from config or defaults")
        
        return detected
    
    def _start_can_bus(self) -> bool:
        """Start CAN bus communication"""
        import platform
        import sys
        
        try:
            logger.info(f"🔧 [can_controller] Attempting to start CAN bus on {self.can_interface}:{self.can_channel} at {self.can_bitrate} bps")
            
            # Check if socketcan is being used on Windows (not supported)
            if platform.system() == 'Windows' and self.can_interface == 'socketcan':
                logger.error(f"❌ [can_controller] SocketCAN is Linux-only and not available on Windows")
                logger.info(f"🔍 [can_controller] Attempting auto-detection of Windows CAN interfaces...")
                
                # Try auto-detection as last resort
                detected = self._detect_available_can_interfaces()
                if detected:
                    # Use first detected interface
                    self.can_interface = detected[0]["interface"]
                    self.can_channel = detected[0]["channel"]
                    logger.info(f"✅ [can_controller] Auto-configured to use: {self.can_interface}:{self.can_channel}")
                else:
                    logger.error(f"❌ [can_controller] Please configure a Windows-compatible CAN interface:")
                    logger.error(f"   - 'pcan' (PEAK CAN)")
                    logger.error(f"   - 'vector' (Vector CAN)")
                    logger.error(f"   - 'kvaser' (Kvaser CAN)")
                    logger.error(f"   - 'slcan' (Serial CAN)")
                    logger.error(f"   - 'usb2can' (USB2CAN)")
                    logger.warning(f"⚠️ [can_controller] CAN bus initialization failed. Node will continue but CAN functionality will be unavailable.")
                    return False
            
            self.can_bus = can.interface.Bus(
                interface=self.can_interface,
                channel=self.can_channel,
                bitrate=self.can_bitrate
            )
            logger.info(f"✅ [can_controller] CAN bus initialized successfully: {self.can_bus}")
            
            # Start CAN message listener
            self.can_running = True
            self.can_thread = threading.Thread(target=self._can_message_loop)
            self.can_thread.daemon = True
            self.can_thread.start()
            logger.info(f"✅ [can_controller] CAN message listener thread started")
            
            logger.info(f"✅ [can_controller] CAN bus started successfully on {self.can_interface}:{self.can_channel}")
            return True
            
        except OSError as e:
            error_code = getattr(e, 'winerror', None) or getattr(e, 'errno', None)
            if error_code == 10047:  # WinError 10047: Address incompatible with protocol
                logger.error(f"❌ [can_controller] CAN interface '{self.can_interface}' is not compatible with Windows")
                if platform.system() == 'Windows':
                    logger.error(f"❌ [can_controller] SocketCAN (Linux) cannot be used on Windows")
                    logger.error(f"❌ [can_controller] Please configure a Windows-compatible interface in config.json:")
                    logger.error(f"   - 'pcan' for PEAK CAN adapters")
                    logger.error(f"   - 'vector' for Vector CAN interfaces")
                    logger.error(f"   - 'kvaser' for Kvaser CAN interfaces")
            else:
                logger.error(f"❌ [can_controller] Failed to start CAN bus: {e}")
            logger.error(f"❌ [can_controller] Error type: {type(e).__name__}, Code: {error_code}")
            logger.warning(f"⚠️ [can_controller] CAN bus initialization failed. Node will continue but CAN functionality will be unavailable.")
            return False
        except Exception as e:
            logger.error(f"❌ [can_controller] Failed to start CAN bus: {e}")
            logger.error(f"❌ [can_controller] Error type: {type(e).__name__}")
            import traceback
            logger.error(f"❌ [can_controller] Traceback: {traceback.format_exc()}")
            logger.error(f"❌ [can_controller] CAN interface: {self.can_interface}, channel: {self.can_channel}, bitrate: {self.can_bitrate}")
            logger.warning(f"⚠️ [can_controller] CAN bus initialization failed. Node will continue but CAN functionality will be unavailable.")
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
        """Process incoming CAN message with NMEA2000 decoding"""
        try:
            # Extract CAN ID components
            received_pgn = (can_message.arbitration_id >> 8) & 0x3FFFF
            
            # Decode pgn_id, source_id, dest, priority using NMEA2000 decoder
            can_id_29bits_decoded = self.decoder._extract_header(can_message.arbitration_id)
            
            pgn_id = can_id_29bits_decoded[0]  # PGN as integer
            source_id = can_id_29bits_decoded[1]  # Source as integer
            dest = can_id_29bits_decoded[2]  # Destination as integer
            priority = can_id_29bits_decoded[3]  # Priority as integer
            length = str(can_message.dlc)
            
            logger.info(f"Received message with PGN: HEX - {hex(received_pgn)} DEC - {received_pgn}, CAN ID: {hex(can_message.arbitration_id)}, data: {list(can_message.data)}")
            
            # Format timestamp
            time_of_message = datetime.now().strftime("%Y-%m-%d-%H:%M:%S.%f")
            
            # Format payload data to hex string
            payload_data_string = ""
            counter = 0
            for item in can_message.data:
                if counter == 0:
                    payload_data_string = payload_data_string + format(item, '02x')
                    counter += 1
                else:
                    payload_data_string = payload_data_string + "," + format(item, '02x')
            
            # Create N2K ASCII formatted frame
            formated_frame = (time_of_message + "," + str(priority) + "," + str(pgn_id) + "," + str(source_id) + "," + str(dest) + "," + length + "," + payload_data_string)
            logger.info(f"N2K ASCII Version: {formated_frame}")
            
            # Decode the frame using NMEA2000 decoder
            decoded_data = self.decoder.decode_basic_string(formated_frame)
            
            if decoded_data:
                # Categorize the data
                category = self._categorize_data(decoded_data)
                
                # Send to database via Master Core -> DB Client
                self._send_parsed_data_to_db(decoded_data, category)
                
                # Broadcast raw CAN data to subscribers (for real-time monitoring)
                message_data = {
                    "arbitration_id": can_message.arbitration_id,
                    "data": list(can_message.data),
                    "timestamp": can_message.timestamp,
                    "is_extended_id": can_message.is_extended_id,
                    "is_remote_frame": can_message.is_remote_frame,
                    "decoded": True,
                    "pgn": decoded_data.PGN,
                    "category": category.value if hasattr(category, 'value') else str(category)
                }
                self._broadcast_to_subscribers(message_data)
                
                # Send to master core
                self.send_to_master_core(
                    MessageType.DATA,
                    {"can_message": message_data, "parsed_data": self._extract_data_fields(decoded_data, category)},
                    Priority.NORMAL
                )
            else:
                # Broadcast raw CAN data even if not decoded
                message_data = {
                    "arbitration_id": can_message.arbitration_id,
                    "data": list(can_message.data),
                    "timestamp": can_message.timestamp,
                    "is_extended_id": can_message.is_extended_id,
                    "is_remote_frame": can_message.is_remote_frame,
                    "decoded": False
                }
                self._broadcast_to_subscribers(message_data)
                
                # Send to master core
                self.send_to_master_core(
                    MessageType.DATA,
                    {"can_message": message_data},
                    Priority.NORMAL
                )
                
        except Exception as e:
            logger.error(f"Error processing CAN message: {e}")
            # Still broadcast raw data for debugging
            message_data = {
                "arbitration_id": can_message.arbitration_id,
                "data": list(can_message.data),
                "timestamp": can_message.timestamp,
                "is_extended_id": can_message.is_extended_id,
                "is_remote_frame": can_message.is_remote_frame,
                "error": str(e)
            }
            self._broadcast_to_subscribers(message_data)
    
    def _broadcast_to_subscribers(self, data: Dict[str, Any]):
        """Broadcast data to subscribed nodes"""
        for subscriber in self.data_subscribers:
            self.send_to_node(
                subscriber,
                MessageType.DATA,
                {"can_data": data},
                Priority.NORMAL
            )
    
    def _categorize_data(self, data):
        """Categorize decoded NMEA2000 data based on PGN"""
        if data.PGN == 126993:  # 1F011 - "Heartbeat"
            return DataCategories.HEARTBEAT
        elif data.PGN == 127488:  # 1F200 - "Engine Parameters, Rapid Update"
            return DataCategories.ENGINE
        elif data.PGN == 127505:  # 1F211 - "Fluid Level"
            return DataCategories.FUEL
        elif data.PGN == 127250:  # 1F112 - "Vessel Heading"
            return DataCategories.NAVIGATION
        elif data.PGN == 127257:  # 1F119 - "Attitude"
            return DataCategories.NAVIGATION
        elif data.PGN == 129026:  # 1F802 - "COG & SOG, Rapid Update"
            return DataCategories.NAVIGATION
        elif data.PGN == 129025:  # 1F801 - "Position, Rapid Update"
            return DataCategories.NAVIGATION
        elif data.PGN == 129540:  # 1FA04 - "GNSS Sats in View"
            return DataCategories.NAVIGATION
        elif data.PGN == 129029:  # 1F805 - "GNSS Position Data"
            return DataCategories.NAVIGATION
        elif data.PGN == 126992:  # 1F010 - "System Time"
            return DataCategories.NAVIGATION
        elif data.PGN == 129539:  # 1FA03 - "GNSS DOPs"
            return DataCategories.NAVIGATION
        elif data.PGN == 127258:  # 1F11A - "Magnetic Variation"
            return DataCategories.NAVIGATION
        elif data.PGN == 127489:  # 1F201 - "Engine Parameters, Dynamic"
            return DataCategories.ENGINE
        elif data.PGN == 127497:  # 1F209 - "Trip Fuel Consumption, Engine"
            return DataCategories.ENGINE
        elif data.PGN == 127751:  # 1F307 - "DC Voltage / Current"
            return DataCategories.ENERGYDISTRIBUTION
        elif data.PGN == 127500:  # 1F20C - "Load Controller Connection State / Control"
            return DataCategories.ENERGYDISTRIBUTION
        elif data.PGN == 127501:  # 1F20D - "Switch Bank Status"
            return DataCategories.ENERGYDISTRIBUTION
        elif data.PGN == 127245:  # 1F10D - "Rudder"
            return DataCategories.STEERING
        elif data.PGN == 127508:  # 1F214 - "Battery Status"
            return DataCategories.BATTERY
        elif data.PGN == 127506:  # 1F212 - "DC Detailed Status"
            return DataCategories.BATTERY
        elif data.PGN == 129283:  # 1F903 - "Cross Track Error"
            return DataCategories.NAVIGATION
        elif data.PGN == 129284:  # 1F904 - "Navigation Data"
            return DataCategories.NAVIGATION
        elif data.PGN == 65361:  # FF51 - "Raymarine Alarm"
            return DataCategories.PRODUCT
        elif data.PGN == 60928:  # EE00 - "ISO Address Claim"
            return DataCategories.PRODUCT
        elif data.PGN == 59392:  # E800 - "ISO Acknowledge"
            return DataCategories.PRODUCT
        else:
            return DataCategories.UNKNOWN
    
    def _generate_field_title(self, fields):
        """Generate concatenated title from field IDs - reusable function"""
        title = []
        for item in fields: 
            title.append(item.id)
        return "-".join(title)
    
    def _extract_data_fields(self, data, category):
        """Extract specific fields from decoded data based on category and PGN - matching original implementation"""
        def _convert_value(value):
            """Convert value to JSON-serializable format (handle bytes)"""
            if isinstance(value, bytes):
                return list(value)
            return value
        
        try:
            if category == DataCategories.HEARTBEAT and data.PGN == 126993:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "data_transmit_offset": _convert_value(data.fields[0].value) if len(data.fields) > 0 else None,
                    "unit_data_transmit_offset": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "sequence_counter": _convert_value(data.fields[1].value) if len(data.fields) > 1 else None,
                    "unit_sequence_counter": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "controller1_state": _convert_value(data.fields[2].raw_value) if len(data.fields) > 2 else None,
                    "unit_controller1_state": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "controller2_state": _convert_value(data.fields[3].raw_value) if len(data.fields) > 3 else None,
                    "unit_controller2_state": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "equipment_status": _convert_value(data.fields[4].raw_value) if len(data.fields) > 4 else None,
                    "unit_equipment_status": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "reserved_30": _convert_value(data.fields[5].value) if len(data.fields) > 5 else None,
                    "unit_reserved_30": data.fields[5].unit_of_measurement if len(data.fields) > 5 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.FUEL and data.PGN == 127505:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "instance": data.fields[0].value if len(data.fields) > 0 else None,
                    "unit": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "type": data.fields[1].value if len(data.fields) > 1 else None,
                    "unit_type": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "level": data.fields[2].value if len(data.fields) > 2 else None,
                    "unit_level": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "capacity": data.fields[3].value if len(data.fields) > 3 else None,
                    "unit_capacity": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "reserved_56": data.fields[4].value if len(data.fields) > 4 else None,
                    "unit_reserved_56": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.NAVIGATION and data.PGN == 127250:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "sid": data.fields[0].value if len(data.fields) > 0 else None,
                    "unit": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "heading": data.fields[1].value if len(data.fields) > 1 else None,
                    "unit_heading": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "deviation": data.fields[2].value if len(data.fields) > 2 else None,
                    "unit_deviation": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "variation": data.fields[3].value if len(data.fields) > 3 else None,
                    "unit_variation": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "reference": data.fields[4].value if len(data.fields) > 4 else None,
                    "unit_reference": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "reserved_58": data.fields[5].value if len(data.fields) > 5 else None,
                    "unit_reserved_58": data.fields[5].unit_of_measurement if len(data.fields) > 5 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.NAVIGATION and data.PGN == 127257:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "sid": data.fields[0].value if len(data.fields) > 0 else None,
                    "unit": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "yaw": data.fields[1].value if len(data.fields) > 1 else None,
                    "unit_yaw": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "pitch": data.fields[2].value if len(data.fields) > 2 else None,
                    "unit_pitch": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "roll": data.fields[3].value if len(data.fields) > 3 else None,
                    "unit_roll": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "reserved_56": data.fields[4].value if len(data.fields) > 4 else None,
                    "unit_reserved_56": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.NAVIGATION and data.PGN == 129026:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "sid": data.fields[0].value if len(data.fields) > 0 else None,
                    "unit": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "cog_reference": data.fields[1].value if len(data.fields) > 1 else None,
                    "unit_cog_reference": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "reserved_10": data.fields[2].value if len(data.fields) > 2 else None,
                    "unit_reserved_10": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "cog": data.fields[3].value if len(data.fields) > 3 else None,
                    "unit_cog": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "sog": data.fields[4].value if len(data.fields) > 4 else None,
                    "unit_sog": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "reserved_48": data.fields[5].value if len(data.fields) > 5 else None,
                    "unit_reserved_48": data.fields[5].unit_of_measurement if len(data.fields) > 5 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.NAVIGATION and data.PGN == 129025:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "latitude": data.fields[0].value if len(data.fields) > 0 else None,
                    "unit_latitude": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "longitude": data.fields[1].value if len(data.fields) > 1 else None,
                    "unit_longitude": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.NAVIGATION and data.PGN == 129029:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "sid": data.fields[0].value if len(data.fields) > 0 else None,
                    "unit": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "date": data.fields[1].value.strftime('%Y/%m/%d') if len(data.fields) > 1 and hasattr(data.fields[1].value, 'strftime') else data.fields[1].value if len(data.fields) > 1 else None,
                    "unit_date": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "time": data.fields[2].value.strftime('%H:%M:%S') if len(data.fields) > 2 and hasattr(data.fields[2].value, 'strftime') else data.fields[2].value if len(data.fields) > 2 else None,
                    "unit_time": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "latitude": data.fields[3].value if len(data.fields) > 3 else None,
                    "unit_latitude": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "longitude": data.fields[4].value if len(data.fields) > 4 else None,
                    "unit_longitude": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "altitude": data.fields[5].value if len(data.fields) > 5 else None,
                    "unit_altitude": data.fields[5].unit_of_measurement if len(data.fields) > 5 else None,
                    "gnss_type": data.fields[6].value if len(data.fields) > 6 else None,
                    "unit_gnss_type": data.fields[6].unit_of_measurement if len(data.fields) > 6 else None,
                    "method": data.fields[7].value if len(data.fields) > 7 else None,
                    "unit_method": data.fields[7].unit_of_measurement if len(data.fields) > 7 else None,
                    "integrity": data.fields[8].value if len(data.fields) > 8 else None,
                    "unit_integrity": data.fields[8].unit_of_measurement if len(data.fields) > 8 else None,
                    "reserved_258": data.fields[9].value if len(data.fields) > 9 else None,
                    "unit_reserved_258": data.fields[9].unit_of_measurement if len(data.fields) > 9 else None,
                    "number_of_svs": data.fields[10].value if len(data.fields) > 10 else None,
                    "unit_number_of_svs": data.fields[10].unit_of_measurement if len(data.fields) > 10 else None,
                    "hdop": data.fields[11].value if len(data.fields) > 11 else None,
                    "unit_hdop": data.fields[11].unit_of_measurement if len(data.fields) > 11 else None,
                    "pdop": data.fields[12].value if len(data.fields) > 12 else None,
                    "unit_pdop": data.fields[12].unit_of_measurement if len(data.fields) > 12 else None,
                    "geoidal_separation": data.fields[13].value if len(data.fields) > 13 else None,
                    "unit_geoidal_separation": data.fields[13].unit_of_measurement if len(data.fields) > 13 else None,
                    "reference_stations": data.fields[14].value if len(data.fields) > 14 else None,
                    "unit_reference_stations": data.fields[14].unit_of_measurement if len(data.fields) > 14 else None,
                    "reference_station_type": data.fields[15].value if len(data.fields) > 15 else None,
                    "unit_reference_station_type": data.fields[15].unit_of_measurement if len(data.fields) > 15 else None,
                    "reference_station_id": data.fields[16].value if len(data.fields) > 16 else None,
                    "unit_reference_station_id": data.fields[16].unit_of_measurement if len(data.fields) > 16 else None,
                    "age_of_dgnss_corrections": data.fields[17].value if len(data.fields) > 17 else None,
                    "unit_age_of_dgnss_corrections": data.fields[17].unit_of_measurement if len(data.fields) > 17 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.NAVIGATION and data.PGN == 129540:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "sid": data.fields[0].value if len(data.fields) > 0 else None,
                    "unit": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "range_residual_mode": data.fields[1].value if len(data.fields) > 1 else None,
                    "unit_range_residual_mode": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "reserved_10": data.fields[2].value if len(data.fields) > 2 else None,
                    "unit_reserved_10": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "sats_in_view": data.fields[3].value if len(data.fields) > 3 else None,
                    "unit_sats_in_view": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "prn": data.fields[4].value if len(data.fields) > 4 else None,
                    "unit_prn": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "elevation": data.fields[5].value if len(data.fields) > 5 else None,
                    "unit_elevation": data.fields[5].unit_of_measurement if len(data.fields) > 5 else None,
                    "azimuth": data.fields[6].value if len(data.fields) > 6 else None,
                    "unit_azimuth": data.fields[6].unit_of_measurement if len(data.fields) > 6 else None,
                    "snr": data.fields[7].value if len(data.fields) > 7 else None,
                    "unit_snr": data.fields[7].unit_of_measurement if len(data.fields) > 7 else None,
                    "range_residuals": data.fields[8].value if len(data.fields) > 8 else None,
                    "unit_range_residuals": data.fields[8].unit_of_measurement if len(data.fields) > 8 else None,
                    "status": data.fields[9].value if len(data.fields) > 9 else None,
                    "unit_status": data.fields[9].unit_of_measurement if len(data.fields) > 9 else None,
                    "reserved_116": data.fields[10].value if len(data.fields) > 10 else None,
                    "unit_reserved_116": data.fields[10].unit_of_measurement if len(data.fields) > 10 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.NAVIGATION and data.PGN == 126992:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "sid": data.fields[0].value if len(data.fields) > 0 else None,
                    "unit": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "measure_source": data.fields[1].value if len(data.fields) > 1 else None,
                    "unit_measure_source": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "reserved_12": data.fields[2].value if len(data.fields) > 2 else None,
                    "unit_reserved_12": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "date": data.fields[3].value.strftime('%Y/%m/%d') if len(data.fields) > 3 and hasattr(data.fields[3].value, 'strftime') else data.fields[3].value if len(data.fields) > 3 else None,
                    "unit_date": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "time": data.fields[4].value.strftime('%H:%M:%S') if len(data.fields) > 4 and hasattr(data.fields[4].value, 'strftime') else data.fields[4].value if len(data.fields) > 4 else None,
                    "unit_time": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.NAVIGATION and data.PGN == 129283:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "sid": data.fields[0].value if len(data.fields) > 0 else None,
                    "unit_sid": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "xte_mode": _convert_value(data.fields[1].raw_value) if len(data.fields) > 1 else None,
                    "unit_xte_mode": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "reserved_12": _convert_value(data.fields[2].value) if len(data.fields) > 2 else None,
                    "unit_reserved_12": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "navigation_terminated": _convert_value(data.fields[3].raw_value) if len(data.fields) > 3 else None,
                    "unit_navigation_terminated": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "xte": data.fields[4].value if len(data.fields) > 4 else None,
                    "unit_xte": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "reserved_48": data.fields[5].value if len(data.fields) > 5 else None,
                    "unit_reserved_48": data.fields[5].unit_of_measurement if len(data.fields) > 5 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.NAVIGATION and data.PGN == 129284:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "sid": data.fields[0].value if len(data.fields) > 0 else None,
                    "unit_sid": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "distance_to_waypoint": data.fields[1].value if len(data.fields) > 1 else None,
                    "unit_distance_to_waypoint": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "course_bearing_reference": _convert_value(data.fields[2].raw_value) if len(data.fields) > 2 else None,
                    "unit_course_bearing_reference": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "perpendicular_crossed": _convert_value(data.fields[3].raw_value) if len(data.fields) > 3 else None,
                    "unit_perpendicular_crossed": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "arrival_circle_entered": _convert_value(data.fields[4].raw_value) if len(data.fields) > 4 else None,
                    "unit_arrival_circle_entered": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "calculation_type": _convert_value(data.fields[5].raw_value) if len(data.fields) > 5 else None,
                    "unit_calculation_type": data.fields[5].unit_of_measurement if len(data.fields) > 5 else None,
                    "eta_time": data.fields[6].value if len(data.fields) > 6 else None,
                    "unit_eta_time": data.fields[6].unit_of_measurement if len(data.fields) > 6 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.ENGINE and data.PGN == 127488:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "instance": data.fields[0].value if len(data.fields) > 0 else None,
                    "unit": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "speed": data.fields[1].value if len(data.fields) > 1 else None,
                    "unit_speed": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "boost_pressure": data.fields[2].value if len(data.fields) > 2 else None,
                    "unit_boost_pressure": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "tilt_trim": data.fields[3].value if len(data.fields) > 3 else None,
                    "unit_tilt_trim": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "reserved_48": data.fields[4].value if len(data.fields) > 4 else None,
                    "unit_reserved_48": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.ENERGYDISTRIBUTION and data.PGN == 127751:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "sid": data.fields[0].value if len(data.fields) > 0 else None,
                    "unit": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "connection_number": data.fields[1].value if len(data.fields) > 1 else None,
                    "unit_connection_number": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "dc_voltage": data.fields[2].value if len(data.fields) > 2 else None,
                    "unit_dc_voltage": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "dc_current": data.fields[3].value if len(data.fields) > 3 else None,
                    "unit_dc_current": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "reserved_56": data.fields[4].value if len(data.fields) > 4 else None,
                    "unit_reserved_56": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.PRODUCT and data.PGN == 65361:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "manufacturer_code": _convert_value(data.fields[0].raw_value) if len(data.fields) > 0 else None,
                    "unit_manufacturer_code": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "reserved_11": _convert_value(data.fields[1].value) if len(data.fields) > 1 else None,
                    "unit_reserved_11": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "industry_code": _convert_value(data.fields[2].raw_value) if len(data.fields) > 2 else None,
                    "unit_industry_code": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "alarm_id": _convert_value(data.fields[3].raw_value) if len(data.fields) > 3 else None,
                    "unit_alarm_id": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "alarm_group": _convert_value(data.fields[4].raw_value) if len(data.fields) > 4 else None,
                    "unit_alarm_group": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "reserved_32": data.fields[5].value if len(data.fields) > 5 else None,
                    "unit_reserved_32": data.fields[5].unit_of_measurement if len(data.fields) > 5 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.PRODUCT and data.PGN == 60928:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "unique_number": data.fields[0].value if len(data.fields) > 0 else None,
                    "unit_unique_number": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "manufacturer_code": _convert_value(data.fields[1].raw_value) if len(data.fields) > 1 else None,
                    "unit_manufacturer_code": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "device_instance_lower": _convert_value(data.fields[2].value) if len(data.fields) > 2 else None,
                    "unit_device_instance_lower": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "device_instance_upper": _convert_value(data.fields[3].value) if len(data.fields) > 3 else None,
                    "unit_device_instance_upper": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "device_function": _convert_value(data.fields[4].raw_value) if len(data.fields) > 4 else None,
                    "unit_device_function": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "spare": _convert_value(data.fields[5].value) if len(data.fields) > 5 else None,
                    "unit_spare": data.fields[5].unit_of_measurement if len(data.fields) > 5 else None,
                    "device_class": _convert_value(data.fields[6].raw_value) if len(data.fields) > 6 else None,
                    "unit_device_class": data.fields[6].unit_of_measurement if len(data.fields) > 6 else None,
                    "industry_code": _convert_value(data.fields[7].raw_value) if len(data.fields) > 7 else None,
                    "unit_industry_code": data.fields[7].unit_of_measurement if len(data.fields) > 7 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.PRODUCT and data.PGN == 59392:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "control": _convert_value(data.fields[0].raw_value) if len(data.fields) > 0 else None,
                    "unit_control": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "group_function": _convert_value(data.fields[1].value) if len(data.fields) > 1 else None,
                    "unit_group_function": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "reserved_16": data.fields[2].value if len(data.fields) > 2 else None,
                    "unit_reserved_16": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "ack_pgn": data.fields[3].value if len(data.fields) > 3 else None,
                    "unit_ack_pgn": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.STEERING and data.PGN == 127245:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "instance": data.fields[0].value if len(data.fields) > 0 else None,
                    "unit_instance": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "direction_order": _convert_value(data.fields[1].raw_value) if len(data.fields) > 1 else None,
                    "unit_direction_order": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "reserved_11": _convert_value(data.fields[2].value) if len(data.fields) > 2 else None,
                    "unit_reserved_11": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "angle_order": _convert_value(data.fields[3].value) if len(data.fields) > 3 else None,
                    "unit_angle_order": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "position": _convert_value(data.fields[4].value) if len(data.fields) > 4 else None,
                    "unit_position": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "reserved_48": data.fields[5].value if len(data.fields) > 5 else None,
                    "unit_reserved_48": data.fields[5].unit_of_measurement if len(data.fields) > 5 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.BATTERY and data.PGN == 127508:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "instance": data.fields[0].value if len(data.fields) > 0 else None,
                    "unit_instance": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "voltage": data.fields[1].value if len(data.fields) > 1 else None,
                    "unit_voltage": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "current": data.fields[2].value if len(data.fields) > 2 else None,
                    "unit_current": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "temperature": data.fields[3].value if len(data.fields) > 3 else None,
                    "unit_temperature": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "sid": data.fields[4].value if len(data.fields) > 4 else None,
                    "unit_sid": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            elif category == DataCategories.BATTERY and data.PGN == 127506:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "sid": data.fields[0].value if len(data.fields) > 0 else None,
                    "unit_sid": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "instance": data.fields[1].value if len(data.fields) > 1 else None,
                    "unit_instance": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "dc_type": _convert_value(data.fields[2].raw_value) if len(data.fields) > 2 else None,
                    "unit_dc_type": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "state_of_charge": _convert_value(data.fields[3].value) if len(data.fields) > 3 else None,
                    "unit_state_of_charge": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "state_of_health": data.fields[4].value if len(data.fields) > 4 else None,
                    "unit_state_of_health": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "time_remaining": data.fields[5].value if len(data.fields) > 5 else None,
                    "unit_time_remaining": data.fields[5].unit_of_measurement if len(data.fields) > 5 else None,
                    "ripple_voltage": data.fields[6].value if len(data.fields) > 6 else None,
                    "unit_ripple_voltage": data.fields[6].unit_of_measurement if len(data.fields) > 6 else None,
                    "remaining_capacity": data.fields[7].value if len(data.fields) > 7 else None,
                    "unit_remaining_capacity": data.fields[7].unit_of_measurement if len(data.fields) > 7 else None,
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
            else:
                # Generic extraction for unknown PGNs
                return {
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "fields": [_convert_value(field.value) for field in data.fields] if data.fields else [],
                    "timestamp": data.timestamp.isoformat() if hasattr(data.timestamp, 'isoformat') else str(data.timestamp)
                }
        except Exception as e:
            logger.error(f"Error extracting data fields: {e}")
            return {
                "pgn": data.PGN,
                "source": data.source,
                "dest": data.destination,
                "error": str(e),
                "timestamp": data.timestamp
            }
    
    def _send_parsed_data_to_db(self, data, category):
        """Send parsed data to database via Master Core -> DB Client"""
        try:
            # Extract data fields
            extracted_data = self._extract_data_fields(data, category)
            
            # Add TTL expiration
            expiration_date = datetime.now() + timedelta(days=self.data_ttl_days)
            extracted_data["ttl_expiration"] = expiration_date.isoformat()
            
            # Determine collection name based on category
            collection_name = self._get_collection_name(category)
            
            # Send to Master Core for database storage
            self.send_to_master_core(
                MessageType.COMMAND,
                {
                    "command": "store_can_data",
                    "collection": collection_name,
                    "data": extracted_data,
                    "category": category.value if hasattr(category, 'value') else str(category)
                },
                Priority.NORMAL
            )
            
            logger.debug(f"Sent {category} data to Master Core for storage in {collection_name}")
            
        except Exception as e:
            logger.error(f"Error sending parsed data to DB: {e}")
    
    def _get_collection_name(self, category):
        """Get MongoDB collection name based on data category"""
        if category == DataCategories.HEARTBEAT:
            return "NodeHeartbeat"
        elif category == DataCategories.FUEL:
            return "Fuel"
        elif category == DataCategories.NAVIGATION:
            return "Navigation"
        elif category == DataCategories.ENGINE:
            return "Engine"
        elif category == DataCategories.ENERGYDISTRIBUTION:
            return "EnergyDistribution"
        elif category == DataCategories.STEERING:
            return "Steering"
        elif category == DataCategories.BATTERY:
            return "Battery"
        elif category == DataCategories.PRODUCT:
            return "Product"
        else:
            return "Unknown"
    
    def _handle_can_command(self, message: NodeMessage, addr: tuple):
        """Handle CAN-specific commands"""
        command = message.payload.get("command")
        
        if command == "start_monitoring":
            if self.can_bus:
                # Already started
                self._send_response(message, {"status": "success", "message": "CAN monitoring already started"}, addr)
            else:
                success = self._start_can_bus()
                if success:
                    self._send_response(message, {"status": "success", "message": "CAN monitoring started"}, addr)
                else:
                    self._send_error_response(message, "Failed to start CAN bus", addr)
        elif command == "stop_monitoring":
            if not self.can_bus:
                self._send_response(message, {"status": "success", "message": "CAN monitoring already stopped"}, addr)
            else:
                self._stop_can_bus()
                self._send_response(message, {"status": "success", "message": "CAN monitoring stopped"}, addr)
        elif command == "send_message":
            can_data = message.payload.get("can_data")
            if can_data:
                success = self._send_can_message(can_data)
                self._send_response(message, {
                    "status": "success" if success else "error",
                    "message": "CAN message sent" if success else "Failed to send CAN message"
                }, addr)
            else:
                self._send_error_response(message, "Missing can_data in payload", addr)
        elif command == "send_j1939":
            # Handle J1939/NMEA2000 message format
            # Expected payload: {"pgn": int, "source_address": int, "data": list, "priority": int (optional)}
            pgn = message.payload.get("pgn")
            source_address = message.payload.get("source_address")
            data = message.payload.get("data", [])
            priority = message.payload.get("priority", 6)  # Default priority 6 (normal)
            
            if pgn is None or source_address is None:
                self._send_error_response(message, "Missing pgn or source_address in payload", addr)
                return
            
            success = self._send_j1939_message(pgn, source_address, data, priority)
            self._send_response(message, {
                "status": "success" if success else "error",
                "message": f"J1939 message sent (PGN: {pgn}, SA: {source_address})" if success else "Failed to send J1939 message",
                "pgn": pgn,
                "source_address": source_address
            }, addr)
        elif command == "send_can_message":
            # Handle send_can_message command from other nodes (e.g., Steering Control Node)
            # Expected payload: {"pgn": int, "data": dict}
            pgn = message.payload.get("pgn")
            data = message.payload.get("data", {})
            
            if pgn is None:
                self._send_error_response(message, "Missing pgn in payload", addr)
                return
            
            if pgn == 127245:  # Rudder PGN
                # Format NMEA2000 message
                logger.info(f"📤 Received send_can_message command for PGN 127245 (Rudder), data: {data}")
                can_message_data = self._format_rudder_message(data)
                if can_message_data:
                    logger.info(f"📤 Formatted CAN message: arbitration_id=0x{can_message_data['arbitration_id']:X}, data={can_message_data['data']}")
                    if not self.can_bus:
                        logger.error("❌ CAN bus not initialized! Cannot send message.")
                    success = self._send_can_message(can_message_data)
                    self._send_response(message, {
                        "status": "success" if success else "error",
                        "pgn": pgn,
                        "can_bus_initialized": self.can_bus is not None,
                        "message": "Rudder message sent to CAN bus" if success else "Failed to send rudder message (check CAN bus initialization)"
                    }, addr)
                else:
                    logger.error("❌ Failed to format rudder message")
                    self._send_error_response(message, "Failed to format rudder message", addr)
            else:
                self._send_error_response(message, f"Unsupported PGN for send_can_message: {pgn}", addr)
        else:
            logger.warning(f"Unknown CAN command: {command}")
            self._send_error_response(message, f"Unknown command: {command}", addr)
    
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
    
    def _start_heartbeat_task(self):
        """Start background heartbeat task"""
        self.heartbeat_task_running = True
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_worker)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        logger.debug("Background heartbeat task started")
    
    def _stop_heartbeat_task(self):
        """Stop background heartbeat task"""
        self.heartbeat_task_running = False
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=5)
        logger.debug("Background heartbeat task stopped")
    
    def _heartbeat_worker(self):
        """Background worker thread for periodic heartbeat"""
        while self.heartbeat_task_running:
            try:
                current_time = time.time()
                
                # Send heartbeat every 10 seconds (for connection state tracking)
                if int(current_time) % 10 == 0:
                    self.send_heartbeat()
                
                time.sleep(1)  # Check every second
            except Exception as e:
                logger.error(f"Error in heartbeat worker: {e}")
                time.sleep(1)
    
    def _format_rudder_message(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Format rudder position data as NMEA2000 CAN message (PGN 127245)
        
        Args:
            data: Dictionary containing rudder data fields:
                - instance: Instance ID (0-255)
                - position: Rudder position in radians
                - directionOrder: Direction order (0-65535, optional, default 0)
                - angleOrder: Requested angle in radians (optional, default 0.0)
                - reserved_48: Reserved field (optional, default 0)
        
        Returns:
            Dictionary with 'arbitration_id', 'data' (bytes), and 'is_extended_id' keys,
            or None if formatting fails
        """
        try:
            # Extract fields with defaults
            instance = int(data.get("instance", 0)) & 0xFF
            position_rad = float(data.get("position", 0.0))
            direction_order = int(data.get("directionOrder", 0)) & 0xFFFF
            angle_order_rad = float(data.get("angleOrder", 0.0))
            reserved_48 = int(data.get("reserved_48", 0xFFFF)) & 0xFFFF  # Default to 0xFFFF (16 bits) to match real-world hardware
            
            # NMEA2000 PGN 127245 (Rudder) BIT-LEVEL structure (per library definition):
            # Bit 0-7: Instance (8 bits)
            # Bit 8-10: Direction Order (3 bits)
            # Bit 11-15: Reserved (5 bits)
            # Bit 16-31: Angle Order (16 bits, signed, resolution 0.0001 rad/LSB)
            # Bit 32-47: Position (16 bits, signed, resolution 0.0001 rad/LSB)
            # Bit 48-63: Reserved (16 bits)
            
            # NMEA2000 uses 0.0001 rad per LSB for 16-bit signed values
            RAD_SCALE = 10000  # 0.0001 rad per LSB
            
            # Convert radians to scaled integers (signed 16-bit)
            angle_order_scaled = int(round(angle_order_rad * RAD_SCALE))
            angle_order_scaled = max(-32768, min(32767, angle_order_scaled))  # Clamp to int16
            # Convert to unsigned for bit manipulation (two's complement)
            if angle_order_scaled < 0:
                angle_order_unsigned = (angle_order_scaled + 65536) & 0xFFFF
            else:
                angle_order_unsigned = angle_order_scaled & 0xFFFF
            
            position_scaled = int(round(position_rad * RAD_SCALE))
            position_scaled = max(-32768, min(32767, position_scaled))  # Clamp to int16
            # Convert to unsigned for bit manipulation (two's complement)
            if position_scaled < 0:
                position_unsigned = (position_scaled + 65536) & 0xFFFF
            else:
                position_unsigned = position_scaled & 0xFFFF
            
            # Pack using bit-level encoding (matching NMEA2000 library structure)
            data_raw = 0
            data_raw |= (instance & 0xFF) << 0                    # Bits 0-7: Instance
            data_raw |= (direction_order & 0x7) << 8              # Bits 8-10: Direction Order (3 bits)
            data_raw |= (0 & 0x1F) << 11                          # Bits 11-15: Reserved (5 bits, set to 0)
            data_raw |= (angle_order_unsigned & 0xFFFF) << 16     # Bits 16-31: Angle Order
            data_raw |= (position_unsigned & 0xFFFF) << 32        # Bits 32-47: Position
            data_raw |= (reserved_48 & 0xFFFF) << 48              # Bits 48-63: Reserved
            
            # Convert to bytes (little-endian)
            data_bytes = data_raw.to_bytes(8, byteorder='little')
            
            # Get source address from config or use default
            source_address = self.get_config_value("can_source_address", 0x91)  # Default: 0x91 (common for control systems)
            priority = 6  # Normal priority for steering commands
            
            # Calculate CAN ID using NMEA2000 format
            pgn = 127245  # Rudder PGN
            can_id = self._j1939_to_can_id(pgn, source_address, priority)
            
            logger.debug(f"Formatted rudder message: instance={instance}, position={position_rad:.4f} rad ({position_scaled}), "
                        f"angle_order={angle_order_rad:.4f} rad ({angle_order_scaled}), CAN_ID=0x{can_id:X}")
            
            return {
                "arbitration_id": can_id,
                "data": list(data_bytes),  # Convert bytes to list for JSON serialization compatibility
                "is_extended_id": True  # NMEA2000 uses 29-bit extended IDs
            }
            
        except Exception as e:
            logger.error(f"Failed to format rudder message: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return None
    
    def _send_can_message(self, can_data: Dict[str, Any]):
        """Send message on CAN bus"""
        try:
            if not self.can_bus:
                logger.error("CAN bus not initialized")
                return False
            
            # Convert data list back to bytes if needed
            data = can_data["data"]
            if isinstance(data, list):
                data_bytes = bytes(data)
            else:
                data_bytes = data
                
            message = can.Message(
                arbitration_id=can_data["arbitration_id"],
                data=data_bytes,
                is_extended_id=can_data.get("is_extended_id", False)
            )
            
            self.can_bus.send(message)
            logger.info(f"✅ CAN message sent to bus: arbitration_id=0x{can_data['arbitration_id']:X}, data={[hex(b) for b in data_bytes]}, length={len(data_bytes)}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send CAN message: {e}")
            return False
    
    def _j1939_to_can_id(self, pgn: int, source_address: int, priority: int = 6) -> int:
        """
        Convert J1939/NMEA2000 parameters to CAN arbitration ID
        
        CAN ID format (29-bit extended):
        - Priority (3 bits, bits 26-28)
        - Reserved (1 bit, bit 25, always 0)
        - PGN (18 bits, bits 8-24)
        - Source Address (8 bits, bits 0-7)
        
        Formula: (priority << 26) | (pgn << 8) | source_address
        """
        # Ensure values are within valid ranges
        priority = max(0, min(7, priority))  # 3 bits: 0-7
        pgn = max(0, min(0x3FFFF, pgn))  # 18 bits: 0-262143
        source_address = max(0, min(0xFF, source_address))  # 8 bits: 0-255
        
        can_id = (priority << 26) | (pgn << 8) | source_address
        return can_id
    
    def _send_j1939_message(self, pgn: int, source_address: int, data: list, priority: int = 6) -> bool:
        """
        Send J1939/NMEA2000 message on CAN bus
        
        Args:
            pgn: Parameter Group Number (18-bit)
            source_address: Source address (8-bit)
            data: Message data (list of bytes/ints, 0-8 bytes)
            priority: Message priority (0-7, default 6 for normal)
        
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        try:
            if not self.can_bus:
                logger.error("CAN bus not initialized - cannot send J1939 message")
                return False
            
            # Convert data to bytes if needed
            if isinstance(data, list):
                data_bytes = bytes([int(b) & 0xFF for b in data])
            else:
                data_bytes = bytes(data)
            
            # Limit to 8 bytes (CAN frame max)
            if len(data_bytes) > 8:
                logger.warning(f"J1939 data truncated from {len(data_bytes)} to 8 bytes")
                data_bytes = data_bytes[:8]
            
            # Convert J1939 parameters to CAN arbitration ID
            can_id = self._j1939_to_can_id(pgn, source_address, priority)
            
            # Create CAN message (NMEA2000 uses extended 29-bit IDs)
            can_message = can.Message(
                arbitration_id=can_id,
                data=data_bytes,
                is_extended_id=True  # NMEA2000 uses 29-bit extended IDs
            )
            
            # Send message
            self.can_bus.send(can_message)
            logger.info(f"J1939 message sent: PGN=0x{pgn:X} ({pgn}), SA=0x{source_address:02X} ({source_address}), "
                       f"CAN_ID=0x{can_id:X}, data={list(data_bytes)}, priority={priority}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send J1939 message (PGN: {pgn}, SA: {source_address}): {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _send_response(self, original_message: NodeMessage, data: Dict[str, Any], addr: tuple):
        """Send response message"""
        response = NodeMessage(
            message_id=str(uuid.uuid4()),
            type=MessageType.RESPONSE,
            priority=Priority.NORMAL,
            source=self.node_name,
            destination=original_message.source,
            payload=data,
            timestamp=time.time()
        )
        self._send_message(response, addr)
    
    def _send_error_response(self, original_message: NodeMessage, error: str, addr: tuple):
        """Send error response message"""
        response = NodeMessage(
            message_id=str(uuid.uuid4()),
            type=MessageType.RESPONSE,
            priority=Priority.HIGH,
            source=self.node_name,
            destination=original_message.source,
            payload={"error": error},
            timestamp=time.time()
        )
        self._send_message(response, addr)
    
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

# Global variables for signal handler
_node_instance = None
_running = True

def signal_handler(signum, frame):
    """Handle SIGINT and SIGTERM signals for graceful shutdown"""
    global _running
    signal_name = signal.Signals(signum).name
    logger.info(f"Received {signal_name} signal, initiating graceful shutdown...")
    _running = False
    if _node_instance:
        _node_instance.stop()

def main():
    """Main entry point for CAN Controller Node"""
    global _node_instance, _running
    
    # Register signal handlers for graceful shutdown
    # SIGINT: Ctrl+C or kill -INT
    # SIGTERM: systemd/service manager shutdown or kill -TERM
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
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
            "node_port": 14554,
            "master_core_host": "localhost",
            "master_core_port": 14551,  # Master Core UDP server port
            "direct_communication": True,
            "emergency_nodes": ["engine", "steering", "autopilot"],
            "playback_enabled": True
        }
    
    # Configure logging
    # On Windows daemon mode, ensure errors go to stderr for Master Core to capture
    if args.daemon and platform.system() == 'Windows':
        # Windows daemon: log to stderr so Master Core can capture it
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stderr)]
        )
    else:
        # Normal mode: use default stdout
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    # Create and start node
    node = CANControllerNode(config)
    _node_instance = node  # Store for signal handler
    
    if args.daemon:
        # Run as daemon (cross-platform)
        
        # Windows doesn't support fork(), so run in background without forking
        if platform.system() == 'Windows':
            # Windows: Run in background without forking
            # Write PID file to temp directory
            temp_dir = tempfile.gettempdir()
            pid_file = os.path.join(temp_dir, 'can_controller_node.pid')
            with open(pid_file, 'w') as f:
                f.write(str(os.getpid()))
            
            logger.info(f"Running as background process on Windows (PID: {os.getpid()})")
            if node.start():
                logger.info("CAN Controller Node started successfully, entering main loop")
                _running = True
                try:
                    while _running:
                        time.sleep(1)
                except KeyboardInterrupt:
                    # Fallback for KeyboardInterrupt
                    logger.info("KeyboardInterrupt received, shutting down")
                    _running = False
            else:
                logger.error("Failed to start CAN Controller Node, exiting")
                sys.exit(1)
            node.stop()
        else:
            # Unix/Linux: Use fork approach
            try:
                pid = os.fork()
                if pid > 0:
                    # Parent process - exit
                    sys.exit(0)
            except OSError as e:
                logger.error(f"Failed to fork: {e}")
                sys.exit(1)
            
            # Child process - continue
            os.setsid()  # Create new session
            os.chdir("/")  # Change to root directory
            
            # Write PID file (use temp directory for cross-platform consistency)
            temp_dir = tempfile.gettempdir()
            pid_file = os.path.join(temp_dir, 'can_controller_node.pid')
            with open(pid_file, 'w') as f:
                f.write(str(os.getpid()))
            
            if node.start():
                _running = True
                try:
                    while _running:
                        time.sleep(1)
                except KeyboardInterrupt:
                    # Fallback for KeyboardInterrupt
                    logger.info("KeyboardInterrupt received, shutting down")
                    _running = False
            else:
                logger.error("Failed to start CAN Controller Node")
                sys.exit(1)
            node.stop()
    else:
        # Run in foreground
        if node.start():
            node.run_daemon()
        else:
            logger.error("Failed to start CAN Controller Node")
            sys.exit(1)

if __name__ == "__main__":
    main()
