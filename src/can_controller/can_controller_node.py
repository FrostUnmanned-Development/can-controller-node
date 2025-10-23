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
import os
import sys

# Add parent directory to path for base_node import
sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent / "src" / "onboard_core"))

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
        self.data_ttl_days = config.get("data_ttl_days", 7)
        
        # CAN file playback
        self.playback_enabled = config.get("playback_enabled", True)
        self.playback_thread = None
        self.playback_running = False
        
        # Register CAN-specific handlers
        self.register_handler(MessageType.COMMAND, self._handle_can_command)
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
        try:
            if category == DataCategories.HEARTBEAT and data.PGN == 126993:
                # Generate title from field IDs (modularized function)
                title = self._generate_field_title(data.fields)
                
                return {
                    "title": title,
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "data_transmit_offset": data.fields[0].value if len(data.fields) > 0 else None,
                    "unit_data_transmit_offset": data.fields[0].unit_of_measurement if len(data.fields) > 0 else None,
                    "sequence_counter": data.fields[1].value if len(data.fields) > 1 else None,
                    "unit_sequence_counter": data.fields[1].unit_of_measurement if len(data.fields) > 1 else None,
                    "controller1_state": data.fields[2].raw_value if len(data.fields) > 2 else None,
                    "unit_controller1_state": data.fields[2].unit_of_measurement if len(data.fields) > 2 else None,
                    "controller2_state": data.fields[3].raw_value if len(data.fields) > 3 else None,
                    "unit_controller2_state": data.fields[3].unit_of_measurement if len(data.fields) > 3 else None,
                    "equipment_status": data.fields[4].raw_value if len(data.fields) > 4 else None,
                    "unit_equipment_status": data.fields[4].unit_of_measurement if len(data.fields) > 4 else None,
                    "reserved_30": data.fields[5].value if len(data.fields) > 5 else None,
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
            else:
                # Generic extraction for unknown PGNs
                return {
                    "pgn": data.PGN,
                    "source": data.source,
                    "dest": data.destination,
                    "fields": [field.value for field in data.fields] if data.fields else [],
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
        else:
            return "Unknown"
    
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
    import sys
    import os
    
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
            "node_port": 14553,
            "master_core_host": "localhost",
            "master_core_port": 14551,  # IPC server port
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
        # Run as daemon using simple fork approach
        
        # Fork the process
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
        
        # Write PID file
        with open('/tmp/can_controller_node.pid', 'w') as f:
            f.write(str(os.getpid()))
        
        if node.start():
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass
        else:
            logger.error("Failed to start CAN Controller Node")
            sys.exit(1)
    else:
        # Run in foreground
        if node.start():
            node.run_daemon()
        else:
            logger.error("Failed to start CAN Controller Node")
            sys.exit(1)

if __name__ == "__main__":
    main()
