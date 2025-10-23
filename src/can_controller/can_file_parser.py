#!/usr/bin/env python3
"""
CAN File Playback Utility - Parses and replays CAN log files
"""

import os
import sys
import time
import re
import logging
import can
from pathlib import Path
from typing import List, Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CANFileParser:
    """Parser for CAN log files in the format shown in the image"""
    
    def __init__(self):
        self.messages = []
        
    def parse_log_file(self, file_path: str) -> List[Dict]:
        """Parse a CAN log file and extract messages"""
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"CAN file not found: {file_path}")
            
        logger.info(f"Parsing CAN file: {file_path}")
        
        with open(file_path, 'r') as f:
            lines = f.readlines()
            
        # Skip header line if present
        start_line = 0
        if lines and any(keyword in lines[0].lower() for keyword in ['winno', 'pgn', 'sa', 'da']):
            start_line = 1
            
        for line_num, line in enumerate(lines[start_line:], start_line + 1):
            try:
                message = self._parse_kvaser_line(line.strip())
                if message:
                    self.messages.append(message)
            except Exception as e:
                logger.warning(f"Failed to parse line {line_num}: {e}")
                continue
                
        logger.info(f"Parsed {len(self.messages)} CAN messages from file")
        return self.messages
        
    def _parse_kvaser_line(self, line: str) -> Optional[Dict]:
        """Parse a single line from the Kvaser CAN log file"""
        if not line or line.startswith('#'):
            return None
            
        # Split by multiple spaces to handle variable spacing
        parts = re.split(r'\s+', line.strip())
        if len(parts) < 12:  # Minimum expected columns for Kvaser format
            return None
            
        try:
            # Extract fields based on the real Kvaser log format:
            # Header: WinNo P   PGN SA  DA Flg   Len  D0...1...2...3...4...5...6..D7      Time   Dir
            # Actual: CAN 1 6 1F10D DE->*         8   05  FF  5E  06  FF  FF  FF  FF    1840.937662 R
            # The header is misleading! The actual format is:
            # CAN 1 6 1F211 91->* means Priority=1, Unknown=6, PGN=1F211, SA=91, DA=*
            win_no = parts[0] if len(parts) > 0 else ""
            priority = parts[1] if len(parts) > 1 else ""
            unknown_field = parts[2] if len(parts) > 2 else ""  # Unknown field (maybe reserved)
            pgn = parts[3] if len(parts) > 3 else ""  # PGN is in position 3
            sa_da_field = parts[4] if len(parts) > 4 else ""  # SA->DA field like "91->*"
            length = parts[5] if len(parts) > 5 else "8"
            
            # Extract data bytes (D0-D7) - they start from index 6
            data_bytes = []
            for i in range(6, min(14, len(parts))):  # D0-D7
                if parts[i] and parts[i] != '-':
                    try:
                        data_bytes.append(int(parts[i], 16))
                    except ValueError:
                        data_bytes.append(0)
                        
            # Pad to 8 bytes if needed
            while len(data_bytes) < 8:
                data_bytes.append(0)
                
            # Extract timestamp (second to last field)
            timestamp = float(parts[-2]) if len(parts) > 1 else time.time()
            
            # Extract direction (last field)
            direction = parts[-1] if len(parts) > 0 else "R"
            
            # Convert PGN to integer (handle hex format)
            try:
                # Remove any non-hex characters and convert
                pgn_clean = pgn.replace('0x', '').replace('0X', '')
                pgn_int = int(pgn_clean, 16) if pgn_clean else 0
            except ValueError:
                pgn_int = 0
                
            # Convert source address to integer (extract from SA->DA field)
            try:
                if '->' in sa_da_field:
                    sa_part = sa_da_field.split('->')[0]
                    sa_int = int(sa_part, 16) if sa_part else 0
                else:
                    sa_int = int(sa_da_field, 16) if sa_da_field else 0
            except ValueError:
                sa_int = 0
                
            # Convert destination address to integer (handle ->* format)
            # Format: "91->*" means SA=91, DA=* (broadcast = 0xFF)
            try:
                if '->' in sa_da_field:
                    # Extract destination from "91->*" format
                    da_part = sa_da_field.split('->')[1] if '->' in sa_da_field else sa_da_field
                    if da_part == '*':
                        da_int = 0xFF  # Broadcast
                    else:
                        da_int = int(da_part, 16) if da_part else 0xFF
                else:
                    da_int = int(sa_da_field, 16) if sa_da_field else 0xFF
            except ValueError:
                da_int = 0xFF
                
            # Construct CAN ID (simplified NMEA2000 format)
            # Priority (3 bits) + Reserved (1 bit) + PGN (18 bits) + Source Address (8 bits)
            priority_int = int(priority) if priority.isdigit() else 6
            can_id = (priority_int << 26) | (pgn_int << 8) | sa_int
            
            message = {
                'can_id': can_id,
                'pgn': pgn_int,
                'source': sa_int,
                'destination': da_int,
                'priority': priority_int,
                'data': data_bytes[:8],  # Ensure exactly 8 bytes
                'timestamp': timestamp,
                'direction': direction,
                'raw_line': line
            }
            
            return message
            
        except Exception as e:
            logger.debug(f"Error parsing line '{line}': {e}")
            return None

class CANFilePlayer:
    """Plays back CAN messages from a parsed file"""
    
    def __init__(self, bus: can.BusABC):
        self.bus = bus
        self.is_playing = False
        self.playback_thread = None
        
    def play_file(self, messages: List[Dict], speed_multiplier: float = 1.0):
        """Play back CAN messages with optional speed control"""
        if not messages:
            logger.warning("No messages to play back")
            return
            
        logger.info(f"Starting playback of {len(messages)} messages (speed: {speed_multiplier}x)")
        self.is_playing = True
        
        # Calculate time intervals between messages
        if len(messages) > 1:
            base_interval = messages[1]['timestamp'] - messages[0]['timestamp']
            playback_interval = base_interval / speed_multiplier
        else:
            playback_interval = 0.1  # Default 100ms
            
        for i, msg_data in enumerate(messages):
            if not self.is_playing:
                break
                
            try:
                # Create CAN message
                can_msg = can.Message(
                    arbitration_id=msg_data['can_id'],
                    data=msg_data['data'],
                    is_extended_id=True
                )
                
                # Send message
                self.bus.send(can_msg)
                logger.debug(f"Sent message {i+1}/{len(messages)}: PGN={hex(msg_data['pgn'])}, "
                           f"CAN ID={hex(msg_data['can_id'])}, data={[hex(x) for x in msg_data['data']]}")
                
                # Wait for next message
                if i < len(messages) - 1:
                    next_timestamp = messages[i + 1]['timestamp']
                    current_timestamp = msg_data['timestamp']
                    interval = (next_timestamp - current_timestamp) / speed_multiplier
                    time.sleep(max(0.001, interval))  # Minimum 1ms interval
                    
            except Exception as e:
                logger.error(f"Error sending message {i+1}: {e}")
                
        logger.info("Playback completed")
        self.is_playing = False
        
    def stop_playback(self):
        """Stop the current playback"""
        self.is_playing = False
        logger.info("Playback stopped")

def main():
    """Main function for testing CAN file playback"""
    if len(sys.argv) < 2:
        print("Usage: python can_file_playback.py <can_file_path> [speed_multiplier]")
        sys.exit(1)
        
    file_path = sys.argv[1]
    speed_multiplier = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
    
    try:
        # Parse the CAN file
        parser = CANFileParser()
        messages = parser.parse_log_file(file_path)
        
        if not messages:
            print("No valid messages found in file")
            sys.exit(1)
            
        # Initialize CAN bus (using virtual CAN for testing)
        bus = can.interface.Bus(interface='socketcan', channel='vcan0', bitrate=250000)
        
        # Create player and start playback
        player = CANFilePlayer(bus)
        
        print(f"Starting playback of {len(messages)} messages...")
        print("Press Ctrl+C to stop")
        
        player.play_file(messages, speed_multiplier)
        
    except KeyboardInterrupt:
        print("\nPlayback interrupted by user")
    except Exception as e:
        logger.error(f"Playback error: {e}")
    finally:
        if 'bus' in locals():
            bus.shutdown()

if __name__ == "__main__":
    main()
