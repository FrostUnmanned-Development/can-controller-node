# CAN Controller Node

A specialized node for CAN bus communication and NMEA2000 message processing in the OBS01PY system.

## Features

- **CAN Bus Communication**: Real-time CAN message processing with python-can
- **NMEA2000 PGN Parsing**: Automatic decoding of NMEA2000 Parameter Group Numbers
- **Data Categorization**: Intelligent mapping of PGNs to data categories (HEARTBEAT, ENGINE, NAVIGATION, etc.)
- **IPC Communication**: Seamless integration with Master Core and DB Client via standardized IPC
- **CAN File Playback**: Testing capability with recorded CAN data (Kvaser format)
- **TTL Data Management**: Automatic data expiration for database storage
- **Direct Node Communication**: Emergency and critical system messaging
- **Data Streaming**: Broadcast CAN data to subscribed nodes

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Edit `config.json` to configure:
- CAN interface settings (socketcan, vcan, etc.)
- Node communication ports
- Master Core IPC connection
- Emergency node list
- Playback settings

```json
{
    "can_interface": "socketcan",
    "can_channel": "vcan0",
    "can_bitrate": 250000,
    "node_port": 14553,
    "master_core_host": "localhost",
    "master_core_port": 14551,
    "direct_communication": true,
    "emergency_nodes": ["engine", "steering", "autopilot"],
    "playback_enabled": true
}
```

## Usage

### Run as standalone node:
```bash
python src/can_controller/can_controller_node.py
```

### Run as daemon:
```bash
python src/can_controller/can_controller_node.py --daemon
```

### With custom config:
```bash
python src/can_controller/can_controller_node.py --config my_config.json
```

## NMEA2000 PGN Processing

The CAN Controller automatically processes NMEA2000 messages and categorizes them:

### Supported PGNs:
- **126993**: HEARTBEAT - System status and health
- **127505**: FUEL - Fuel consumption and tank levels
- **127250**: NAVIGATION - Vessel heading and speed
- **127257**: NAVIGATION - Attitude and position
- **129026**: NAVIGATION - Cross track error
- **129025**: NAVIGATION - Position rapid update
- **129029**: NAVIGATION - GNSS position data
- **129540**: NAVIGATION - GNSS satellites in view
- **126992**: NAVIGATION - System time
- **127488**: ENGINE - Engine parameters
- **127751**: ENERGYDISTRIBUTION - Power management

### Data Flow:
```
CAN Bus → CAN Controller → NMEA2000 Decoder → Data Categorization → IPC to Master Core → DB Client → MongoDB
```

## CAN File Playback

Place CAN log files in the `data/` directory and use the playback command:

```bash
# Via UDP client (from main OBS01PY directory)
python ../../src/onboard_core/obs_client.py play_can_file test_can_log.txt
```

### Supported Log Formats:
- **Kvaser Format**: `CAN 1 6 1F211 91->* 8 00 0E 51 FF FF FF FF FF 1842.051382 R`
- **Candump Format**: `vcan0 19F211FE [8] 00 FC 53 FF FF FF FF FF`

## IPC Communication

The CAN Controller communicates with other nodes via standardized IPC messages:

### Outgoing Messages:
- `store_can_data`: Send parsed CAN data to Master Core for database storage
- `data`: Stream raw CAN messages to subscribed nodes
- `status`: Report CAN interface and processing status

### Incoming Messages:
- `can_command`: Control CAN bus operations
- `subscribe_data`: Subscribe to CAN data stream
- `emergency_stop`: Trigger emergency stop procedures

## Testing

```bash
# Unit tests
pytest tests/ -v

# Integration tests
python tests/test_can_integration.py

# Test with virtual CAN interface
sudo modprobe vcan
sudo ip link add dev vcan0 type vcan
sudo ip link set up vcan0
```

## API

### Commands
- `can_command`: Control CAN bus operations
- `subscribe_data`: Subscribe to CAN data stream
- `emergency_stop`: Trigger emergency stop
- `play_can_file`: Start CAN file playback

### Status
Returns CAN-specific status including:
- CAN interface status
- Active subscribers
- Playback status
- Emergency stop capability
- PGN processing statistics
- Data categorization metrics