# CAN Controller Node

A specialized node for CAN bus communication in the OBS01PY system.

## Features

- **CAN Bus Communication**: Real-time CAN message processing
- **Direct Node Communication**: Emergency and critical system messaging
- **CAN File Playback**: Testing capability with recorded CAN data
- **Data Streaming**: Broadcast CAN data to subscribed nodes
- **Emergency Stop**: Automatic emergency procedures

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Edit `config.json` to configure:
- CAN interface settings
- Node communication ports
- Emergency node list
- Playback settings

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

## CAN File Playback

Place CAN log files in the `data/` directory and use the playback command:

```bash
# Via UDP client
python ../../src/onboard_core/obs_client.py play_can_file test_can_log.txt
```

## Testing

```bash
# Unit tests
pytest tests/ -v

# Integration tests
python tests/test_can_integration.py
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