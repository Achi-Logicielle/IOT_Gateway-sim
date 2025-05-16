#!/usr/bin/env python3
"""
IoT Gateway Simulator

This script simulates an IoT gateway that collects data from various energy-related sensors
and publishes it to an MQTT broker, which is part of a Microgrid Energy Management System.
"""

import json
import random
import time
from datetime import datetime
import paho.mqtt.client as mqtt
import argparse
import logging
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("IoT_Gateway_Simulator")

# Constants
DEFAULT_BROKER_HOST = "localhost"
DEFAULT_BROKER_PORT = 1883
DEFAULT_QOS = 1

class SensorType:
    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"
    VOLTAGE = "voltage"
    CURRENT = "current"
    POWER = "power"
    ENERGY = "energy"
    SOLAR_IRRADIANCE = "solar_irradiance"
    WIND_SPEED = "wind_speed"
    BATTERY_LEVEL = "battery_level"

class IoTGatewaySimulator:
    def __init__(self, broker_host, broker_port, num_sensors=10, gateway_id=None):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.gateway_id = gateway_id or f"gateway-{uuid.uuid4().hex[:8]}"
        self.client = None
        self.connected = False
        self.num_sensors = num_sensors
        self.sensors = self._create_sensors(num_sensors)
        
    def _create_sensors(self, num_sensors):
        """Create a collection of simulated sensors"""
        sensors = []
        
        # Ensure at least one of each important sensor type
        sensor_types = [
            SensorType.TEMPERATURE,
            SensorType.HUMIDITY,
            SensorType.VOLTAGE,
            SensorType.CURRENT,
            SensorType.POWER,
            SensorType.ENERGY,
            SensorType.SOLAR_IRRADIANCE,
            SensorType.WIND_SPEED,
            SensorType.BATTERY_LEVEL
        ]
        
        # Add core sensor types
        for i, sensor_type in enumerate(sensor_types):
            if i >= num_sensors:
                break
                
            sensors.append({
                "id": f"sensor-{i+1}",
                "type": sensor_type,
                "location": f"zone-{(i % 3) + 1}"
            })
        
        # Add additional random sensors if needed
        for i in range(len(sensor_types), num_sensors):
            sensor_type = random.choice(sensor_types)
            sensors.append({
                "id": f"sensor-{i+1}",
                "type": sensor_type,
                "location": f"zone-{(i % 3) + 1}"
            })
            
        return sensors
    
    def connect(self):
        """Connect to the MQTT broker"""
        self.client = mqtt.Client(client_id=f"iot-gateway-{self.gateway_id}")
        
        # Set up callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish
        
        try:
            logger.info(f"Connecting to MQTT broker at {self.broker_host}:{self.broker_port}")
            self.client.connect(self.broker_host, self.broker_port, 60)
            self.client.loop_start()
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            return False
    
    def _on_connect(self, client, userdata, flags, rc):
        """Callback for when the client connects to the broker"""
        if rc == 0:
            self.connected = True
            logger.info("Connected to MQTT broker successfully")
        else:
            logger.error(f"Failed to connect to broker with result code {rc}")
    
    def _on_disconnect(self, client, userdata, rc):
        """Callback for when the client disconnects from the broker"""
        self.connected = False
        if rc != 0:
            logger.warning(f"Unexpected disconnection from broker: {rc}")
        else:
            logger.info("Disconnected from broker")
    
    def _on_publish(self, client, userdata, mid):
        """Callback for when a message is published"""
        logger.debug(f"Message {mid} published")
        
    def _generate_sensor_reading(self, sensor):
        """Generate realistic sensor data based on sensor type"""
        reading = {"timestamp": datetime.now().isoformat()}
        
        if sensor["type"] == SensorType.TEMPERATURE:
            # Temperature in Celsius
            reading["value"] = round(random.uniform(18.0, 30.0), 2)
            reading["unit"] = "°C"
            
        elif sensor["type"] == SensorType.HUMIDITY:
            # Humidity as percentage
            reading["value"] = round(random.uniform(30.0, 70.0), 2)
            reading["unit"] = "%"
            
        elif sensor["type"] == SensorType.VOLTAGE:
            # Voltage in volts
            reading["value"] = round(random.uniform(220.0, 240.0), 1)
            reading["unit"] = "V"
            
        elif sensor["type"] == SensorType.CURRENT:
            # Current in amperes
            reading["value"] = round(random.uniform(0.5, 15.0), 2)
            reading["unit"] = "A"
            
        elif sensor["type"] == SensorType.POWER:
            # Power in watts
            reading["value"] = round(random.uniform(100.0, 5000.0), 2)
            reading["unit"] = "W"
            
        elif sensor["type"] == SensorType.ENERGY:
            # Energy in kilowatt-hours
            reading["value"] = round(random.uniform(0.1, 50.0), 3)
            reading["unit"] = "kWh"
            
        elif sensor["type"] == SensorType.SOLAR_IRRADIANCE:
            # Solar irradiance in watts per square meter
            hour = datetime.now().hour
            # Simulate day/night cycle
            if 6 <= hour <= 18:
                base = 600 * math.sin(math.pi * (hour - 6) / 12)
                reading["value"] = round(max(0, base + random.uniform(-50, 50)), 2)
            else:
                reading["value"] = 0.0
            reading["unit"] = "W/m²"
            
        elif sensor["type"] == SensorType.WIND_SPEED:
            # Wind speed in meters per second
            reading["value"] = round(random.uniform(0.0, 15.0), 2)
            reading["unit"] = "m/s"
            
        elif sensor["type"] == SensorType.BATTERY_LEVEL:
            # Battery level as percentage
            reading["value"] = round(random.uniform(20.0, 95.0), 1)
            reading["unit"] = "%"
            
        else:
            # Generic sensor
            reading["value"] = round(random.uniform(0.0, 100.0), 2)
            reading["unit"] = "units"
            
        return reading
        
    def publish_sensor_data(self):
        """Publish data for all sensors to the MQTT broker"""
        if not self.connected:
            logger.error("Not connected to MQTT broker. Cannot publish data.")
            return False
            
        for sensor in self.sensors:
            reading = self._generate_sensor_reading(sensor)
            
            # Build complete message
            message = {
                "gateway_id": self.gateway_id,
                "sensor_id": sensor["id"],
                "sensor_type": sensor["type"],
                "location": sensor["location"],
                "reading": reading
            }
            
            # Build topic path
            topic = f"iot/gateway/{self.gateway_id}/sensors/{sensor['type']}/{sensor['id']}"
            
            # Convert message to JSON and publish
            payload = json.dumps(message)
            
            try:
                logger.debug(f"Publishing to {topic}: {payload}")
                result = self.client.publish(topic, payload, qos=DEFAULT_QOS)
                if result.rc != mqtt.MQTT_ERR_SUCCESS:
                    logger.error(f"Failed to publish message to {topic}: {result.rc}")
            except Exception as e:
                logger.error(f"Error publishing message: {e}")
                
        return True
        
    def run(self, interval=5, duration=None):
        """Run the simulator for a specified duration or indefinitely"""
        logger.info(f"Starting IoT Gateway simulator with {len(self.sensors)} sensors")
        logger.info(f"Publishing data every {interval} seconds")
        
        if duration:
            logger.info(f"Simulator will run for {duration} seconds")
            end_time = time.time() + duration
        else:
            logger.info("Simulator will run until interrupted")
            end_time = None
            
        try:
            count = 0
            while end_time is None or time.time() < end_time:
                if self.publish_sensor_data():
                    count += 1
                    logger.info(f"Published sensor data batch #{count}")
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Simulator interrupted by user")
        finally:
            self.disconnect()
            
    def disconnect(self):
        """Disconnect from the MQTT broker"""
        if self.client and self.connected:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("Disconnected from MQTT broker")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="IoT Gateway Simulator for Energy Management System")
    parser.add_argument("--broker-host", default=DEFAULT_BROKER_HOST, help=f"MQTT broker hostname (default: {DEFAULT_BROKER_HOST})")
    parser.add_argument("--broker-port", type=int, default=DEFAULT_BROKER_PORT, help=f"MQTT broker port (default: {DEFAULT_BROKER_PORT})")
    parser.add_argument("--interval", type=int, default=5, help="Data publishing interval in seconds (default: 5)")
    parser.add_argument("--duration", type=int, help="Duration to run in seconds (default: run indefinitely)")
    parser.add_argument("--sensors", type=int, default=10, help="Number of sensors to simulate (default: 10)")
    parser.add_argument("--gateway-id", help="Custom gateway ID (default: auto-generated)")
    
    args = parser.parse_args()
    
    # Create and run the simulator
    simulator = IoTGatewaySimulator(
        broker_host=args.broker_host,
        broker_port=args.broker_port,
        num_sensors=args.sensors,
        gateway_id=args.gateway_id
    )
    
    if simulator.connect():
        simulator.run(interval=args.interval, duration=args.duration)
    else:
        logger.error("Failed to start simulator due to connection issues")
        return 1
        
    return 0

if __name__ == "__main__":
    import math  # Import needed for solar irradiance calculation
    exit(main())
