import os
import logging
import threading
from flask import Flask, request, jsonify
from dotenv import load_dotenv

from config_loader import get_kafka_producer_config, load_kafka_config
from kafka_producer import KafkaProducerWrapper
from free_producer import FreeProducer
from scenarios import (
    LagSpikeScenario,
    ConsumerSlowScenario,
    RebalanceStormScenario,
    HotPartitionScenario
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Global state
producer_wrapper = None
free_producer = None
scenarios = {}
active_scenario = None
scenario_lock = threading.Lock()


def init_producer():
    """Initialize Kafka producer and scenarios"""
    global producer_wrapper, free_producer, scenarios

    try:
        kafka_config = get_kafka_producer_config()
        producer_wrapper = KafkaProducerWrapper(kafka_config)

        # Initialize free producer
        free_producer = FreeProducer(producer_wrapper)

        # Initialize scenarios
        kafka_topic_config = load_kafka_config()
        scenarios = {
            "lag_spike": LagSpikeScenario(producer_wrapper, kafka_topic_config),
            "consumer_slow": ConsumerSlowScenario(producer_wrapper, kafka_topic_config),
            "rebalance_storm": RebalanceStormScenario(producer_wrapper, kafka_topic_config),
            "hot_partition": HotPartitionScenario(producer_wrapper, kafka_topic_config)
        }

        logger.info("Simulator initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize simulator: {e}", exc_info=True)
        raise


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "ifood-anomaly-simulator"}), 200


@app.route('/simulator/status', methods=['GET'])
def get_status():
    """Get current simulator status"""
    return jsonify({
        "free_producer": {
            "running": free_producer.is_running() if free_producer else False,
            "throughput": free_producer.throughput if free_producer else 0,
            "consumer_group": free_producer.consumer_group if free_producer else None
        },
        "active_scenario": active_scenario,
        "available_scenarios": list(scenarios.keys())
    }), 200


@app.route('/simulator/free-producer/start', methods=['POST'])
def start_free_producer():
    """Start free producer with configurable throughput"""
    data = request.json or {}

    throughput = data.get('throughput', 100)
    consumer_group = data.get('consumer_group', 'checkout-service')
    target_topic = data.get('target_topic', 'orders')

    try:
        free_producer.start(
            throughput=throughput,
            consumer_group=consumer_group,
            target_topic=target_topic
        )

        return jsonify({
            "status": "started",
            "throughput": throughput,
            "consumer_group": consumer_group,
            "target_topic": target_topic
        }), 200
    except Exception as e:
        logger.error(f"Failed to start free producer: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/simulator/free-producer/stop', methods=['POST'])
def stop_free_producer():
    """Stop free producer"""
    try:
        free_producer.stop()
        return jsonify({"status": "stopped"}), 200
    except Exception as e:
        logger.error(f"Failed to stop free producer: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/simulator/scenario/<scenario_name>', methods=['POST'])
def run_scenario(scenario_name):
    """
    Trigger a specific anomaly scenario.

    Available scenarios:
    - lag_spike: Rapidly increases consumer lag
    - consumer_slow: Simulates degraded consumer performance
    - rebalance_storm: Triggers repeated rebalances
    - hot_partition: Creates partition skew
    """
    global active_scenario

    if scenario_name not in scenarios:
        return jsonify({
            "error": f"Unknown scenario: {scenario_name}",
            "available_scenarios": list(scenarios.keys())
        }), 404

    with scenario_lock:
        if active_scenario:
            return jsonify({
                "error": f"Scenario already running: {active_scenario}",
                "message": "Wait for current scenario to complete or stop it first"
            }), 409

        active_scenario = scenario_name

    # Get scenario parameters from request
    data = request.json or {}
    consumer_group = data.get('consumer_group', 'checkout-service')
    target_topic = data.get('target_topic', 'orders')

    # Run scenario in background thread
    def run_in_background():
        global active_scenario
        try:
            scenario = scenarios[scenario_name]
            scenario_id = scenario.run(
                consumer_group=consumer_group,
                target_topic=target_topic,
                **data
            )
            logger.info(f"Scenario '{scenario_name}' completed: {scenario_id}")
        except Exception as e:
            logger.error(f"Scenario '{scenario_name}' failed: {e}", exc_info=True)
        finally:
            with scenario_lock:
                active_scenario = None

    thread = threading.Thread(target=run_in_background, daemon=True)
    thread.start()

    scenario = scenarios[scenario_name]
    return jsonify({
        "status": "started",
        "scenario": scenario_name,
        "description": scenario.description,
        "duration_seconds": scenario.duration_seconds,
        "consumer_group": consumer_group,
        "target_topic": target_topic
    }), 202


@app.route('/simulator/stop', methods=['POST'])
def stop_all():
    """Stop all simulator activity"""
    global active_scenario

    free_producer.stop()

    with scenario_lock:
        active_scenario = None

    return jsonify({"status": "all_stopped"}), 200


if __name__ == '__main__':
    init_producer()

    port = int(os.getenv('SIMULATOR_PORT', 5001))
    host = os.getenv('SIMULATOR_HOST', '0.0.0.0')

    logger.info(f"Starting simulator API on {host}:{port}")
    app.run(host=host, port=port, debug=False)
