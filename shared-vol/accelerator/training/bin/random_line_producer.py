import argparse
import random
import time


def print_lines(filepath, kafka_topic=None):
    pr = print
    producer = None
    
    if kafka_topic:
        from kafka import KafkaProducer
        producer = KafkaProducer(bootstrap_servers="kafka:9092")
        pr = lambda line: producer.send(kafka_topic, line.encode('utf-8'))
    
    try:
        for line in generate_lines(filepath):
            pr(line)
            time.sleep(0.2)
    finally:
        if producer:
            producer.close()


def generate_lines(filepath):
    lines = open(filepath).read().splitlines()
    while True:
        random_line = random.choice(lines)
        yield random_line


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Keep printing random lines.')
    parser.add_argument('--filepath', type=str, help='Path to file')
    parser.add_argument('--kafka_topic', type=str, help='Kafka topic')
    args = parser.parse_args()
    print_lines(args.filepath, kafka_topic=args.kafka_topic)
