# This script categorizes the user agent strings from the nu.nl logs into distinct categories based on device type.
import argparse
import csv
from device_detector import DeviceDetector


def create_device_type_mapping(input_file_path, output_file_path):
    with open(input_file_path, "r") as input_file, open(output_file_path, "w") as output_file:
        writer = csv.writer(output_file)
        for line in input_file:
            device = DeviceDetector(line).parse()
            device_type = device.device_type()
            writer.writerow((line.rstrip(), device_type))
    print("User agent mapping complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file_path', dest='input_file_path', default='user_agents.txt', help='input file to process')
    parser.add_argument('--output_file_path', dest='output_file_path', default='user_agents_map.csv', help='output file to write to')
    args = parser.parse_args()
    create_device_type_mapping(args.input_file_path, args.output_file_path)
