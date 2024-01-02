import argparse


parser = argparse.ArgumentParser(prog='Dataset generator')
parser.add_argument('--output_topic', required=True)
parser.add_argument('--broker', required=True)
args = parser.parse_args()
