import argparse


parser = argparse.ArgumentParser(prog='Dataset generator')
parser.add_argument('--output_dir', required=True)
parser.add_argument('--partitions', required=True)
args = parser.parse_args()
