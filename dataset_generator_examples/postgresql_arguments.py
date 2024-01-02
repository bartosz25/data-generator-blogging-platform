import argparse


parser = argparse.ArgumentParser(prog='Dataset generator')
parser.add_argument('--host', required=True)
parser.add_argument('--dbname', required=True)
parser.add_argument('--user', required=True)
parser.add_argument('--password', required=True)
parser.add_argument('--table_name', required=True)
args = parser.parse_args()
