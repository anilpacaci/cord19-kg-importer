import pandas as pd
import numpy as np
import csv
import argparse
import os
import json

# parsing command line argument
parser = argparse.ArgumentParser("Normalize a hyperedge in a given ")
parser.add_argument('-i', '--input_file' , type=str, nargs='+', required=True, help="absolute path for the input file")
parser.add_argument('-o', '--output_dir', type=str, nargs=1, required=True, help='absolute path for output directory')
parser.add_argumment('-s','--source', type=int, nargs=1, required=True, help='source field name')
parser.add_argumment('-t','--target', type=int, nargs=1, required=True, help='target field name')
parser.add_argumment('-a','--array', type=int, nargs=1, required=True, help='source field name')
args = parser.parse_args()

header = []
nodes = []
source_edge = []
target_edge = []
array_edge = []

filename = os.path.basename(args.input_file)

with open(args.input_file) as input_file:
	ixnreader = csv.reader(input_file, delimiter='\t')
	# read header seperately
	headerstring = next(ixnreader)
	header = headerstring.split()
	# read input file line by line
	for ixn in ixnreader:
		# read three endpoints of the hyper edge
		source = ixn[args.source]
		target = ixn[args.target]
		array = ixn[args.array].split('|')

		# first create fact node and source and targets
		node_id = hash(np.array2string(np.delete(ixn, (args.array))))
		node_properties = np.delete(ixn, (args.source, args.target, args.array))
		nodes.append(np.insert(node_properties, 0, node_id).tolist())

		# create source and target edges
		source_edge.append([node_id, source])
		target_edge.append([node_id, target])

		# finally create array edges
		for array_element in array:
			array_edge.append([node_id, array_element])

# now write output files
with open(os.path.join(args.output_dir, "node-" + filename), 'w', delimiter='\t') as file:
	writer = csv.writer(file)
	# write header 
	nodeheader = np.delete(header, (args.source, args.target, args.array))
	writer.writerow(np.insert(nodeheader, 0, 'FactID').tolist())
	writer.writerows(nodes)

with open(os.path.join(args.output_dir, "source-" + filename), 'w', delimiter='\t') as file:
	writer = csv.writer(file)
	# write header 
	writer.writerow(['FactID', header[args.source]])
	writer.writerows(source_edge)

with open(os.path.join(args.output_dir, "target-" + filename), 'w', delimiter='\t') as file:
	writer = csv.writer(file)
	# write header 
	writer.writerow(['FactID', header[args.target]])
	writer.writerows(target_edge)

with open(os.path.join(args.output_dir, "array-" + filename), 'w', delimiter='\t') as file:
	writer = csv.writer(file)
	# write header 
	writer.writerow(['FactID', header[args.array]])
	writer.writerows(array_edge)

print("Normalizing {} is complete".format(args.input_file))