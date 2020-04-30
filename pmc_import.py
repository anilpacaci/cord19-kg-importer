import pandas as pd
import argparse
import os
import json

# parsing command line argument
parser = argparse.ArgumentParser("Preparing collection of PM JSON files for Neo4j Import")
parser.add_argument('-i', '--input_dir' , type=str, nargs='+', required=True, help="list of directories contain PMC collection")
parser.add_argument('-o', '--output_file', type=str, nargs=1, required=True, help='absolute path for output file')
args = parser.parse_args()

column_names = ["pmid", "pmcid", "title", "journal", "authors", "year"]
df = pd.DataFrame(columns = column_names)

# read each input directory and all files inside
for input_dir in args.input_dir:
	print('Reading files in directory: {}'.format(input_dir))
	files = [os.path.join(input_dir, file) for file in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, file))]
	
	# read each json file and populate the pandas data frame
	for file in files:
		with open(file) as pmc_json_file:
			pmc_data = json.load(pmc_json_file)
			# process only if publication has pmid
			if 'pmid' in pmc_data:
				pmc_entry = {}
				pmc_entry['pmid'] = pmc_data.get('pmid')
				pmc_entry['pmcid'] = pmc_data.get('pmcid')
				pmc_entry['title'] = pmc_data.get('title')
				pmc_entry['journal'] = pmc_data.get('journal')
				pmc_entry['year'] = pmc_data.get('year')
				pmc_entry['authors'] = '|'.join(pmc_data.get('authors'))
				# append data into the dataframe
				df = df.append(pmc_entry, ignore_index=True)
			else:
				print('File does not contain pmid:{}'.format(file))
# dump contents of the dataframe to output file
df.to_csv(args.output_file[0])
print('{} rows written into {}'.format(df.shape[0], args.output_file))