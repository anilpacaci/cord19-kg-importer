#!/bin/bash

# default parameter in case importer.conf does not exist
NEO4J_HOME="/tuna1/scratch/apacaci/neo4j-community-3.5.17"

KG_HOME="/tuna1/scratch/apacaci/KG"

CSV_FIELD_SEPERATOR="\t"
CSV_PROPERTY_SEPERATOR="|"

# include conf file if it is defined
if [ -f ./kg.conf ]; then
    . kg.conf
fi

# set variables to be used in the script
KG_IMPORT_DIR=${KG_HOME}/import

# input files
CHEM_GENE_IXNS_RELATION=${KG_HOME}/chem_gene_ixns_relation.csv

# helper function to write given string into a file
# arg1: filename
# arg2: input string 
write_csv()
{
	local FILENAME=$1
	local HEADER="$2"
	printf "\\n\\n ===> Write CSV"
	printf "\\n"	

	if [ $# -lt 2 ] ; then
    		echo "Supply file name and header string"
    		exit 1
    	fi


	mkdir -p ${KG_IMPORT_DIR}

	echo "echo -e ${HEADER} > ${FILENAME} "
	echo -e ${HEADER} > ${FILENAME}
}

# utility function to flatten parent relationship 
flatten_parent()
{
	local FILENAME=$1
	local PARENT_FILENAME=$2
	local SOURCE_FIELD=$3
	local PARENT_FIELD=$4

	printf "\\n\\n ===> Flatten parent relationships"
	printf "\\n"

	if [ $# -lt 4 ] ; then
    		echo "Supply input and output file names, source and parent column numbers"
    		exit 1
    	fi

   	awk -F '\t' 'BEGIN{OFS="\t"} {len = split($'$PARENT_FIELD', parents, "|");   for(i=1; i <= len; i++ ) print $'$SOURCE_FIELD', parents[i]  }' \
   		$FILENAME > $PARENT_FILENAME

}

# utility funtion to flatten pmids for chemical_disease and gene_disease relationships
# assumes that the input file has three fields, source, target and '|' seperated array of pmids
flatten_disease_pmid()
{
	local INPUT_FILENAME=$1
        local OUTPUT_FILENAME=$2

	printf "\\n\\n ===> Flatten pmids %s" $INPUT_FILENAME
        printf "\\n"
	
	if [ $# -lt 2 ] ; then
                echo "Supply input and output file names"
                exit 1
        fi

	awk -F '\t' 'BEGIN{OFS="\t"} {len = split($3, pmids, "|");   for(i=1; i <= len; i++ ) print $1, $2, pmids[i]  }' \
                $INPUT_FILENAME > $OUTPUT_FILENAME
}

# utility funtion to eliminate duplicate lines in a given file
dedup_lines()
{
	local INPUT_FILENAME=$1

        printf "\\n\\n ===> Remove duplicates from %s" $INPUT_FILENAME
        printf "\\n"

        if [ $# -lt 1 ] ; then
                echo "Supply input fille name"
                exit 1
        fi

	sort $INPUT_FILENAME | uniq > $INPUT_FILENAME.temp
	mv $INPUT_FILENAME.temp $INPUT_FILENAME
}

# Utility function to create node files from a ternary relation
# arg1: input filename
# arg2: output filename for neo4j nodes
# arg3, arg4: two keys for the ternary relation which will be used to generate node id
ternary_to_node()
{
	local INPUT_FILENAME=$1
	local OUTPUT_FILENAME=$2
        local FIELD1=$3
        local FIELD2=$4

        printf "\\n\\n ===> Generate node file from ternary relation in %s" $INPUT_FILENAME
        printf "\\n"

        if [ $# -lt 5 ] ; then
                echo "Supply input and output file names, and three column numbers of the ternary relation"
                exit 1
        fi

	# generate the node file 
	awk -F '\t' 'BEGIN{OFS="\t"} {print $'$FIELD1'"-"$'$FIELD2'}' $INPUT_FILENAME > $OUTPUT_FILENAME
	
	# eliminate duplicates in the node file 
	dedup_lines $OUTPUT_FILENAME
}

# Utility function to create node files from a ternary relation
# arg1: input filename
# arg2: output filename for neo4j nodes
# arg3,4: two keys for the ternary relation which will be used to generate node id
# arg5: target field for the binary relation
ternary_to_binary()
{
        local INPUT_FILENAME=$1
        local OUTPUT_FILENAME=$2
        local FIELD1=$3
        local FIELD2=$4
	local TARGET_FIELD=$5

        printf "\\n\\n ===> Generate edge file from ternary relation in %s using column %s" $INPUT_FILENAME $SOURCE_FIELD $TARGET_FIELD
        printf "\\n"

        if [ $# -lt 5 ] ; then
                echo "Supply input and output file names, and three column numbers of the ternary relation"
                exit 1
        fi

        # generate the node file 
        awk -F '\t' 'BEGIN{OFS="\t"} {print $'$FIELD1'"-"$'$FIELD2', $'$TARGET_FIELD'}' $INPUT_FILENAME > $OUTPUT_FILENAME
	
	# eliminate duplicates in the node file 
        dedup_lines $OUTPUT_FILENAME
}


# Utility function to perform janitorial task before import
# 1. prepare temp directories
# 2. update chemical IDs in chem-gene relation
setup()
{
	# create the impor directory
        mkdir -p ${KG_IMPORT_DIR}
	#chem_gene_ixns require prefix MESH on each chemical
	awk -F '\t' 'BEGIN{OFS="\t"}  {if(NR>1) $1="MESH:"$1; print }' ${KG_HOME}/chem_gene_ixns_relation.csv > ${KG_IMPORT_DIR}/chem_gene_ixns_relation_updated.csv
	# generate publication data from pmc entities
	python3 pmc_import.py -i ${KG_HOME}/pmcid ${KG_HOME}/pmid_abs -o  ${KG_IMPORT_DIR}/pm-entity.csv
}

flatten_arrays()
{
	# flatten parent relationships
	flatten_parent ${KG_HOME}/diseases.csv ${KG_IMPORT_DIR}/disease-parent.csv 2 4
	flatten_parent ${KG_HOME}/chemicals.csv ${KG_IMPORT_DIR}/chemical-parent.csv 2 3

	# flatten disease header files
	flatten_disease_pmid ${KG_HOME}/chemical_diseases_relation.csv ${KG_IMPORT_DIR}/chemical_disease_pmid.csv
	flatten_disease_pmid ${KG_HOME}/genes_diseases_relation.csv ${KG_IMPORT_DIR}/genes_disease_pmid.csv
	# remove duplicate lines
	dedup_lines ${KG_IMPORT_DIR}/chemical_disease_pmid.csv
	dedup_lines ${KG_IMPORT_DIR}/genes_disease_pmid.csv
}

ternary_normalize()
{
	#create nodefiles from each ternary
        ternary_to_node ${KG_IMPORT_DIR}/chemical_disease_pmid.csv ${KG_IMPORT_DIR}/chemical_disease_pmid_node.csv 1 2
        ternary_to_node ${KG_IMPORT_DIR}/genes_disease_pmid.csv ${KG_IMPORT_DIR}/genes_disease_pmid_node.csv 1 2

	# create edge files from each ternary
	ternary_to_binary ${KG_IMPORT_DIR}/chemical_disease_pmid.csv ${KG_IMPORT_DIR}/chemical_disease_pmid_chemical.csv 1 2 1
	ternary_to_binary ${KG_IMPORT_DIR}/chemical_disease_pmid.csv ${KG_IMPORT_DIR}/chemical_disease_pmid_disease.csv 1 2 2
	ternary_to_binary ${KG_IMPORT_DIR}/chemical_disease_pmid.csv ${KG_IMPORT_DIR}/chemical_disease_pmid_pmid.csv 1 2 3
	ternary_to_binary ${KG_IMPORT_DIR}/genes_disease_pmid.csv ${KG_IMPORT_DIR}/genes_disease_pmid_gene.csv 1 2 1
	ternary_to_binary ${KG_IMPORT_DIR}/genes_disease_pmid.csv ${KG_IMPORT_DIR}/genes_disease_pmid_disease.csv 1 2 2
	ternary_to_binary ${KG_IMPORT_DIR}/genes_disease_pmid.csv ${KG_IMPORT_DIR}/genes_disease_pmid_pmid.csv 1 2 3
}

# helper to create all header
create_headers()
{
	# create header file for each node type
	write_csv ${KG_IMPORT_DIR}/disease-header.csv 'DiseaseName\tDiseaseID:ID(Disease-ID)\tAltDiseaseIDs:string[]\t:IGNORE'
	write_csv ${KG_IMPORT_DIR}/chemical-header.csv 'ChemicalName\tChemicalID:ID(Chemical-ID)\t:IGNORE'
	write_csv ${KG_IMPORT_DIR}/gene-header.csv 'GeneSymbol\tGeneName\tGeneID:ID(Gene-ID)\tAltGeneIDs:string[]'
	#write_csv ${KG_IMPORT_DIR}/organism-header.csv ':IGNORE\t:IGNORE\tOrganism\tOrganismID:ID(Organism-ID)\t:IGNORE\t:IGNORE\t:IGNORE'
	#write_csv ${KG_IMPORT_DIR}/interaction-header.csv ':IGNORE\t:IGNORE\tOrganism\t:IGNORE\tInteraction\tInterationActions\pmids'
	
	# create header for fact nodes for ternary relations
	write_csv ${KG_IMPORT_DIR}/chemical-disease-fact-header.csv 'FactID:ID(Fact-ID)'
	write_csv ${KG_IMPORT_DIR}/genes-disease-fact-header.csv 'FactID:ID(Fact-ID)'
	write_csv ${KG_IMPORT_DIR}/pm-header.csv 'PMID:ID(PM-ID)\tPMCID\tTitle\tJournal\tAuthors:string[]\tYear'

	# create header for ternary relation edges
	write_csv ${KG_IMPORT_DIR}/chemical-disease-fact-chemical-header.csv ':START_ID(Fact-ID)\t:END_ID(Chemical-ID)'
	write_csv ${KG_IMPORT_DIR}/chemical-disease-fact-disease-header.csv ':START_ID(Fact-ID)\t:END_ID(Disease-ID)'
	write_csv ${KG_IMPORT_DIR}/chemical-disease-fact-pmid-header.csv ':START_ID(Fact-ID)\t:END_ID(PM-ID)'
	write_csv ${KG_IMPORT_DIR}/genes-disease-fact-gene-header.csv ':START_ID(Fact-ID)\t:END_ID(Gene-ID)'
        write_csv ${KG_IMPORT_DIR}/genes-disease-fact-disease-header.csv ':START_ID(Fact-ID)\t:END_ID(Disease-ID)'
        write_csv ${KG_IMPORT_DIR}/genes-disease-fact-pmid-header.csv ':START_ID(Fact-ID)\t:END_ID(PM-ID)'	

	# create header file for each edge type
	#write_csv ${KG_IMPORT_DIR}/chemical-disease-header.csv ':START_ID(Chemical-ID)\t:END_ID(Disease-ID)\tpmids'
	#write_csv ${KG_IMPORT_DIR}/gene-disease-header.csv ':START_ID(Gene-ID)\t:END_ID(Disease-ID)\tpmids'
	write_csv ${KG_IMPORT_DIR}/chemical-gene-header.csv ':START_ID(Chemical-ID)\t:END_ID(Gene-ID)\tOrganism\tOrganismID\tInteraction\tInterationActions\tpmids:string[]'
	# create header file for each parent relationship
	write_csv ${KG_IMPORT_DIR}/disease-parent-header.csv ':START_ID(Disease-ID)\t:END_ID(Disease-ID)'
	write_csv ${KG_IMPORT_DIR}/chemical-parent-header.csv ':START_ID(Chemical-ID)\t:END_ID(Chemical-ID)'
}

# invoke neo4j-admin import tool
invoke_import()
{
        printf "\\n\\n ===> Invoke Neo4j Import Tool"
        printf "\\n"

        ${NEO4J_HOME}/bin/neo4j-admin import --delimiter=${CSV_FIELD_SEPERATOR} --array-delimiter=${CSV_PROPERTY_SEPERATOR} \
        --ignore-missing-nodes true \
        --ignore-duplicate-nodes true \
        --nodes:CHEMICAL ${KG_IMPORT_DIR}/chemical-header.csv,${KG_HOME}/chemicals.csv \
        --nodes:DISEASE ${KG_IMPORT_DIR}/disease-header.csv,${KG_HOME}/diseases.csv \
        --nodes:GENE ${KG_IMPORT_DIR}/gene-header.csv,${KG_HOME}/genes.csv \
        --nodes:FACT ${KG_IMPORT_DIR}/chemical-disease-fact-header.csv,${KG_IMPORT_DIR}/chemical_disease_pmid_node.csv \
        --nodes:FACT ${KG_IMPORT_DIR}/genes-disease-fact-header.csv,${KG_IMPORT_DIR}/genes_disease_pmid_node.csv \
	--nodes:PUBLICATION ${KG_IMPORT_DIR}/pm-header.csv,${KG_IMPORT_DIR}/pm-entity.csv,${KG_IMPORT_DIR}/pm-id.csv \
	--relationships:FACT_CHEMICAL ${KG_IMPORT_DIR}/chemical-disease-fact-chemical-header.csv,${KG_IMPORT_DIR}/chemical_disease_pmid_chemical.csv \
	--relationships:FACT_DISEASE ${KG_IMPORT_DIR}/chemical-disease-fact-disease-header.csv,${KG_IMPORT_DIR}/chemical_disease_pmid_disease.csv \
	--relationships:FACT_PUBLICATION ${KG_IMPORT_DIR}/chemical-disease-fact-pmid-header.csv,${KG_IMPORT_DIR}/chemical_disease_pmid_pmid.csv \
	--relationships:FACT_GENE ${KG_IMPORT_DIR}/genes-disease-fact-gene-header.csv,${KG_IMPORT_DIR}/genes_disease_pmid_gene.csv \
        --relationships:FACT_DISEASE ${KG_IMPORT_DIR}/genes-disease-fact-disease-header.csv,${KG_IMPORT_DIR}/genes_disease_pmid_disease.csv \
        --relationships:FACT_PUBLICATION ${KG_IMPORT_DIR}/genes-disease-fact-pmid-header.csv,${KG_IMPORT_DIR}/genes_disease_pmid_pmid.csv \
        --relationships:CHEMICAL_GENE_IXN ${KG_IMPORT_DIR}/chemical-gene-header.csv,${KG_IMPORT_DIR}/chem_gene_ixns_relation_updated.csv \
        --relationships:PARENT ${KG_IMPORT_DIR}/disease-parent-header.csv,${KG_IMPORT_DIR}/disease-parent.csv \
        --relationships:PARENT ${KG_IMPORT_DIR}/chemical-parent-header.csv,${KG_IMPORT_DIR}/chemical-parent.csv


##  --relationships:CHEMICAL_DISEASE_IXN ${KG_IMPORT_DIR}/chemical-disease-header.csv,${KG_HOME}/chemicals_diseases_relation.csv \
##  --relationships:GENE_DISEASE_IXN ${KG_IMPORT_DIR}/gene-disease-header.csv,${KG_HOME}/genes_diseases_relation.csv \
}

# first perform setup
setup
# flatten pmid and parent multivalued properties
flatten_arrays
# normalize ternary disease and gene relationships
ternary_normalize
# create headers required by Neo4j import
create_headers
# invoke Neo4j import tool
invoke_import
