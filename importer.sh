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
# create the impor directory
mkdir -p ${KG_IMPORT_DIR}

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


# invoke neo4j-admin import tool
invoke_import()
{
	printf "\\n\\n ===> Invoke Neo4j Import Tool"
	printf "\\n"

	${NEO4J_HOME}/bin/neo4j-admin import --delimiter=${CSV_FIELD_SEPERATOR} --array-delimiter=${CSV_PROPERTY_SEPERATOR} \
	--ignore-missing-nodes true \
	--nodes:CHEMICAL ${KG_IMPORT_DIR}/chemical-header.csv,${KG_HOME}/chemicals.csv \
	--nodes:DISEASE ${KG_IMPORT_DIR}/disease-header.csv,${KG_HOME}/diseases.csv \
	--nodes:GENE ${KG_IMPORT_DIR}/gene-header.csv,${KG_HOME}/genes.csv \
	--relationships:CHEMICAL_DISEASE_IXN ${KG_IMPORT_DIR}/chemical-disease-header.csv,${KG_HOME}/chemicals_diseases_relation.csv \
	--relationships:GENE_DISEASE_IXN ${KG_IMPORT_DIR}/gene-disease-header.csv,${KG_HOME}/genes_diseases_relation.csv \
	--relationships:CHEMICAL_GENE_IXN ${KG_IMPORT_DIR}/chemical-gene-header.csv,${KG_IMPORT_DIR}/chem_gene_ixns_relation_updated.csv \
	--relationships:PARENT ${KG_IMPORT_DIR}/disease-parent-header.csv,${KG_IMPORT_DIR}/disease-parent.csv \
	--relationships:PARENT ${KG_IMPORT_DIR}/chemical-parent-header.csv,${KG_IMPORT_DIR}/chemical-parent.csv
}

#chem_gene_ixns require prefix MESH on each chemical
awk -F '\t' 'BEGIN{OFS="\t"}  {if(NR>1) $1="MESH:"$1; print }' ${KG_HOME}/chem_gene_ixns_relation.csv > ${KG_IMPORT_DIR}/chem_gene_ixns_relation_updated.csv

# flatten parent relationships
flatten_parent ${KG_HOME}/diseases.csv ${KG_IMPORT_DIR}/disease-parent.csv 2 4
flatten_parent ${KG_HOME}/chemicals.csv ${KG_IMPORT_DIR}/chemical-parent.csv 2 3

# create header file for each node type
write_csv ${KG_IMPORT_DIR}/disease-header.csv 'DiseaseName\tDiseaseID:ID(Disease-ID)\tAltDiseaseIDs\t:IGNORE'
write_csv ${KG_IMPORT_DIR}/chemical-header.csv 'ChemicalName\tChemicalID:ID(Chemical-ID)\t:IGNORE'
write_csv ${KG_IMPORT_DIR}/gene-header.csv 'GeneSymbol\tGeneName\tGeneID:ID(Gene-ID)\tAltGeneIDs'
#write_csv ${KG_IMPORT_DIR}/organism-header.csv ':IGNORE\t:IGNORE\tOrganism\tOrganismID:ID(Organism-ID)\t:IGNORE\t:IGNORE\t:IGNORE'
#write_csv ${KG_IMPORT_DIR}/interaction-header.csv ':IGNORE\t:IGNORE\tOrganism\t:IGNORE\tInteraction\tInterationActions\pmids'

# create header file for each edge type
write_csv ${KG_IMPORT_DIR}/chemical-disease-header.csv ':START_ID(Chemical-ID)\t:END_ID(Disease-ID)\tpmids'
write_csv ${KG_IMPORT_DIR}/gene-disease-header.csv ':START_ID(Gene-ID)\t:END_ID(Disease-ID)\tpmids'
write_csv ${KG_IMPORT_DIR}/chemical-gene-header.csv ':START_ID(Chemical-ID)\t:END_ID(Gene-ID)\tOrganism\tOrganismID\tInteraction\tInterationActions\tpmids'
# create header file for each parent relationship
write_csv ${KG_IMPORT_DIR}/disease-parent-header.csv ':START_ID(Disease-ID)\t:END_ID(Disease-ID)'
write_csv ${KG_IMPORT_DIR}/chemical-parent-header.csv ':START_ID(Chemical-ID)\t:END_ID(Chemical-ID)'

invoke_import
