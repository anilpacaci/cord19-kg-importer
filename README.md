# cord19-kg-importer

Neo4j importer script for bulk loading [BLENDER LAB Coronavirus Knowledge Graph](http://blender.cs.illinois.edu/covid19/)

### Steps

1. Download the Covid-19 Knowledge Graph from [here](http://159.89.180.81/demo/covid/KG.zip)

2. The provided script uses `neo4j-admin` tool for bulk loading, therefore, a local Neo4j installation is requited. Download Neo4j Community Edition v3.5.17 (or above)

3. Set following configuration parameters in `kg.conf`
    1. `KG_HOME`: the absolute path of the extracted knowledge path
    2. `NEO4J_HOME`: the absolute path of the local Neo4j installation

4. Make sure that the local Neo4j installation does not contain any data. Existing databases can be deleted:

``` $ rm -rf [NEO4J_HOME]/data/databases/graph.db/```

5. `importer.sh` uses `awk`, make sure `awk` is installed on the system.

```$ sudo apt-get install gawk```

6. Run the import script

``` $ ./importer.sh ```

7. Errors that occur during bulk loading are logged in `import.report`

8. Finally run the Neo4j server from the Neo4j home directory. Neo4j browser should be avaiable at <http://localhost:7474/browser/>

```
$ bin/neo4j console
```

### Schema

The schema of the knowledge graph created using the [Arrows Tool](http://www.apcjones.com/arrows/#):

![Schema](schema.svg)
