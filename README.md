# Motivation
The iRODS indexing capability provides a policy framework around both full text and metadata indexing for the purposes of enhanced data discovery.  Logical collections are annotated with metadata which indicates that any data objects or nested collections of data object should be indexed given a particular indexing technology, index type and index name.

# Configuration
### Collection Metadata

Collections are annotated with metadata indicating they should be indexed.  The metadata is formatted is as follows:
```
irods::indexing::index <index name>::<index type> <technology>
```
Where `<index name>` is an assumed existing index within the given technology, and `<index type>` is either `full_text`, meaning the data object will be read, processed and then submitted to the index, or `metadata` where the metadata triples associated with qualifying data objects will be indexed.

The `<technology>` in the triple references the indexing technology, currently only suppored by `elasticsearch`.  This string is used to dynamically build the policy invocations when the indexing policy is triggered in order to delegate the operations to the appropriate rule engine plugin.

The attribute is configurable within the `plugin_specific_configuration` for the indexing rule engine plugin.

### Resource Metadata

An administrator may wish to restrict indexing activities to particular resources, for example when automatically ingesting data.  Should a storage resource be at the edge, that resource may not be appropriate for indexing.  In order to indicate a resource is available for indexing it may be annotated with metadata:
```
imeta add -R <resource name> irods::indexing::index true
```
By default, should no resource be tagged it is assumed that all resources are available for indexing.  Should the tag exist on any resource in the system, it is assumed that all available resources for indexing are tagged.

### Plugin Settings

There are currently three rule engine plugins to configure for the indexing capability which should be added to the `"rule_engines"` section of `/etc/irods/server_config.json`:

```
"rule_engines": [
    {
        "instance_name": "irods_rule_engine_plugin-indexing-instance",
        "plugin_name": "irods_rule_engine_plugin-indexing",
        "plugin_specific_configuration": {}
    },
    {
        "instance_name": "irods_rule_engine_plugin-elasticsearch-instance",
        "plugin_name": "irods_rule_engine_plugin-elasticsearch",
        "plugin_specific_configuration": {
            "hosts": ["http://localhost:9200/"],
            "bulk_count": 100,
            "read_size": 4194304
        }
    }
]
```
The first is the main indexing rule engine plugin and the second is the plugin responsible for implementing the policy for the indexing technology.

Within each plugin configuration stanza, the "plugin_specific_configuration" object may contain a number of key-value pairs.  The following pairs are currently applicable for the purpose of setting the indexing capability's operating parameters:

| key                                         |      type     | plugin component | default |  purpose                                                                   |
|---------------------------------------------|---------------|------------------|---------|----------------------------------------------------------------------------|
| minimum_delay_time                          | int or string |  indexing        |       1 | lower limit for randomly generated delay-task intervals                    |
| maximum_delay_time                          | int or string |  indexing        |      30 | upper limit for randomly generated delay-task intervals                    |
| job_limit_per_collection_indexing_operation | int or string |  indexing        |    1000 | integer limit to number of concurrent collection operations (0 = no limit) |
| es_version                                  | string        |  elasticsearch   |   "7.x" | set to "6.x" or "7.x" depending on Elasticsearch version                   |
| bulk_count                                  | int           |  elasticsearch   |      10 | the number of text chunks processed at once for ES full-text indexing      |
| read_size                                   | int           |  elasticsearch   | 4194304 | the size of individual text chunks processed for ES full-text indexing     |

Currently, due to 32-bit limitations on many architectures, int-type parameters should not exceed a value of INT_MAX = 2^31 - 1 = 2147483647 or the results may
be undefined.

# Policy Implementation

Policy names are are dynamically crafted by the indexing plugin in order to invoke a particular technology.  The four policies an indexing technology must implement are crafted from base strings with the name of the technology as indicated by the collection metadata annotation.

### Indexing Technology Policies
```
irods_policy_indexing_object_index_<technology>
irods_policy_indexing_object_purge_<technology>
irods_policy_indexing_metadata_index_<technology>
irods_policy_indexing_metadata_purge_<technology>
```

### Plugin Testing

Caveats:
   - start from a baseline configuration with no indexing plugins installed.

Prerequisites:
   - Install a Java 8 JRE or JDK (OpenJDK versions will suffice).
   - Download, untar, and run the ElasticSearch package or equivalent:
     ```
     elasticsearch-7.4.2/bin/elasticsearch --daemonize -E discovery.type=single-node -E http.port=9100
     ```
     (Note, a kernel parameter change may be necessary: `sysctl -w vm.max_map_count=262144` .)
   - To signal all tests should be run, even those requiring the python-irodsclient:
      * `Bash> export MANUALLY_TEST_INDEXING_PLUGIN=1`
      * install Python3 with the pip package.
   - Run the following as the service account user:
     ```
     cd ~/scripts ; python run_tests.py --run_s test_plugin_indexing.TestIndexingPlugin
     ```
