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
                "plugin_specific_configuration": {
                }
            },
            {
                "instance_name": "irods_rule_engine_plugin-elasticsearch-instance",
                "plugin_name": "irods_rule_engine_plugin-elasticsearch",
                "plugin_specific_configuration": {
                    "hosts" : ["http://localhost:9200/"],
                    "bulk_count" : 100,
                    "read_size" : 4194304
                }
            },
            {
                "instance_name": "irods_rule_engine_plugin-document_type-instance",
                "plugin_name": "irods_rule_engine_plugin-document_type",
                "plugin_specific_configuration": {
                }
            },
        ]
```
The first is the main indexing rule engine plugin, the second is the plugin responsible for implementing the policy for the indexing technology, and the third is responsible for implementing the document type introspection.  Currently the default imply returns `text` as the document type.  This policy can be overridden to call out to services like Tika for a better introspeciton of the data.

# Policy Implementation

Policy names are are dynamically crafted by the indexing plugin in order to invoke a particular technology.  The four policies an indexing technology must implement are crafted from base strings with the name of the technology as indicated by the collection metadata annotation.

### Indexing Technology Policies
```
irods_policy_indexing_object_index_<technology>
irods_policy_indexing_object_purge_<technology>
irods_policy_indexing_metadata_index_<technology>
irods_policy_indexing_metadata_purge_<technology>
```

### Document Type Policy

```
irods_policy_indexing_document_type_<technology>
```
