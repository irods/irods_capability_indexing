# iRODS Capability - Indexing

## Motivation

The iRODS indexing capability provides a policy framework around both full text and metadata indexing for the purposes of enhanced data discovery. Logical collections are annotated with metadata which indicates that any data objects or nested collections of data object should be indexed given a particular indexing technology, index type and index name.

> [!IMPORTANT]
> This project supports elasticsearch 7.0.0 and later. As a result, the document-type rule engine plugin _(i.e. irods-rule-engine-plugin-document-type)_ is now obsolete and has been removed. Users upgrading to 4.3.1 will need to remove all references to the document-type rule engine plugin from their server_config.json file. Failing to do so may result in server errors and/or servers not being able to service client requests.

## Build Dependencies

- iRODS development package
- iRODS externals package for boost
- iRODS externals package for fmt
- iRODS externals package for nlohmann-json
- iRODS externals package for spdlog
- OpenSSL development package

## Building

To build, follow the normal CMake steps.

```bash
mkdir build # Preferably outside of the repository.
cd build
cmake /path/to/repository
make package # Pass -j to use more parallelism.
```

## Configuration

### Collection Metadata

Collections are annotated with metadata indicating they should be indexed. The metadata is formatted as follows:
```
irods::indexing::index <index_name>::<index_type> <technology>
```
Where `<index_name>` is an assumed existing index within the given technology, and `<index_type>` is either `full_text`, meaning the data object will be read, processed and then submitted to the index, or `metadata` where the metadata triples associated with qualifying data objects will be indexed.

The `<technology>` in the triple references the indexing technology, currently only suppored by `elasticsearch`. This string is used to dynamically build the policy invocations when the indexing policy is triggered in order to delegate the operations to the appropriate rule engine plugin.

The attribute is configurable within the `plugin_specific_configuration` for the indexing rule engine plugin.

### Resource Metadata

An administrator may wish to restrict indexing activities to particular resources, for example when automatically ingesting data. Should a storage resource be at the edge, that resource may not be appropriate for indexing. In order to indicate a resource is available for indexing it may be annotated with metadata:
```
imeta add -R <resource_name> irods::indexing::index true
```
By default, should no resource be tagged it is assumed that all resources are available for indexing. Should the tag exist on any resource in the system, it is assumed that all available resources for indexing are tagged.

### Enabling the Indexing Capability Plugins

To enable the Indexing capability, prepend the following plugin configuration to the list of rule engines in `/etc/irods/server_config.json`. Plugin-specific configuration options are explained below.

> [!IMPORTANT]
> The comments in the JSON structure are for explanatory purposes and must not be included in your configuration. Failing to follow this requirement will result in the server failing to stand up.

```js
"rule_engines": [
    {
        "instance_name": "irods_rule_engine_plugin-indexing-instance",
        "plugin_name": "irods_rule_engine_plugin-indexing",
        "plugin_specific_configuration": {
            // The lower limit for randomly generated delay task intervals.
            "minimum_delay_time": 1,

            // The upper limit for randomly generated delay task intervals.
            "maximum_delay_time": 30,

            // The maximum number of delay rules allowed to be scheduled for
            // a particular collection at a time.
            //
            // If set to 0, the limit is disabled.
            "job_limit_per_collection_indexing_operation": 1000
        }
    },
    {
        "instance_name": "irods_rule_engine_plugin-elasticsearch-instance",
        "plugin_name": "irods_rule_engine_plugin-elasticsearch",
        "plugin_specific_configuration": {
            // The list of URLs identifying the elasticsearch service.
            //
            // Important things to keep in mind:
            //
            //   - URLs must contain the port number
            //   - If TLS communication is desired, the URL must begin with "https"
            "hosts": [
                "http://localhost:9200"
            ],

            // The number of text chunks processed at once for elasticsearch
            // full-text indexing.
            "bulk_count": 100,

            // The size of an individual text chunk for elasticsearch full-text
            // indexing.
            "read_size": 4194304,

            // The absolute path to a TLS certificate used for secure communication
            // with elasticsearch. If empty, OS-dependent default paths are used for
            // certificates verification.
            //
            // This option only takes effect for host entries beginning with "https".
            "tls_certificate_file": "",

            // The encoded basic authentication credentials for elasticsearch. The
            // value must match one of the following:
            //
            //   - base64_encode(url_encode(username) + ":" + url_encode(password))
            //   - base64_encode(username + ":" + password)
            //
            // This option is not used when empty. Recommended when using TLS, but
            // not required.
            "authorization_basic_credentials": ""
        }
    },

    // ... Previously installed rule engine plugin configs ...
]
```

Currently, due to 32-bit limitations on many architectures, integer-type parameters should not exceed a value of INT_MAX = 2^31 - 1 = 2147483647 or the results may be undefined.

Modifications to the plugin configuration will require a reload or restart of the server to take effect.

## Policy Implementation

Policy names are are dynamically crafted by the indexing plugin in order to invoke a particular technology. The four policies an indexing technology must implement are crafted from base strings with the name of the technology as indicated by the collection metadata annotation.

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
   - Download, untar, and run the ElasticSearch package or equivalent:
     ```
     elasticsearch-8.12.2/bin/elasticsearch -d -E discovery.type=single-node
     ```
     (Note, a kernel parameter change may be necessary: `sysctl -w vm.max_map_count=262144` .)
   - To signal all tests should be run, even those requiring the python-irodsclient:
      * `Bash> export MANUALLY_TEST_INDEXING_PLUGIN=1`
      * install Python3 with the pip package.
   - Run the following as the service account user:
     ```
     cd ~/scripts ; python run_tests.py --run_s test_plugin_indexing.TestIndexingPlugin
     ```
