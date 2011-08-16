# Server ID filter.  This filter drops SQL statements from one or more MySQL
# server IDs, which are given as a comma-separated list.
replicator.filter.ignoreserver=com.continuent.tungsten.replicator.filter.JavaScriptFilter
replicator.filter.ignoreserver.script=${replicator.home.dir}/samples/scripts/javascript-advanced/ignore_server_id.js
replicator.filter.ignoreserver.server_ids=3,7