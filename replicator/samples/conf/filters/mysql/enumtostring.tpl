# Transform enum's int to string before it reaches THL.
replicator.filter.enumtostring=com.continuent.tungsten.replicator.filter.EnumToStringFilter
replicator.filter.enumtostring.url=jdbc:mysql:thin://${replicator.global.db.host}:${replicator.global.db.port}/tungsten_${service.name}