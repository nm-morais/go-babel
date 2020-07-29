# DeMMOn

## Core concepts:

* ### Tenant:

    * A Resource manager (ingests resource collections) and emits deployment configurations

* ###  Node:

    * A node belonging to any entity in any system

* ### Domain:

    * The domain in which the node is installed, i.e. AWS, Google, etc.

* ### Metric: 

    A metric consists in the raw measurement of a certain aspect of the system (i.e. resource usage, application response time, among others)

# API

<!-- * ### Add Domain -->
<!-- * ### Remove Domain -->

* ### Applications:

    * ### InstallApplication(AppName) -> bool

    * ### InstallComponent(ComponentName, NodeID) -> bool

* ## Metrics:

    * ### InstallMetrics([]MetricDescriptor) -> []Metrics 

    * ### SubscribeToMetrics(MetricDescriptor[]) -> []Metrics

* ## Alerts:

    * ### InstallAlert(AlertDescriptor) -> void

* ## Querying:

    * ### FindResources([]Filter) -> []Peers

    * ### QueryMetric(MetricName, queryParams) -> map[metricName]Values

* ## Routing:

    * ### FindPeer(peerID) Peer

    * ### FindResponsiblePeer(resourceName) Peer

# Built-in metrics

* ### Node metrics:

    * CPU
    * Memory
    * Disk space
    * Running components

* ### Network metrics

    * Connected peers
    * Bandwidth utilization
    * Latency pairs <s1,s2>

* ### Applicational metrics

    * Service status and availability

# Metric 

* ### Fields:

    * Aggregation method (Sum, Avg, Histogram, None)

    * Metric ID: an identifier for the collected metric (i.e. certificate hash / name)

    * Issuer (i.e. Application ID, tenant ID)

    * Domain (for restricting the emission of metrics)

# Ideas: 

## Agregation:

* Aggregation Certificates:

    * Advantages:

        * modular, easily programmable, easily stored, decouples the metric obtention from the metric itself (i.e. any programmer can program how the metrics are obtained) 

        * every node can dinamically compute the metrics even if they are not aware of what the metrics mean (or have pre-shared knowledge of how to compute them).

* push-pull probe:

    * A probe which emits a list of collected metrics to surrounding servers, and other servers pull relevant metrics 

* Push-Probe:

    * A probe which emits metrics to "responsible" servers (push approach), the control of the parent is delegated to the emiting node

## Topology:

* Connection bias
* If peers are interested in a subset of the metrics collected, then their links should bias towards the peers which are interested in those metrics 

