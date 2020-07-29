# DeMMOn :

## Core concepts:

* ### Metric: 

     * Fields:

        * Aggregation method (Sum, Avg, Histogram)

        * Metric ID: an identifier for the collected metric (i.e. certificate hash / name)

        * Issuer (i.e. Application ID, tenant ID)

        * Domain (for restricting the emission of metrics)


* ### Tenant:

    * A Resource manager (ingests resource collections) and emits deployment configurations

* ###  Node:

    * A node belonging to any entity in any system

* ### Domain:

    * The domain in which the node is installed, i.e. AWS, Google, etc.

* ### Node attributes:

*   Physical resources:

    * CPU
    * Memory
    * Disk

*   Logical resources:

    * connected peers
    * components running
    * Latency pairs <s1,s2>

## Libraries (running on cloud, edge servers, and clients)

* ### Cloud Library

    * Accept connections from clients
    * Has a generalized view of the system

    * API: 

* ### Edge Library

    * Accept connections from clients
    * Propagates information regarding pushed metrics
    * Performs aggregation

    * API:


* ### Client Library

    * Intercept requests between applications and servers and piggyback information
    * Possibly help on routing
    * Can be a router for the connected servers (possibly offering improvements)
    * Push information periodically to nearby servers

    * API:



## Management API

<!-- * ### Add Domain -->
<!-- * ### Remove Domain -->

* ### Metrics

    * InstallMetrics([]MetricDescriptor)

    * SubscribeMetrics(MetricDescriptor[])

* ### Alerts

    * InstallAlert(AlertDescriptor)

* ### Routing

    * FindPeer(peerID) Peer

    * FindResponsiblePeer(resourceName) Peer

    * FindResources([]Filter) []Peers

    * QueryMetrics(MetricName)


## Topology 

*   Bias topology according to metrics exposed (and latency??)
*   Levels

## Agregation

# Good ideas: 

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

* If peers are interested in a subset of the metrics collected, then their links should bias towards the peers which are interested in those metrics (i.e. tenants)



# Related work :

* ## Prometheus:

* Service discovery: 

    * File
    * Kubernetes

* metrics:

    * Exposed by an exporter (cAdvisor) and scraped using HTTP

* Scraping : 
    
    * Is done by the prometheus server using a pull approach

    * 

        

* ## Astrolabe

* ### API :

    * find_contacts(time, scope) - search for Astrolabe agents in the given time and scope
    * set_contacts(addresses) - specify addresses of initial agents to connect to
    * get_attributes(domain, event queue) - report updates to attributes of domain
    * get_children(domain, event queue) - report updates to domain membership
    * set_attribute(domain, attribute, value) - update the given attribute



