
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
