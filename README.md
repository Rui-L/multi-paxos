A simple Multi-Paxos protocol over HTTP.

## Configuration
A `paxos.yaml` config file is needed for the nodes to run. A sample is as follows:
```Yaml
nodes:
    - host: localhost
      port: 30000
    - host: localhost
      port: 30001
    - host: localhost
      port: 30002
    - host: localhost
      port: 30003
    - host: localhost
      port: 30004
    - host: localhost
      port: 30005

loss%: 0

skip: 10

heartbeat:
    ttl: 20
    interval: 10

election_slice: 10

sync_interval: 10

misc:
    network_timeout: 10
```

## Run the nodes
`for i in {0..5}; do ./node.py -i $i &; done`

## Check the consistency
`for i in .*.dump; do hash $i; done`
