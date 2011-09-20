## Information

Experimental Stuff

## Installation

    rvm use ruby-1.9.2-p290@balancer
    bundle install

## Running



## Goals

* Order of Importance

1. Create a management worker that can be self sufficient
2. Management worker needs to be self sufficient in these areas: redundancy and scaling
3. Successfully run and test a management worker situation on 1 machine (should end up with 2 workers talking to each other)
4. Management workers need to provide a request interface for the creation/adoption of any type of worker.
5. Management workers will provide the needed availability, performance, and scaling for the requesting worker.
6. Requesting workers will be managed to the extent of their needs. (how much to scale, etc...)
7. Allow management to start forwarding requests for workers in different forms; http(s), websockets, tcp/udp, etc...
8. Refactor code
9. Focus on performance
