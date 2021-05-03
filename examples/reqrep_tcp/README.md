# RustyMQ Request-Reply TCP example
This is an example package that is an introduction on how to use RustyMQ.
The executable is able to act as a server (Reply socket) that can receive operation requests
for the addition or multiplication of two natural numbers then execute them and then reply with the result.

It is also able to run as a client that will generate random addition and multiplication tasks of natural numbers,
and will also calulate the round trip time of the request. 

## Usage
### Run as server
`<executable> --mode server --address <binding address>`

### Run as client
`<executable> --mode client --address <server bound address>`