# Use Protobuf and gRPC

## References 

- ScalaPB : [link](https://scalapb.github.io/docs/grpc#project-setup)  


## Service / Msessage definition
`file.proto`
```
syntax = "proto3";

package com.example.protos;

// The greeting service definition.
service Greeter {
  // Sends a greeting
  rpc SayHello (HelloRequest) returns (HelloReply) {}
}

// The request message containing the user's name.
message HelloRequest {
  string name = 1;
}

// The response message containing the greetings
message HelloReply {
  string message = 1;
}



```
