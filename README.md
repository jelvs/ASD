# Algorithms and Distributed Systems

## Default Variant - Project

### Introduction

The goal of this phase of the Project is to implement a publish-subscribe protocol on top of an unstructured overlay 
network (also known as unstructured overlay network).
We consider a publish subscribe system similar to the one in option A, which offers to other protocols the following 
interface:

**subscribe (TOPIC):** That notifies the system that the protocol above is interested in received 
(through a notification) all messages published that are tagged with TOPIC.
 
**unsubscribe (TOPIC) :**  That notifies the system that the protocol above is no longer interested in receiving 
messages tagged with TOPIC

**publish (TOPIC, message):** That publishes, to the system , a message tagged with TOPIC. For simplicity, 
we consider that messages are tagged with exactly one topic.
    
                        |                       Test Application                          |
    
                        -------------------------------------------------------------------

                        |                      Publish-Subscribe                          | 

                        -------------------------------------------------------------------

                        |           Partial View Membership (Overlay Network)             |
    
                        -------------------------------------------------------------------
                        
                                                        |
                                                        |
                                                        |
                                                        |
                                                        |
                                                        |
                                                        |
                                                       \|/
                                                        v
                                                        
                                                       
                        |                       Test Application                          |
                            
                        -------------------------------------------------------------------
                        
                        |                       Publish-Subscribe                         | 
                        
                        -------------------------------------------------------------------
                        
                        |                           PlummTree                             |
                        
                        ------------------------------------------------------------------- 
                        
                        |                           HyperView                             |
                        
                        -------------------------------------------------------------------                                                      
                                                                                   

### Implementation Suggestions

**Partial View Membership:** You can get ideas from Cyclon, Scamp, or HyParView. While you could implement one of these 
algorithms you can also make modifications to them, or combine ideas from multiple algorithms.

**Publish Subscribe Layer:** For the subscribe/unsubscribe operation you can limit this to a local operation 
(i.e, each process can simply store local information about the topics currently subscribed). 
To disseminate information, you can use a gossip-based broadcast protocol, and filter which messages are delivered to 
the application locally. You can also think about optimization that might allow to reduce the amount of messages 
being transmitted without impairing the delivery rate.
        
### Unstructured overlay network 

        an unstructured overlay network organizes nodes in a random topology that ensures global connectivity
    (i.e, there is at least on path in this logical network that allows a message to reach from any node to every
    other node in the system). The interface of this type of overlay is quite simple, it exposes an operation denoted,
    in the context of this project, as neighbors( N ) that expose to the requesting protocol a list of, at most N,
    current neighbors of that node (i.e., a random sample).
    
    
   
