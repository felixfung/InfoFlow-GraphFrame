# InfoFlow
An Apache Spark implementation of the InfoMap community detection algorithm

## Theory

This section provides the discrete maths that allow the InfoMap algorithm to be adapted onto Apache Spark, and the development of the parallel version, InfoFlow.

### Fundamentals

These are the fundamental maths found in the original paper [Martin Rosvall and Carl T. Bergstrom PNAS January 29, 2008. 105 (4) 1118-1123; https://doi.org/10.1073/pnas.0706851105]:

#### Nodes

Each node is indexed, with the index denoted by greek alphabets α, β or γ.
Each node α is associated with an ergodic frequency pα.
In between nodes there may be a directed edge ωαβ from node α to node
β. The directed edge weights are normalized with respect to the outgoing
node, so that

![]("https://latex.codecogs.com/svg.latex?\sum_\alpha&space;\omega_{\alpha\beta}&space;=&space;1)

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

What things you need to install the software and how to install them

```
Give examples
```

### Installing

A step by step series of examples that tell you have to get a development env running

Say what the step will be

```
Give the example
```

And repeat

```
until finished
```

End with an example of getting some data out of the system or using it for a little demo

## Running the tests

Explain how to run the automated tests for this system

### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment

Add additional notes about how to deploy this on a live system

## Built With

* [Dropwizard](http://www.dropwizard.io/1.0.2/docs/) - The web framework used
* [Maven](https://maven.apache.org/) - Dependency Management
* [ROME](https://rometools.github.io/rome/) - Used to generate RSS Feeds

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags).

## Author

* **Felix Fung** - *Everything* - [Felix Fung](https://github.com/felixfung)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone who's code was used
* Inspiration
* etc
