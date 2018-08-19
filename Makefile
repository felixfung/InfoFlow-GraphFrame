# a "vanilla" spark submit framework

.PHONY: run
default: build.timestamp

build.timestamp: $(wildcard src/main/scala/*.scala)
	sbt package
	touch build.timestamp

run: build.timestamp
	spark-submit --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 target/scala-2.11/infoflow_2.11-1.0.jar
