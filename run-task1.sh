#!/usr/bin/env bash

mvn package exec:exec -Dexec.executable="java" -Dexec.args="-cp %classpath eu.stratosphere.tutorial.RunTask1"
