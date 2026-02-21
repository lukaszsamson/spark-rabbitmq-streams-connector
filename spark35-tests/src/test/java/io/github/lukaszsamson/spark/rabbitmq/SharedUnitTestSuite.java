package io.github.lukaszsamson.spark.rabbitmq;

import org.junit.platform.suite.api.ExcludeTags;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectPackages("io.github.lukaszsamson.spark.rabbitmq")
@ExcludeTags("spark4x")
public class SharedUnitTestSuite {
}
