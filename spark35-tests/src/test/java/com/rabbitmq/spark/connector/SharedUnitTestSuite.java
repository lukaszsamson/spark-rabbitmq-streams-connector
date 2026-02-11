package com.rabbitmq.spark.connector;

import org.junit.platform.suite.api.ExcludeTags;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectPackages("com.rabbitmq.spark.connector")
@ExcludeTags("spark4x")
public class SharedUnitTestSuite {
}
