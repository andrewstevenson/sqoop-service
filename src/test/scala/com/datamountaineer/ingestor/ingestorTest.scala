package com.datamountaineer.ingestor

import com.datamountaineer.ingestor.conf.Configuration
import org.scalatest._

abstract class IngestorTest extends FunSuite with Matchers with OptionValues with Inside with Inspectors with Configuration