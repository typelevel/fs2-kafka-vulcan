/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan.stability

import weaver.SimpleIOSuite

object SchemaStabilitySuite extends SimpleIOSuite {
  pureTest("both corpora have the same codecs") {
    expect.same(CorpusV1.schemas.map(_._1), CorpusV2.schemas.map(_._1))
  }

  CorpusV1.schemas.zip(CorpusV2.schemas).foreach { case ((name, v1), (_, v2)) =>
    pureTest(s"schemas are the same for $name") {
      matches(v1) { case Right(_) => expect.same(v1, v2) }
    }
  }
}
