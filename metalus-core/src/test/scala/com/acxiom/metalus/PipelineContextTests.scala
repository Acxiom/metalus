package com.acxiom.metalus

import com.acxiom.metalus.audits.{AuditType, ExecutionAudit}
import org.scalatest.funspec.AnyFunSpec

class PipelineContextTests extends AnyFunSpec{
  describe("PipelineContext") {
    it("Should set global values") {
      val ctx = TestHelper.generatePipelineContext()
      assert(ctx.globals.isDefined)
      assert(ctx.globals.get.isEmpty)
      val map = Map[String, Any]("one" -> 1, "two" -> "two")
      val updatedCtx = ctx.setGlobal("three", 3).setGlobals(map).setGlobalLink("testGL", "!some.path")
      assert(updatedCtx.globals.isDefined)
      assert(updatedCtx.globals.get.size == 4)
      assert(updatedCtx.getGlobalString("two").isDefined)
      assert(updatedCtx.getGlobalString("two").get == "two")
      assert(updatedCtx.getGlobal("one").isDefined)
      assert(updatedCtx.getGlobalAs[Int]("one").get == 1)
      assert(updatedCtx.getGlobal("three").isDefined)
      assert(updatedCtx.getGlobalAs[Int]("three").get == 3)
      assert(updatedCtx.getGlobalString("one").isEmpty)
      assert(updatedCtx.isGlobalLink("testGL"))
      assert(updatedCtx.getGlobal("testGL").isDefined)
      assert(updatedCtx.getGlobal("testGL").get == "!some.path")
    }

    it("Should set audit metrics") {
      val ctx = TestHelper.generatePipelineContext()
      val pipelineKey = PipelineStateKey("pipelineId")
      val stepKey = PipelineStateKey("pipelineId", Some("stepId"))
      val updatedCtx = ctx.setPipelineAudit(ExecutionAudit(pipelineKey, AuditType.PIPELINE, Map[String, Any](),
        System.currentTimeMillis()))
        .setPipelineAudit(ExecutionAudit(stepKey, AuditType.STEP, Map[String, Any](),
          System.currentTimeMillis()))
        .setPipelineAuditMetric(stepKey, "bubba", "gump")

      assert(updatedCtx.getPipelineAudit(pipelineKey).isDefined)
      assert(updatedCtx.getPipelineAudit(pipelineKey).get.metrics.isEmpty)
      val metricsCtx = updatedCtx.setPipelineAuditMetric(pipelineKey, "fred", "redonthehead")
      assert(metricsCtx.getPipelineAudit(pipelineKey).get.metrics.size == 1)
      assert(metricsCtx.getPipelineAudit(pipelineKey).get.metrics.contains("fred"))
      assert(metricsCtx.getPipelineAudit(pipelineKey).get.metrics("fred") == "redonthehead")
      assert(metricsCtx.getPipelineAudit(stepKey).get.metrics("bubba") == "gump")
    }
  }
}
