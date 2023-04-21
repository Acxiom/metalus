package com.acxiom.metalus.utils

import com.acxiom.metalus.actors.ProcessManager
import com.google.inject.AbstractModule
import play.api.libs.concurrent.AkkaGuiceSupport

class AgentModule extends AbstractModule with AkkaGuiceSupport {
  override def configure(): Unit = {
    bind(classOf[AgentUtils]).asEagerSingleton()
    bindActor[ProcessManager]("process-manager")
  }
}
