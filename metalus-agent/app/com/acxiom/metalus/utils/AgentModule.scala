package com.acxiom.metalus.utils

import com.google.inject.AbstractModule

class AgentModule extends AbstractModule {
  override def configure(): Unit = {
    bind(classOf[AgentUtils]).asEagerSingleton()
  }
}
