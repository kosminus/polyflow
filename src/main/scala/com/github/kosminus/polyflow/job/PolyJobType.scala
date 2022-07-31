package com.github.kosminus.polyflow.job

import com.github.kosminus.polyflow.app.{AppComponent, IApp}
import com.github.kosminus.polyflow.context.{BatchContext, StreamingContext}
import shapeless.Poly1

object PolyJobType extends Poly1 {

  implicit def caseBatchContext = at[(BatchContext, IApp, Array[String])] ((x) => {
    val jobComp = new JobComponent with AppComponent {
      override val job: IJob = new BatchJob()
      override val app: IApp = x._2
    }
    jobComp.job.process(x._3)
  })


  implicit def caseStreamingContext = at[(StreamingContext, IApp, Array[String])] ((x) => {
    val jobComp = new JobComponent with AppComponent {
    override val job: IJob = new StructuredStreamingJob()
    override val app: IApp = x._2
  }
    jobComp.job.process(x._3)
  })

}
