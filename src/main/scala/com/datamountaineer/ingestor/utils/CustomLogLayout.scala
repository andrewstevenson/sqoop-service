package com.datamountaineer.ingestor.utils

import java.text.SimpleDateFormat
import java.util.Date

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.{CoreConstants, LayoutBase}
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.StringUtils

class CustomLogLayout extends LayoutBase[ILoggingEvent] {

  override def doLayout(event: ILoggingEvent) : String = {
    val sbuf = new StringBuffer(128)
    sbuf.append(new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date(event.getTimeStamp())))
    sbuf.append(" ")
    sbuf.append(String.format("%-6s", event.getLevel()))
    sbuf.append(" ")
    sbuf.append(event.getFormattedMessage())
    if((event.getLevel() == Level.ERROR || event.getLevel() == Level.WARN) && event.getThrowableProxy() !=null) {
    sbuf.append(" - ");
    sbuf.append(StringUtils.substringBefore(event.getThrowableProxy().getMessage(), IOUtils.LINE_SEPARATOR))
    }
    sbuf.append(CoreConstants.LINE_SEPARATOR).toString
  }
}