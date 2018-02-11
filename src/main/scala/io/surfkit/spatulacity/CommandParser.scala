package io.surfkit.spatulacity

import java.io.File
import java.util.UUID
import scopt._



/**
  * Created by suroot on 22/06/16.
  */
object CommandParser {

  case class Mode(name: String)
  object Mode{
    val none = Mode("none")
    val transform = Mode("transform")
    val ml = Mode("ml")
    val stats = Mode("stats")
  }

  case class CmdConfig(file: File = new File("."),
                       out: File = new File("."),
                       quantquote: File = new File("."),
                       kibot: File = new File("."),
                       debug: Boolean = false,
                       transform: String = "",
                       mode: Mode = Mode.none,
                       symbol: String = "")

  private[this] val parser = new scopt.OptionParser[CmdConfig]("spatula-city") {
    head("spatula-city", "0.0.1-SNAPSHOT")

    opt[File]('f', "files").valueName("<file1>,<file2>...")
      .action( (x, c) => c.copy(file = x) )
      .text("input file")

    opt[Unit]("debug").hidden().action( (_, c) =>
      c.copy(debug = true) ).text("this option is hidden in the usage text")

    help("help").text("prints this usage text")
    note("this utility can be used to alter production data and apply patches.\n")

    cmd("transform").required().action( (_, c) => c.copy(mode = Mode.transform) ).
      text("transform is a command.").
      children(
        opt[File]("file").abbr("f").action( (x, c) =>
          c.copy(transform = "sanity", file = x) ).text("parent directory"),
        opt[File]("out").abbr("o").action( (x, c) =>
          c.copy(transform = "sanity", out = x) ).text("output directory"),
        opt[String]("skip").abbr("s").action( (x, c) =>
          c.copy(transform = "sanity", symbol = x) ).text("skip until")
      )

    cmd("stats").required().action( (_, c) => c.copy(mode = Mode.stats) ).
      text("stats is a command.").
      children(
        opt[File]("quantquote").abbr("q").action( (x, c) =>
          c.copy(quantquote = x) ).text("quantquote directory"),
        opt[File]("kibot").abbr("k").action( (x, c) =>
          c.copy(kibot = x) ).text("kibot directory"),
        opt[File]("out").abbr("o").action( (x, c) =>
          c.copy(out = x) ).text("output directory")
      )
  }


  def parse(args: Array[String]):Option[CmdConfig] =
  // parser.parse returns Option[C]
    parser.parse(args, CmdConfig())

}