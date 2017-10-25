// ****************************
// 9.2 Visualizations in Scala
// ****************************

/*
 Whereas python and R users have a plethora of options for
 visualiztions, Scala users generally are pretty limited. Although
 there are some decent java libraries out there, I've found that plotly
 and vegas-viz are pretty nice for interactive visualizations. I'll 
 provide some examples below. 
 
 Note that since plot.ly for scala is an online tool, you need to register an 
 account at plot.ly and then store your api_key in .plotly/.credentials in your 
 cdsw project. 
*/

//  { “username”: “<username>“, “api_key”: “<apikey>“ }

// ****************************
// Plotly Example
// ****************************


def idToURL(id:String):String={
  val temp=id.split(":")
  return "http://plot.ly/~" + temp(0) + "/" + temp(1) + ".embed" 
}

def genHTML (id:String,width:Integer,height:Integer):String={
  val url=idToURL(id)
  val text=s"""
    <iframe width=$width height=$height frameborder="0" scrolling="no" src=$url></iframe>
    """
  return text
}

def drawHTML(mime:String,content:String)={
  kernel.display.content(mime,content)
}

import co.theasi.plotly._
  
val bballdata=spark.sql("select * from basketball.players")

val (pts,exp) = bballdata.select("pts","exp").collect.map(x=>(x(0).asInstanceOf[Double],x(1).asInstanceOf[Int])).unzip

val commonAxisOptions = AxisOptions().
  tickLength(5).
  gridWidth(2)
  
val p= Plot().
  withScatter(exp,pts,ScatterOptions().mode(ScatterMode.Marker)).
  xAxisOptions(commonAxisOptions.title("Experience")).
  yAxisOptions(commonAxisOptions.title("Points"))
  
val figure = Figure().
  plot(p).title("Points vs Experience")

val id= draw(p, "basketball-points-vs-experience", writer.FileOptions(overwrite=true)).fileId
  
drawHTML("text/html",genHTML(id,600,600))
  
// ****************************
// 9.2 - Vegas-viz Example
// ****************************
  
import vegas._
import vegas.sparkExt._

//set render to html
implicit val render = vegas.render.ShowHTML(kernel.display.content("text/html", _))

val bballdata = spark.sql("Select * from basketball.players where year between 2000 and 2010")

Vegas().
  configCell(width=600, height=600).
  withDataFrame(bballdata).
  encodeX("exp",Quant).
  encodeY("PTS",Quant).
  mark(Point).
  show
 