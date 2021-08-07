val hello:String = "Hola"
  println(hello)

var helloThere:String = hello
helloThere = helloThere+" There "

val numberOne:Int =1
val truth:Boolean=true
val letterA:Char='a'
val pi:Double = 3.142
val piSinglePrecision:Float = 3.142f
val bigNumber:Long = 123454321
val smallNumber:Byte = 127

println("Here's a mess "+numberOne+truth+letterA+pi+piSinglePrecision+bigNumber)
println(f"Pi is about $piSinglePrecision%.3f")
println(f"Zero padding on the left: $numberOne%05d")
println(s"I can use the s prefix to use variables like $numberOne $truth $letterA")
println(s"I can also use expressions ${1+2}")

val theUltimateAnswer:String = "To life,the universe, and everything is 42."
val pattern = """.*([\d]+).*""".r
val pattern(answerString) = theUltimateAnswer
val answer: Int = answerString.toInt
println(answer)

val isGreater = 1>2
val isLesser = 1<2
val impossible = isGreater & isLesser
val anotherWay = isGreater && isLesser

val picard:String = "Picard"
val bestCaptain:String = "Picard"

val isBest:Boolean = picard==bestCaptain
//val isBest:Boolean = picard===bestCaptain

if(1>3) println("Impossible!") else println("The World makes sense.")
if(1>3){ println("Impossible!") }else {println("The World makes sense.")}

val number = 3
number match {
  case 1 => println("One")
  case 2 => println("Two")
  case 3 => println("Three")
  case _ => println("Something else")
}

for(x <- 1 to 4){
  val squared = x*x
  println(squared)
}

var x = 10
while(x>=0){
  println(x)
  x-=1
}

x=0
do{
  println(x);
  x+=1
}while(x<=1)

println({val x=10; x+20})

def squareIt(x:Int):Int = {
  x*x
}

def cubeIt(x:Int):Int = {
  x*x*x
}

println(squareIt(2))
println(cubeIt(2))

def tranformInt(x:Int, f:Int=>Int):Int = {
  f(x)
}

val result = tranformInt(2,x => cubeIt(x))

val captainStuff = ("Picard","Enterprise-D","NCC-1701-D")
println(captainStuff)

println(captainStuff._1)
println(captainStuff._2)
println(captainStuff._3)

val aBunchOfStuff = ("Kirk",1964,true)
val shipList = List("Enterprise","Defiant","Voyager","Deep Space Nine")
println(shipList(1))

println(shipList.tail)

for(ship <- shipList)
  println(ship)

val toPartitionBy:Seq[String] = Seq("")
