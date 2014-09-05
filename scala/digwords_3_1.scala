package digwords

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.util.Sorting
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
/**
 * Usage :
 * 0 输入文件  1 最终结果输出结果  2 词频门限值=24   3 凝结度下界  4 最长词长=5   5  分区个数=20
 * 6 stopword 字典
 * Created by Administrator on 2014/8/18.
 */
object digwords_2_1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("digwords.version 3.1")
    val sc = new SparkContext(conf)
    // 获取参数
    val frequencyThreshold : Int = args(2).toInt   // 最小词频阈值
    val consolidateThresholdLow : Double = args(3).toDouble  // 凝结度下界
    val wordLength : Int= args(4).toInt   // 最长词长度
    val numPartiton : Int = args(5).toInt // partition数，并行度

    val sourceFile = sc.textFile(args(0), numPartiton).flatMap( line => preproccess( line ) )     // 按标点分句,同时洗数据
    val textLength = sourceFile.map(line => line.length).reduce( (a, b) => a + b)       // 计算总文本长度
    // 开始分词 step1 : 词频    setp2: 自由熵 , 过滤自由熵为0的词   step3 : 计算凝结度
    // 抽词，生成1-6的词典 (String ,Int)
    val wordsTimes = sourceFile.flatMap ((line : String ) => splitWord(line, wordLength )).map( ( word : String ) => (word ,1)).reduceByKey(_ + _ )
    //val textLength = textLengthAccumulator.value
    //  生成待查词列表 2-5 step1,且频率> wordFilter
    val stopwords = sc.textFile(args(6))
    val stopwordsLocDic = stopwords.collect()
    val broadcastStopWords = sc.broadcast(stopwordsLocDic)
    //2-5的词，词频大于给定门限值，且经过stopwords的过滤
    val candidateWords = wordsTimes.filter( line => filterFunc( line , wordLength , frequencyThreshold ,broadcastStopWords.value))
    broadcastStopWords.unpersist()
    stopwords.unpersist()
    candidateWords.persist()
    // 计算自由熵 step2  左自由熵
    val wordsTimesReverse = wordsTimes.map{case (key ,value) => ( key.reverse , value)}
    val disDictionaryReverse = wordsTimesReverse.collect()
    Sorting.quickSort(disDictionaryReverse)(Ordering.by[(String, Int), String](_._1))
    val broadbackwardDic = sc.broadcast(disDictionaryReverse)
    // (String ,Int ,Double)
    val leftFreedomRDD = candidateWords.map{case (key ,value) => countDof ((key.reverse , value) ,broadbackwardDic.value)}.map{line => (line._1.reverse, (line._2, line._3))}
    wordsTimesReverse.unpersist()
    broadbackwardDic.unpersist()
    val singleWord = wordsTimes.filter(line => line._1.length == 1).collectAsMap()
    val singleDic = sc.broadcast(singleWord)   // 单字字典
    //右自由熵
    // 一个 Array[String , Int]
    val disDictionary = wordsTimes.collect()
    //对字典进行排序
    Sorting.quickSort(disDictionary)(Ordering.by[(String, Int), String](_._1))
    // 广播正序字典
    val broadforwardDic = sc.broadcast(disDictionary)
    // String , Double
    val rightFreedomRDD = candidateWords.map{case (key ,value) => countDof((key ,value), broadforwardDic.value)}.map{ line:( String ,Int ,Double) => (line._1 , line._3)}
    broadforwardDic.unpersist()
    //(String, (Double, (Int, Double)))
    val freedomRDD_temp = rightFreedomRDD.join(leftFreedomRDD)
    val freedomRDD = freedomRDD_temp.map({line :(String, (Double, (Int, Double))) => (line._1, line._2._2._1 ,math.min(line._2._1, line._2._2._2))})
    val freedomRDD2 = freedomRDD.filter{line :(String ,Int ,Double)=>(line._3!=0.0)}
    // 计算凝结度 step2 ( String , Int ,Double)(词， 频率，自由熵) =》  (String , Int ,Double ,Double)
    val consolidateRDD = freedomRDD2.map{line :( String ,Int ,Double) => countDoc (line, textLength, broadforwardDic.value)}.filter( line => line._4 >= 20)
    //val result = consolidateRDD.map( line => countDice_MI_SCP_ZSCORE_TSCORE( line , textLength ,singleDic.value)).filter{ line => calculation( line._2, line._3, line._4,line._6)}
    //result.saveAsTextFile(args(1))
  }

  def countDice_MI_SCP_ZSCORE_TSCORE(wordline:(String, Int , Double ,Double), TextLen: Int, singleDic: scala.collection.Map[String, Int]):(String, Double ,Double , Double, Double, Double, Double, Double, Double) = {
    val word = wordline._1
    val wordLength = word.length
    val wordfreq = wordline._2.toDouble
    var sum = 0.0
    var product = 1.0
    for (ch <- word){
      val singleWord = ch.toString
      sum += singleDic(singleWord)
      product *= singleDic(singleWord)
    }
    val ksi = product / math.pow(TextLen, wordLength-1)
    val Dice = wordLength * wordfreq / sum
    val MI = math.log(wordfreq / product / TextLen) + wordLength* math.log(1.0 * TextLen)

    val SCP = math.pow(wordfreq, wordLength.toDouble)/product
    val ZSCORE = (wordfreq - ksi) / math.sqrt(ksi * (1 - ksi / TextLen))
    val TSCORE = (wordfreq - ksi) / math.sqrt(wordfreq * (1 - wordfreq / TextLen))

    (word, wordfreq , wordline._3 ,wordline._4 , Dice, MI, SCP, ZSCORE, TSCORE)
  }

  //文本中出现明文的\r\n等转义符号
  def dealWithSpecialCase(content: String): ArrayBuffer[String] = {
    val patterns = Array("\\\\r\\\\n", "\\\\n", "\\\\t", "[0-9]{8,}");
    val tmp = mutable.ArrayBuffer[String]()
    val ret = mutable.ArrayBuffer[String]()
    ret += content
    for (pat <- patterns) {
      tmp.clear()
      for (ele <- ret) {
        val e = ele.trim()
        if (e != "") {
          tmp ++= e.replaceAll(pat, "|").split( """\|""")
        }
      }
      ret.clear()
      ret ++= tmp.clone()
    }
    ret
  }
  //判断符号是否有意义
  def isMeaningful(ch: Char): Boolean = {
    var ret = false
    val meaningfulMarks = Array('*', '-', 'X', '.','\\')
    if ((ch >= '一' && ch <= '龥') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || meaningfulMarks.contains(ch))
      ret = true
    ret
  }
  //把句子按标点符号分隔成小短句，同时过滤掉没有意义的符号
  def preproccess(content: String): ArrayBuffer[String] = {
    val ret = mutable.ArrayBuffer[String]()
    val puncs = Array(',', '，', '.', '。', '!', '！', '?', '？', ';', '；', ':', '：', '\'', '‘', '’', '\"', '”', '“', '、', '(', '（', ')', '）', '<', '《', '>', '》', '[', '【', '】', ']', '{', '}', ' ', '\t', '\r', '\n') // 标点集合
    var tmp = ""
    var i = 0
    var before = ' '
    for (ch <- content) {
      i += 1
      if (ch == '.' && Character.isDigit(before) && i < (content.length) && Character.isDigit(content.charAt(i))) {
        tmp += ch
      }
      else if (puncs.contains(ch)) {
        if (tmp != "") {
          ret ++= dealWithSpecialCase(tmp)
          tmp = ""
        }
      }
      else {
        if (isMeaningful(ch)) tmp += ch
        if (i == content.length) {
          ret ++= dealWithSpecialCase(tmp)
          tmp = ""
        }
      }
      before = ch
    }
    ret
  }
  //把小短句分成长度1-wordLength+1长度的词，时间复杂度为n^2
  def splitWord(v: String, wordLength: Int ):ArrayBuffer[String] = {
    val len : Int= v.length
    val greetStrings =  mutable.ArrayBuffer[String]()
    for (i <- 0 to len - 1) {
      // 单词起始点位置
      var j: Int = 1 // 新词长度
      while (i + j <= len && j <= wordLength) {
        val tmp: String = v.substring(i, i + j)
        greetStrings += tmp
        j += 1
      }
    }
    greetStrings
  }
  //过滤得到candidate word：长度2-5，频率大于wordfilter
  def filterFunc( line : (String , Int) , wordLength : Int , wordFilter : Int ,dictionary : Array[ String]) : Boolean= {
    val word = line._1
    if ( dictionary.contains( word ))
      false
    else if(word.startsWith(".") || word.endsWith(".") || word.startsWith("-") || word.endsWith("-"))
      false
    else{
      var flag = false
      val len = word.length
      val frequency = line._2
      for(ch <- word){
        if (ch < '0' || ch > '9'){
          flag = true
        }
      }
      if (flag == true){
        if(len >= 2 && len <= wordLength && frequency > wordFilter)
          return true
      }
      return false
    }
  }

  //过滤掉stopword
  def filterStopWords ( words : String , dictionary : Array[ String]) : Boolean = {
    if ( dictionary.contains( words ))
      false
    else
      true
  }
  //从广播变量中，用二分查找，查找到某个词的位置和频率
  def BinarySearch(word: String , dictionary : Array[(String ,Int)], from: Int = 0, to: Int) : (Int, Int) = {
    var L = from
    var R = to - 1
    var mid : Int = 0
    while (L < R) {
      mid = (L + R) / 2
      if (word > dictionary(mid)._1)
        L = mid + 1
      else
        R = mid
    }
    if (word != dictionary(L)._1){
      println("NOT FIND WORD" + word)
      return (-1, 0)
    }
    return (L,  dictionary(L)._2)
  }
  //计算凝结度

  def countDoc(word: (String , Int , Double), TextLen: Int ,  dictionary : Array[(String ,Int)]): (String , Int ,Double , Double) = {
    var tmp = TextLen.toDouble
    val len = dictionary.length
    for (num <- 1 to word._1.length-1){
      val Lword = word._1.substring(0, num)
      val Rword = word._1.substring(num)
      val searchDicLword = BinarySearch(Lword, dictionary, 0, len)
      val searchDicRword = BinarySearch(Rword, dictionary, 0, len)
      if( searchDicLword._1 == -1 || searchDicRword._1 == -1){
        println("Lword: " + Lword)
        println("Rword: " + Rword)
        println("No words found")
      }
      else{
        tmp = math.min(tmp, word._2.toDouble * TextLen /searchDicLword._2.toDouble/ searchDicRword._2.toDouble)

      }
    }
    if(tmp < 0)
      println(word)
    (word._1 , word._2, word._3 ,tmp)
  }
  //计算自由熵
  def countDof(wordLine: (String , Int) , dictionary : Array[(String ,Int)]) : (String , Int ,Double) = {
    val word = wordLine._1
    val Len = word.length
    var total = 0
    var dof = mutable.ArrayBuffer[Int]()
    var pos = BinarySearch(word, dictionary, 0, dictionary.length)._1
    val dictionaryLen = dictionary.length
    if ( pos == 0){
      print(word)
      println( "No words found")
      return ( word , 0 , 0)
    }
    while( pos < dictionaryLen && dictionary(pos)._1.startsWith(word)) {
      val tmp = dictionary(pos)._1
      if (tmp.length == Len + 1) {
        val freq = dictionary(pos)._2
        dof += freq
        total += freq
      }
      pos += 1
    }
    ( word ,wordLine._2 , dof.map((x:Int) => 1.0*x/total).map((i:Double) => -1 * math.log(i)*i).foldLeft(0.0)((x0, x)=> x0 + x))
  }
  //设定阈值过滤结果
  /*def calculation ( f : Int ,  fr : Double ,c : Double , frequency : Int  , free : Double ,consolidate_low : Double , consolidate_high : Double) : Boolean = {
    val consolidate : Boolean = c > consolidate_low && c < consolidate_high
    return ( (f >= frequency && consolidate) || ( f >= frequency && fr >= free ) || ( consolidate && fr >= free))
  }*/

}