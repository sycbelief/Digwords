/**
 * Created by Administrator on 2014/9/5.
 */
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting
/**

 * 改变： 分离stopword字典和正常字典
 * Usage :
 * 0 并行度 =20   1 最长有效词长=5    2 有效词频门限值=24
 * 3 输入文件  4 stopword   5 字典位置  6 最终结果
 */
object digwords_3_0 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("digwords.version 3.0")
    val sc = new SparkContext(conf)
    // 获取参数
    val numPartiton : Int = args(0).toInt // partition数
    val wordLength : Int = args(1).toInt   // 最长词长度
    val frequencyThreshold : Int = args(2).toInt   // 结果词频阈值
    // 预处理
    val sourceFile = sc.textFile(args(3), numPartiton).flatMap( line => preproccess( line ) )     // 按标点分句
    val stopwords = sc.textFile(args(4))                            //stopwords
    val stopwordsDic = stopwords.collect()
    val broadcastStopWords = sc.broadcast(stopwordsDic)                                 //  广播的stopword 字典
    val textLength = sourceFile.map(line => line.length).reduce((a, b) => a + b)       // 计算总文本长度
    // 开始分词
    // Step1 : 词频  Step2 ； 自由熵   Step3 : 凝结度
    // 抽词，生成词长1-6的词频词典
    val freqDic = sourceFile.flatMap((line: String) => splitWord(line, wordLength + 1)).map((word: String) => (word, 1)).reduceByKey(_ + _ )
    //  对待查词表的初步过滤，留下长度为2-5，过滤 stopword, 过滤词频小于词频阈值的词
    val candidateWords = freqDic.filter(line => filterFunc(line, wordLength, frequencyThreshold,broadcastStopWords.value))   //RDD[(String, Int)]
    candidateWords.persist()    // 得到candidate及词频
    broadcastStopWords.unpersist()
    val wordsTimesReverse = freqDic.map{case (key ,value) => ( key.reverse , value)}
    val disDictionaryReverse = wordsTimesReverse.collect()
    wordsTimesReverse.unpersist()
    Sorting.quickSort(disDictionaryReverse)(Ordering.by[(String, Int), String](_._1))
    val broadbackwardDic = sc.broadcast(disDictionaryReverse)    // 广播逆序字典
    // 计算自由熵 step2   左自由熵 (String ,(Int ,Double))
    val leftFreedomRDD = candidateWords.map{ line => (line._1.reverse , line._2)}.map{ line => countDof (line ,broadbackwardDic.value)}.map( line => ( line._1.reverse , line._2))
    broadbackwardDic.unpersist()
    leftFreedomRDD.saveAsTextFile(args(9))
    val disDictionary = freqDic.collect()           // 广播正序字典 一个 Array[String , Int]
    Sorting.quickSort(disDictionary)(Ordering.by[(String, Int), String](_._1))    //对字典进行排序
    val broadforwardDic = sc.broadcast(disDictionary)     // 广播正序字典
    // 右自由熵[String ,(Int ,Double)]
    val rightFreedomRDD = candidateWords.map{line => countDof(line, broadforwardDic.value)}
    rightFreedomRDD.saveAsTextFile(args(10))
    //(String, (Double, (Int, Double)))
    val freedomRDD_temp = rightFreedomRDD.join(leftFreedomRDD)
    val freedomRDD = freedomRDD_temp.map({line :(String, ((Int ,Double), (Int, Double))) => (line._1, line._2._1._1 ,math.min(line._2._1._2, line._2._2._2))})
    freedomRDD_temp.saveAsTextFile(args(8))
    val freedomRDD2 = freedomRDD.filter{line :(String ,Int ,Double)=>(line._3 > 0.0)}
    // 计算凝结度 step2 ( String , Int ,Double)(词， 频率，自由熵) =》  (String , Int ,Double ,Double)
    val consolidateRDD = freedomRDD2.map{line :( String ,Int ,Double) => countDoc (line, textLength, broadforwardDic.value)}
    broadforwardDic.unpersist()
    candidateWords.unpersist()
    consolidateRDD.persist()
    // 分离字典 和 广告中候选词
    val dictionaryWord = sc.textFile( args(5))    // 读入字典  RDD[String]
    val dic = dictionaryWord.collect()
    val dicWord = sc.broadcast( dic )            // 广播字典 Array[String]
    // 字典数据RDD (String , Int ,Double ,Double)
    val candidateDicWords = consolidateRDD.filter{ line : (String , Int ,Double ,Double)=> filterDicWord( line._1 , dicWord.value)}
    val freedomTh = candidateDicWords.map{ line : (String , Int ,Double ,Double)=> line._3}
    val freedomThresholdHigh : Double = freedomTh.max()
    val freedomThresholdLow : Double = freedomTh.min()
    val consolidateThresholdRDD = candidateDicWords.map{line : (String , Int ,Double ,Double)=> line._4}
    val consolidateThresholdLow : Double = consolidateThresholdRDD.min()  // 最小凝结度阈值
    val consolidateThresholdHigh : Double = consolidateThresholdRDD.max()
    println( "Freedom Hight : " + freedomThresholdHigh)
    println( "Freedom Low : " + freedomThresholdLow)
    println( "Consolidate Hight : " + consolidateThresholdHigh)
    println( "Consolidate Low : " + consolidateThresholdLow)
    consolidateRDD.saveAsTextFile(args(7))
    val candidateADWords = consolidateRDD.filter( line => filterADWord( line ,dicWord.value,freedomThresholdLow,freedomThresholdHigh, consolidateThresholdLow,consolidateThresholdHigh))
    candidateADWords.saveAsTextFile(args(6))
  }
  def filterDicWord( word : String , dictionary : Array[String]) : Boolean={
    if ( dictionary.contains( word))
      true
    else
      false
  }
  //文本中出现明文的\r\n等转以符号
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
  //把小短句分成长度1-wordLength+1长度的词，时间复杂度为n^2,同时过滤掉stopword
  def splitWord(v: String, wordLength: Int ):ArrayBuffer[String] = {
    val len = v.length
    //textLength += len
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
  def filterFunc( line : (String , Int) , wordLength : Int , wordFilter : Int  ,dictionary : Array[ String]) : Boolean= {
    val word = line._1
    if ( dictionary.contains( word ))
      false
    else{
      val len = word.length
      val frequency = line._2
      len >= 2 && len <= wordLength && frequency >= wordFilter
    }
  }
  def filterADWord( word : (String , Int ,Double,Double) ,dictionary : Array[ String] , freedomLow : Double ,freedomHigh: Double ,consolidateLow :Double,consolidateHigh : Double ) : Boolean ={
    val free = word._3
    val consolidate = word._4
    if ( dictionary.contains( word ))
      false
    else if( (free >= freedomLow && free <= freedomHigh) && ( consolidate >= consolidateLow && consolidate <= consolidateHigh))
      true
    else false
  }
  //过滤掉stopword
  def filterStopWords ( words : String , dictionary : Array[ String]) : Boolean = {
    if (dictionary.contains( words ))
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
  def countDof(wordLine: (String , Int) , dictionary : Array[(String ,Int)]) : (String , (Int ,Double)) = {
    val word = wordLine._1
    val Len = word.length
    var total = 0
    var dof = mutable.ArrayBuffer[Int]()
    var pos = BinarySearch(word, dictionary, 0, dictionary.length)._1
    val dictionaryLen = dictionary.length
    if ( pos == -1){
      print(word)
      println( "No words found")
      return ( word , (0 , 0))
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
    ( word ,(wordLine._2 , dof.map((x:Int) => 1.0*x/total).map((i:Double) => -1 * math.log(i)*i).foldLeft(0.0)((x0, x)=> x0 + x)))
  }
}