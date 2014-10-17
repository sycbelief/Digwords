import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting
/**
 * Created by Administrator on 2014/8/18.
 */
object digwords_1_0 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("digwords.version 1.0")
    val sc = new SparkContext(conf)
    // 获取参数
    val numPartiton : Int = args(0).toInt // partition数
    val wordLength : Int = args(1).toInt   // 最长词长度
    val wordFilter : Int = args(2).toInt   // 词频阈值
    val frequencyThreshold : Int = args(3).toInt   // 最小词频阈值
    val consolidateThreshold : Double = args(4).toDouble  // 最小凝结度阈值
    val freedomThreshold : Double = args(5).toDouble  // 最小自由熵阈值

    // 预处理
    val sourceFile = sc.textFile(args(6), numPartiton).flatMap( line => preproccess( line ) )     // 按标点分句
    val stopwords = sc.textFile(args(7))  //高频词及常用词
    val stopwordsDic = stopwords.collect()
    val broadcastStopWords = sc.broadcast(stopwordsDic)
    val textLength = sourceFile.map(line => line.length).reduce((a, b) => a + b)       // 计算总文本长度
    // 开始分词
    // Step1 : 词频  Step2 ； 凝结度   Step3 : 自由熵
    // 抽词，生成词长1-6的词频词典
    val FreqDic = sourceFile.flatMap((line: String) => splitWord(line, wordLength + 1)).map((word: String) => (word, 1)).reduceByKey(_ + _ )
    //  对待查词表的初步过滤，留下长度为2-5，频率>wordFilter且不在常用词字典中的词
    val candidateWords = FreqDic.filter(line => filterFunc(line, wordLength, wordFilter)).filter(line => filterStopWords(line._1, broadcastStopWords.value))
    candidateWords.persist()    // 得到candidate及词频

    val disDictionary = FreqDic.collect()    // 一个 Array[String , Int]
    Sorting.quickSort(disDictionary)(Ordering.by[(String, Int), String](_._1))    //对字典进行排序
    val broadforwardDic = sc.broadcast(disDictionary)     // 广播正序字典
    // Step2 计算凝结度 返回：( String , (Int ,Double))(词， （频率，凝结度）)
    val consolidateRDD = candidateWords.map(line => countDoc(line, textLength, broadforwardDic.value))
    // 计算自由熵 step3
    //右自由熵
    val rightFreedomRDD = candidateWords.map{case (key ,value) => countDof((key ,value), broadforwardDic.value)}
    broadforwardDic.unpersist()
    // 左自由熵
    val wordsTimesReverse = FreqDic.map{case (key ,value) => ( key.reverse , value)}   // 逆序词典
    val disDictionaryReverse = wordsTimesReverse.collect()
    Sorting.quickSort(disDictionaryReverse)(Ordering.by[(String, Int), String](_._1))
    val broadbackwardDic = sc.broadcast(disDictionaryReverse)                                       // 广播逆序字典
    val leftFreedomRDD = candidateWords.map{case (key ,value) => countDof ((key.reverse , value) ,broadbackwardDic.value)}.map{case (key,value) => (key.reverse, value)}
    broadbackwardDic.unpersist()
    // 整合三个特征
    val freedomRDD_temp = rightFreedomRDD.join(leftFreedomRDD)
    val freedomRDD = freedomRDD_temp.map({case (key, (value1, value2)) => (key, math.min(value1, value2))})
    val resultRDD = consolidateRDD.join(freedomRDD)
    // 经过阈值过滤得到最终的词
    val finalRDD = resultRDD.filter{line : (String, ((Int, Double), Double)) => calculation( line._2._1._1 , line._2._1._2 ,line._2._2 ,frequencyThreshold ,consolidateThreshold ,freedomThreshold  )}
    finalRDD.saveAsTextFile(args(8))
    //新加参数
    //FreqDic.map{ case(key,value)=> (value,key)}.sortByKey( false , 1).map{ case(key ,value) => (value , key)}.saveAsTextFile( args(9))
    //consolidateRDD.map{ case(key,value)=> (value._2 ,(key , value._1))}.sortByKey( false , 1).map{ case(key ,value) => (value._1 ,(value._2 , key) )}.saveAsTextFile( args(10))
    //freedomRDD.map{ case(key,value)=> (value,key)}.sortByKey( false , 1).map{ case(key ,value) => (value , key)}.saveAsTextFile( args(11))
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
  //把小短句分成长度1-wordLength+1长度的词，时间复杂度为n^2
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
  def filterFunc( line : (String , Int) , wordLength : Int , wordFilter : Int) : Boolean= {
    val len = line._1.length
    val frequency = line._2
    len >= 2 && len <= wordLength && frequency > wordFilter
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
  def countDoc(word: (String , Int), TextLen: Int ,  dictionary : Array[(String ,Int)]): (String , (Int ,Double)) = {
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
        /*if (tmp > word._2.toDouble * TextLen.toDouble / searchDicLword._2.toDouble / searchDicRword._2.toDouble){
          realL = Lword
          realR = Rword
          realLfreq = searchDicLword._2
          realRfreq = searchDicRword._2
          tmp =  word._2 * TextLen * 1.0 / (searchDicLword._2* searchDicRword._2)
        }*/
      }
    }
    if(tmp < 0)
      println(word)
    (word._1 , (word._2, tmp))
  }
  //计算自由熵
  def countDof(wordLine: (String , Int) , dictionary : Array[(String ,Int)]) : (String , Double) = {
    val word = wordLine._1
    val Len = word.length
    var total = 0
    var dof = mutable.ArrayBuffer[Int]()
    var pos = BinarySearch(word, dictionary, 0, dictionary.length)._1
    val dictionaryLen = dictionary.length
    if ( pos == 0){
      print(word)
      println( "No words found")
      return ( word , 0)
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
    ( word ,dof.map((x:Int) => 1.0*x/total).map((i:Double) => -1 * math.log(i)*i).foldLeft(0.0)((x0, x)=> x0 + x))
  }
  //设定阈值过滤结果
  def calculation ( f : Int , c : Double , fr : Double , frequency : Int , consolidate : Double , free : Double) : Boolean = {
    f >= frequency && c >= consolidate && fr >= free
  }
}