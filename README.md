Digwords
========
所有做的事情，都是围绕http://www.matrix67.com/blog/archives/5044 这篇论文进行的，同时也参考了一篇《基于大规模语料的中文新词抽取算法的设计与实现》的毕业论文。一句话总结来说，做的就是无字典的抽词。  
其中经历了python单机，python+spark尝试，scala单机，scala+spark尝试，调bug定阈值定规则几个阶段。  
代码暂时还没有整理好，后面还有一个多星期可能还会有一些新的东西，暂时做一个阶段性的总结，也为自己理一下思路。  
首先说算法。其实主要就是三个特征。频率，凝固度，自由熵。因为是无字典的抽词，所以只能通过偏统计方面的特征让一个candidate被推成一个真正的词。  
频率不用解释了，可以肯定的是频率低的词肯定不能成词，而频率很高的词除了是热词之外，也很可能是一个常用词，比如：“我们”、“年月”和停用词：“并且”、“因此”。  
凝固度表明一个词它的左右两部分的“凝实度”。比如说“蝴蝶”，“蜘蛛”这种词，当“蝴”出现的时候后面很可能就出现“蝶”，反之亦然。那么“蝴蝶”这个词的两个字就“粘合”得非常紧，凝固度就比较高。相反，“货车司机”中“货车”和“司机”这两个词的粘合度就不是很高了。需要考虑，二字词的凝固度很容易高，但是多字词一般可以分割成两个小词，凝固度不容易很高（说不容易是因为凝固度跟文本也是相关的。比如在二手车这个类目下，“前四后八”这个词在AD中很容易一起出现，它的凝固度也会比较高）。  
自由熵，表明一个词的“自由度”。举论文中的一个例子：“被子”和“辈子”。“被子”这个词很百搭，它可以是“晒”“盖”“挂”“踢”“新”“掀”被子，因而它的自由熵就很大。但是“辈子”就不一样了，很局限的只有“一”“这”“上”“几”“八”辈子有限的几个前缀。我们可能更倾向于认为“辈子”不是一个词，而“一辈子”更像一个词。自由熵可以分为右自由熵和左自由熵，取两者的最小值作为这个词的自由熵。实际上，在文本不是很大的时候，尤其是有领域局限的时候，很多词的自由熵是0，通过滤掉这些自由熵为0的词，可以筛掉大部分无意义词或者不成词的词。  
在《基于大规模语料的中文新词抽取算法的设计与实现》论文中提到了更多的特征（如下图），其中7-11这5个特征提取的函数已经写好了，但是还没有加到现在做的里面去使用。（主要是三个指标调阈值已经很困难了，特征加到8个只是用简单的阈值法不是很适合，如果要用机器学习的方法话，后面还是可以再加上去）  

 ![image](https://github.com/sycbelief/Digwords/blob/master/pic/otherFeatures.png)  
下面说说实现的几个阶段和几个比较大的坑。  
1.	在开始决定用python3来进行中文文本处理，是因为python3在中文编码方面做出了很大的改进。读入后直接对unicode进行操作，写入时也不需要再指定编码。不过一开始就掉到了编码问题的坑里，文件一直读不进来。最后发现是“utf-8”而不是“utf8”，也是无奈了。所以最后完成的时候发现，python3真的在处理中文文本上具有很大的优越性。Python的硬伤在于速度慢。以前一直不觉得，这次跟其他语言一对比，劣势就出来了。其中遇到一个两重for循环，哪个放外面哪个放里面的问题，有点想当然了，后面做了测试才发现自己想的不对。所以任何时候，都不能用自己认为对的就省下验证的过程。  
2.	对于三个特征的处理，一开始是打算把特征全部归一化，给不同的权重，把3维降到一维再定阈值。尝试了最大-最小规范化和零-均值规范化方法。两种方法在归一化的过程中都遇到数据范围过广，归一化后特征区分度变小的问题。尝试过用log把大范围拉小也未果。所以最后决定放弃这种降维思路，直接定三个特征的阈值。  
3.	在计算左右自由熵的时候，通过排序+二分查找代替全文本顺序搜索，把时间复杂度从O(n^2)降到O(nlogn)  
4.	在python3取得不错效果后，spark也搭好了，所以开始在集群上跑抽词。这个时候出现了一个pyspark的坑。我搞了很久才弄明白，好像非要在电脑上搭个虚拟spark或者是在shell上跑，才可以debug写的python程序。而且spark上面的python是2.5的，版本不互相兼容，没有利用到python处理编码问题的优势。纠缠了很久之后最后还是投入了scala的怀抱。话说回来，觉得scala、spark还是属于一个比较新的东西，网上的介绍很少，基本都是翻译官方文档或者是英文的文档。而官方文档只显示shell的执行结果，跟打了jar包跑出来结果是可能有不一样的。  
学习scala用了半个多星期，后来秋秋秒写了一个单机简化版的抽词程序，借鉴过来看了。感觉还是看了代码才会更加熟悉一门语言。随后单机的scala程序就写得比较顺手了。  
5.	在把scala扔到spark跑的时候，遇到的第一个问题是，每次处理一个文本，第一行总是会被处理两遍，还会多出很奇葩的换行符和空字串，这个bug找了很久一直找不出来。开始听从泉哥的建议，在文本第一行加了一个逗号，绕开了这个bug。后来去问了包大人，终于明白原来是编码带不带bom的问题（这个在前两天物理哥转过来的一个邮件里面也提到了）。  
6.	参照3，里面用到了二分查找算法。这里被吴宏挖了个坑，我跟秀学姐debug了好久，才发现一个传出变量的错误。这个坑给我们带来了一些无法料想的结果，一直没定位到这个bug，真是泪奔了。谴责脸！  
7.	在抽词程序粗粗地实现了以后，秀学姐问了下泉哥明确了目的，我也问了蒙奇一些细节方面的需求。其实感觉这一步进行得有点晚了。需求明确下来之后，有些函数需要重写，有些结构需要调整，浪费了一些时间，感觉这是很没有必要的。其中涉及到数字、英文字母以及带小数点的数字的过滤方法的函数，感谢沈艳在离职前最后两天帮忙写了一版。  
8.	最近最后遇到的一个坑是凝结度为负的坑。Scala集群上跑的中间结果一直输出不了结果， 因此一直不明白问题出在哪里。函数被排查了几遍，而且只有在数据量大了会出现这种问题。最后怒了，直接把所有中间结果都保存下来才发现时因为乘法位数太高溢出为负的情况。本来想用Big来改这个错，后来觉得这会占用更多的空间，在目前看来还用不到。所以调整了一下乘除顺序，避免了两个大数直接相乘的情形，暂时解决了这个问题。但是这也是一个隐患，可能后面再想想要不要改成Big或者Long来做。  

下面简单写一下version1的流程：  
RDD操作图如下。  
![image](https://github.com/sycbelief/Digwords/blob/master/pic/Stage%200.png)  
Note: 图中正方形代表一个RDD。整个过程分为 个stage。  
Stage 0 主要是对数据进行预处理，在获得文本长度时触发action，完成第一个stage。preprocess()方法的功能：  
(1) 按标点符号断句，但会考虑.是小数点的情况  
(2) 保留合法字符，祛除非法字符  
(3) 英文字符大小写统一，阿拉伯数字和中文数字统一  
最后的action获得一个Int类型的值  
![image](https://github.com/sycbelief/Digwords/blob/master/pic/Stage%201.png)  
Stage 1 主要读取stopwords和常用词，并广播成字典，通过collect触发action，并把字典广播  
![image](https://github.com/sycbelief/Digwords/blob/master/pic/Stage%202.png)  
Stage 2 生成词频词典FreqDic  
![image](https://github.com/sycbelief/Digwords/blob/master/pic/Stage%203_1.png)  
![image](https://github.com/sycbelief/Digwords/blob/master/pic/Stage%203.png)  
Stage 3  
(1) 生成candidate词典  
(2) 计算凝结度  
(3) 计算左右自由熵，并取其中的最小值作为最终的自由熵  
(4) 合并结果，过滤  

更新了版本2，修改了一些东西，速度比1更快。
1. 去掉两个词过滤阈值改为一个。在统计出所有词的词频后直接对candidate words进行过滤，减少了后续计算量。
2. 首先计算自由熵，把自由熵为0的词再次过滤，减少了凝结度计算的词的个数。
3. 只关注一个RDD而不是把每一个特征存在一个RDD里面，最后join到一起。减少了RDD的数量。
