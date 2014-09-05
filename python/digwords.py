import math
import time
import codecs


def washTextFile(textfileName, textfileNameNew):
	inputfiles = codecs.open(textfileName, "r", encoding = "gbk")
	text = inputfiles.read()
	newText = ""
	for ch in text:
		if ((ch >= '一' and ch <= '龥')):
		#or  (ch >= 'a' and ch <= 'z')
		#or  (ch >= 'A' and ch <= 'Z')
		#or  (ch >= '0' and ch <= '9')):
			newText += ch
	inputfiles.close()
	f = open(textfileNameNew, "w")
	f.write(newText)
	f.close()


def sortThePostfix(text, WordLen, PostfixList):
	TextLen = len(text)
	for i in range(WordLen + 5):
		text += 'a'
	for i in range(TextLen):
		Postfix = text[i : i + WordLen + 1]
		PostfixList.append(Postfix)
	PostfixList.sort()
	text = text[0 : TextLen]

def countFreq(dict, PostfixList, WordLen, MinFreq, WordDict, totalFreq):
	cnt = 0
	lastword = ""
	aStr = ""
	for i in range(1, WordLen + 5):
		aStr += 'a'
		dict[aStr] = 1
	for Len in range(1, WordLen + 1):
		for postfix in PostfixList:
			word = postfix[0 : Len]
			if (word == lastword):
				cnt += 1
			else:
				dict[lastword] = cnt
				totalFreq += cnt
				if (cnt >=  MinFreq and Len > 1):
					WordDict[lastword] = 0
				cnt = 1
				lastword = word
		dict[lastword] = cnt
		totalFreq += cnt
		if (cnt >= MinFreq and Len > 1):WordDict[lastword] = 0	
		lastword = ""
		cnt = 0


def countDof(dict, dictFreq, PostfixList, WordLen, WordDict, reverse = False):
	for Len in range(1, WordLen + 1):
		lastword = ""
		lastNextChar = ""
		lastCharCount = 0
		wordFreq = 1
		nowDoc = 0
		for postfix in PostfixList:
			word = postfix[0 : 0 + Len]
			if reverse: word = word[::-1]
			if word not in WordDict: continue
			if word == lastword:
				if (postfix[0 + Len] == lastNextChar):
					lastCharCount += 1
				else:
					if lastCharCount > 0 : nowDoc += -lastCharCount / wordFreq * math.log(lastCharCount / wordFreq)
					lastCharCount = 1
					lastNextChar = postfix[0 + Len]
			else:
				if lastCharCount > 0 : nowDoc += -lastCharCount / wordFreq * math.log(lastCharCount / wordFreq)
				dict[lastword] = nowDoc
				nowDoc = 0
				lastword = word;
				lastCharCount= 1
				lastNextChar = postfix[Len]
				wordFreq = dictFreq[word]
		if lastCharCount > 0 : nowDoc += -lastCharCount / wordFreq * math.log(lastCharCount / wordFreq)
		dict[lastword] = nowDoc


def countDoc(dict, wordDict, dictFreq, TextLen):
	for word in wordDict.keys():
		wordLen = len(word)
		tmp = 1000000000
		wordFreq = dictFreq[word]
		for i in range(1, wordLen):
			Lword = word[0 : i]
			Rword = word[i : ]
			LwordFreq = dictFreq[Lword]
			RwordFreq = dictFreq[Rword]
			tmp = min(tmp, wordFreq * TextLen / LwordFreq / RwordFreq)
		dict[word] = tmp



def main(textfileName, wordLen, dofVal, docVal, setMinFreq = 5):
	textfileNameNew = "tmp.txt"
	washTextFile(textfileName, textfileNameNew)
	f = open(textfileNameNew, "r")
	text = f.read()
	f.close()
	PostfixList = []
	sortThePostfix(text, wordLen, PostfixList)

	tSort = time.clock()
	print("After Sort:", tSort)

	dictFreq = {}
	wordDict = {}
	totalFreq = 0
	countFreq(dictFreq, PostfixList, wordLen, setMinFreq, wordDict, totalFreq)
	del dictFreq[""]

	tFreq = time.clock()
	print("After Freq", tFreq)


	dictRightDof = {}
	countDof(dictRightDof, dictFreq, PostfixList, wordLen, wordDict)
	del dictRightDof[""]

	tRDof = time.clock()
	print("After R Dof:", tRDof)


	dictLeftDof = {}
	reverseText = text[::-1]
	reversePostfixList = []
	sortThePostfix(reverseText, wordLen, reversePostfixList)
	countDof(dictLeftDof, dictFreq, reversePostfixList, wordLen, wordDict, True)
	del dictLeftDof[""]

	tLDof = time.clock()
	print("After L Dof:", tLDof)

	dictDoc = {}
	TextLen = len(text)
	print(TextLen)

	countDoc(dictDoc, wordDict, dictFreq, TextLen)


	tDoc = time.clock()
	print("AfterDoc:", tDoc)


	dictDof = {}
	ResultWords = []
	for word in wordDict:
		dictDof[word] = DOF = min(dictLeftDof[word], dictRightDof[word])
		DOC = dictDoc[word]
		FRE = dictFreq[word]
		if (DOF > 1.7 and DOC > 23):
			ResultWords.append((word, FRE, DOF, DOC))

	FNor = FAve = 0
	CNor = CAve = 0
	FreNor = FreAve = 0
	n = len(ResultWords)
	for rs in ResultWords:
		FAve += rs[2]
		CAve += rs[3]
		FreAve += rs[1]
	FAve = FAve / n
	CAve = CAve / n
	FreAve = FreAve / n

	print(CAve)
	print(FAve)
	print(FreAve)

	for rs in ResultWords:
		FNor += (rs[2] - FAve) * (rs[2] - FAve)
		CNor += (rs[3] - CAve) * (rs[3] - CAve)
		FreNor += (rs[1] - FreAve) * (rs[1] - FreAve)
	FNor = math.sqrt(FNor) / n
	CNor = math.sqrt(CNor) / n
	FreNor = math.sqrt(FreNor) / n

	tmp = []
	mxVal = [0, 0, 0]
	miVal = [10000000000, 10000000000, 10000000000]
	for rs in ResultWords:
		tmp.append([(rs[2] - FAve) / FNor, (rs[3] - CAve) / CNor, (rs[1] - FreAve) / FreNor, rs[0]])
	for tp in tmp:
		for i in range(3): 
			mxVal[i] = max(mxVal[i], tp[i]) 
			miVal[i] = min(miVal[i], tp[i])


	for tp in tmp:
		for i in range(3):
			tp[i] = 1 - math.exp(-(tp[i] - miVal[i]) / (mxVal[i] - miVal[i]))

	RW = []
	for tp in tmp:
		RW.append((tp[3], dictDof[tp[3]], dictDoc[tp[3]], dictFreq[tp[3]]))
	RW.sort(key = lambda t:t[3], reverse = True)

	f = open("zhibenlun0.txt", "w")
	for rw in RW:
		f.write(str(rw) + '\n')
	f.close()





t1 = time.clock()
print(t1)
#main(textfileName, wordLen, dof, doc, setMinFreq = 5)
main("zhibenlun.txt", 6, 0, 0, 10)
t2 = time.clock()
print(t2)
