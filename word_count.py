from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("WordCount")
sc = SparkContext(conf=conf)

# Ganti '25MB.txt' dengan nama file yang ingin Anda hitung kata-katanya
lines = sc.textFile("25MB.txt")

# Hitung jumlah kata unik
words = lines.flatMap(lambda line: line.split(" "))
wordCounts = words.countByValue()

# Hitung jumlah seluruh kata
totalWords = words.count()

# Tampilkan jumlah kata unik
for word, count in wordCounts.items():
    print(f"{word}: {count}")

# Tampilkan jumlah seluruh kata
print(f"Total Words: {totalWords}")

sc.stop()
