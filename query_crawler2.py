import csv, json
import wikipedia
from textblob import TextBlob
import random

queries = []

with open('../../sample_dataset 2.json', 'r') as reader:
	data = json.load(reader)

	textB = ""
	cnt=0
	for query in data:
		textB += query['text']
		cnt+=1
		if cnt>1000:
			break
	blob = TextBlob(textB)

	# print(blob.noun_phrases)
	listToStr = ' '.join([str(elem) for elem in blob.noun_phrases])
	noun_file = open('text_nouns.txt', 'w')
	noun_file.write(listToStr)
	noun_file.close()
	list_of_random_items = random.sample(blob.noun_phrases, 150)
	# print(list_of_random_items)
	i= 0
	for query in list_of_random_items:

		wiki_titles = wikipedia.search(query, results=10)
		# print(query,"--- ",wiki_titles)
		i+=1
		singleq = {}
		singleq['id'] = 'QID'+str(i)
		singleq['query'] = query
		singleq['results'] = wiki_titles
		queries.append(singleq)

keys = queries[0].keys()

a_file = open("query_results.csv", "w")
dict_writer = csv.DictWriter(a_file, keys)
dict_writer.writeheader()
dict_writer.writerows(queries)
a_file.close()
