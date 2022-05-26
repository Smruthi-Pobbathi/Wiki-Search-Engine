import csv
import wikipedia

queries = []
with open('queries.txt', 'r') as reader:
	i= 0
	for query in reader.readlines():

		wiki_titles = wikipedia.search(query, results=10)
		print(query,"--- ",wiki_titles)
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
