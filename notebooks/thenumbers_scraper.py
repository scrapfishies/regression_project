from bs4 import BeautifulSoup
from os import path
import pandas as pd
import requests
import pickle
import time

def page_movie_bugets():
	movies_list = []
	
	for page in range(601):
		if page == 0:
			data = requests.get(f'https://www.the-numbers.com/movie/budgets/all')
		else:
			url = (page * 100) + 1
			data = requests.get(f'https://www.the-numbers.com/movie/budgets/all/{url}')
			print(f'processing {url}')

		# load data into bs4
		soup = BeautifulSoup(data.text, features="lxml")

		movie_table = soup.find('table')

		for row in movie_table.find_all('tr')[1:]:
			release_date = row.find_all('td')[1].find('a').text

			if len(release_date) < 8:
				continue

			if int(release_date.split(',')[-1]) < 2010:
				continue

			rank = row.find_all('td')[0].text
			title = row.find_all('td')[2].find('a').text
			budget = row.find_all('td')[3].text.replace(',','').strip()[1:]

			movies_list.append([rank, release_date, title, budget])

		if (page > 0) & (page % 10 == 0):
			print(f'page {(page * 100) + 1} complete, pausing')
			time.sleep(2)

	the_numbers_df = pd.DataFrame(movies_list, columns=['rank', 'release_date', 'title', 'tn_budget'])

	pickle.dump(the_numbers_df, open(f'data/the_numbers/budgets_df', 'wb'))

page_movie_bugets()