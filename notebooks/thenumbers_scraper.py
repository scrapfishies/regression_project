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

		# load data into bs4
		soup = BeautifulSoup(data.text, features="lxml")

		movie_table = soup.find('table')

		for row in movie_table.find_all('tr')[1:]:
			valid_check = row.find_all('td')[1].find('a').text

			if len(valid_check) < 8:
				continue

			if int(valid_check.split(',')[-1]) < 2010:
				continue

			release_date = row.find_all('td')[1].find('a').text.split(',')[0]
			release_year = row.find_all('td')[1].find('a').text.split(',')[1].strip()
			rank = row.find_all('td')[0].text
			title = row.find_all('td')[2].find('a').text
			tn_budget = row.find_all('td')[3].text.replace(',','').strip()[1:]

			movies_list.append([rank, release_date, release_year, title, tn_budget])

		if (page > 0) & (page % 30 == 0):
			time.sleep(5)

	print('Scrape Complete, saving file')

	the_numbers_df = pd.DataFrame(movies_list, columns=['rank', 'tn_release_date', 'tn_release_year', 'tn_title', 'tn_budget'])

	pickle.dump(the_numbers_df, open(f'data/the_numbers/budgets_df', 'wb'))

page_movie_bugets()