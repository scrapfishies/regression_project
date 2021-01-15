import requests
from bs4 import BeautifulSoup
import pandas as pd
import pickle
import os.path
from os import path
import math
import time

def movies_by_release_year(year):
	result = pd.DataFrame()

	# retrieve site data
	data = requests.get(f'https://www.boxofficemojo.com/year/{year}')

	# load data into bs4
	soup = BeautifulSoup(data.text, features="lxml")

	movie_table = soup.find('div', { 'id': 'table' }).find('div').find('table')

	for row in movie_table.find_all('tr')[1:]:
		title = row.find_all('td')[1].find('a').text
		link = row.find_all('td')[1].find('a').get('href').split('?')[0][9:-1]
		release_date = row.find_all('td')[8].text
		distributor = row.find_all('td')[9].text[:-2]
		result = result.append({'link': link, 'title': title, 'release_date': release_date, 'distributor': distributor}, ignore_index=True)	

	return result

def retrieve_movie_information(link, title, release_date, distributor):
	result = {}

	# retrieve site data
	data = requests.get(f'https://www.boxofficemojo.com/release/{link}')

	# load data into bs4
	soup = BeautifulSoup(data.text, features="lxml")


	domestic_gross = soup.find('div', { 'class': 'a-section a-spacing-none mojo-performance-summary-table' }).find('span', { 'class': 'money' }).text.replace(',','')[1:]

	metrics_table = soup.find('div', { 'class': 'a-section a-spacing-none mojo-summary-values mojo-hidden-from-mobile' })

	budget = metrics_table.find(text='Budget')
	if budget:
		budget = budget.parent.findNext('span').text.replace(',','')[1:]
	else:
		budget = None

	rating = metrics_table.find(text='MPAA')
	if rating:
		rating = rating.parent.findNext('span').text
	else:
		rating = None
	
	genre = metrics_table.find(text='Genres')
	if genre:
		genre = genre.parent.findNext('span').text.replace('\n    ',',').replace(',    ','').strip()
	else:
		genre = None
	
	release_duration = metrics_table.find(text='In Release')
	if release_duration:
		release_duration = release_duration.parent.findNext('span').text.split(' ')[0]
	else:
		release_duration = None
		
	result['title'] = title
	result['domestic_gross'] = domestic_gross
	result['distributor'] = distributor
	result['budget'] = budget
	result['release_date'] = release_date
	result['rating'] = rating
	result['genre'] = genre
	result['release_duration'] = release_duration

	# print(f'appending {result}')
	return result


def create_yearly_movies_list_dataframes(start_year, end_year):
	for year in range(end_year, start_year - 1, -1):
		yearly_movies_list_df = pd.DataFrame()

		if path.exists(f'../data/movies_list/{year}_movies_list_df'):
			print(f'{year}_movies_list_df exists')
		else:
			print(f'creating {year}_movies_list_df')
			yearly_movies_list_df = movies_by_release_year(year)
			pickle.dump(yearly_movies_list_df, open(f'../data/movies_list/{year}_movies_list_df', 'wb'))


def chunk_creation(yearly_movies_list_df, year, delay, chunks, chunk_size):
	columns=['title','domestic_gross', 'distributor', 'budget', 'release_date', 'rating', 'genre', 'release_duration']

	for number in range(chunks):
		if path.exists(f'../data/df_chunks/{year}_df_chunk_{number}'):
			print(f'{year}_df_chunk_{number} found')
		else:
			print(f'creating {year}_df_chunk_{number}')

			chunk_df = pd.DataFrame(columns=columns)

			start_pos = number * chunk_size
			end_pos = (number + 1) * chunk_size if number < 19 else None

			for idx, row in yearly_movies_list_df[start_pos:end_pos].iterrows():
				chunk_df = chunk_df.append(retrieve_movie_information(row.link, row.title, row.release_date, row.distributor), ignore_index=True)
				# if (idx % 25 ==0) and (idx > 0):
				# 	print(f'pausing at {idx}')
				# 	time.sleep(20)
			
			pickle.dump(chunk_df, open(f'../data/df_chunks/{year}_df_chunk_{number}', 'wb'))

			print(f'----- {year} chunk {number} complete -----')

			time.sleep(delay)

def create_yearly_dataframes(start_year, end_year):
	create_yearly_movies_list_dataframes(start_year, end_year)

	print(f'------------------')
	print(f'creating chunks')
	print(f'------------------')

	for year in range(end_year, start_year - 1, -1):
		if path.exists(f'../data/yearly_df/{year}_df'):
			print(f'{year}_df exists')
		else:
			with open(f'../data/movies_list/{year}_movies_list_df','rb') as read_file:
				yearly_movies_list_df = pickle.load(read_file)
			
			delay = 30
			chunks = 20
			chunk_size = math.floor(yearly_movies_list_df.title.count()/chunks)
			chunk_creation(yearly_movies_list_df, year, delay, chunks, chunk_size)

			print(f'\n')
			print(f'------------------')
			print(f'all chunks created')
			print(f'------------------')
			print(f'\n')
			print(f'------------------')
			print(f'creating {year}_df')
			print(f'------------------')

			# pickle.dump(result, open(f'../data/{year}_df', 'wb'))

			# print(f'created {year}_df')

		# print('Return merged dataframe here')

	# return df