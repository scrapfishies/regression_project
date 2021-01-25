from bs4 import BeautifulSoup
from os import path
import pandas as pd
import requests
import pickle
import time
import re

def all_movies_for_year(year):
	# initialize primary list
	movie_list = []

	# retrieve data for movies by year
	data = requests.get(f'https://www.boxofficemojo.com/year/{year}')

	# load data into bs4
	soup = BeautifulSoup(data.text, features="lxml")

	movie_table = soup.find('div', { 'id': 'table' }).find('div').find('table')

	for row in movie_table.find_all('tr')[1:]:
		title = row.find_all('td')[1].find('a').text
		link = row.find_all('td')[1].find('a').get('href').split('?')[0][9:-1]
		distributor = row.find_all('td')[9].text[:-2]
		movie_list.append([link, title, distributor])

	return pd.DataFrame(movie_list, columns=['link', 'title', 'distributor'])

def generate_yearly_movies_index(start_year, end_year):
	for year in range(end_year, start_year - 1, -1):
		if path.exists(f'../data/yearly_index/{year}_index'):
			continue
		else:
			print(f'creating {year}_index')
			yearly_movies_index = all_movies_for_year(year)
			pickle.dump(yearly_movies_index, open(f'../data/yearly_index/{year}_index', 'wb'))

def retrieve_movie_information(link, title, distributor):
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

	release_date = metrics_table.find(text=re.compile('Release Date'))
	if release_date:
		release_year = release_date.parent.findNext('span').text.split(' - ')[0][-4:]
		release_date = release_date.parent.findNext('span').text.split(',')[0]
	else:
		release_date = None
		release_year = None

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

	return [title,domestic_gross,distributor,budget,release_date,release_year,rating,genre,release_duration]

def generate_movies_2d_list(yearly_movies_list_df):
	year_list = []

	for idx, row in yearly_movies_list_df.iterrows():
		if (idx > 0) & (idx % 25 == 0):
			print(f'Pausing at idx {idx}')
			time.sleep(5)
			print('Resuming')
		year_list.append(retrieve_movie_information(row.link, row.title, row.distributor))
	
	return year_list

def generate_yearly_lists(start_year, end_year):
	for year in range(end_year, start_year - 1, -1):
		if path.exists(f'../data/yearly_scraped_movie_info/{year}_df'):
			continue
		else:
			if path.exists(f'../data/yearly_2d_lists/{year}_list'):
				continue
			else:
				with open(f'../data/yearly_index/{year}_index','rb') as read_file:
					year_index = pickle.load(read_file)
				
				print(f'creating 2d array - {year}')

				year_list = generate_movies_2d_list(year_index)
				
				pickle.dump(year_list, open(f'../data/yearly_2d_lists/{year}_list', 'wb'))

def generate_yearly_dataframes(start_year, end_year):
	columns = ['title','domestic_gross', 'distributor', 'budget', 'release_date', 'release_year', 'rating', 'genre', 'release_duration']

	generate_yearly_movies_index(start_year, end_year)

	generate_yearly_lists(start_year, end_year)

	for year in range(end_year, start_year - 1, -1):
		if path.exists(f'../data/yearly_scraped_movie_info/{year}_df'):
			continue
		else:
			with open(f'../data/yearly_2d_lists/{year}_list','rb') as read_file:
				year_list = pickle.load(read_file)

			df = pd.DataFrame(year_list, columns=columns)

			pickle.dump(df, open(f'../data/yearly_scraped_movie_info/{year}_df', 'wb'))

	if path.exists(f'../data/yearly_scraped_movie_info/combined_df'):
		print('combined_df exists')
	else:
		combined_list = []
		
		for year in range(end_year, start_year - 1, -1):
			with open(f'../data/yearly_2d_lists/{year}_list','rb') as read_file:
				year_list = pickle.load(read_file)

			combined_list = combined_list + year_list

		combined_df = pd.DataFrame(combined_list, columns = columns)

		pickle.dump(combined_df, open(f'../data/yearly_scraped_movie_info/combined_df', 'wb'))

def movies_by_yearly_season(start_year, end_year):
	# initialize primary list
	movie_list = []

	for year in range(end_year, start_year - 1, -1):
		seasons = ['winter', 'spring', 'summer', 'fall', 'holiday']

		for season in seasons:
			data = requests.get(f'https://www.boxofficemojo.com/season/{season}/{year}')

			# load data into bs4
			soup = BeautifulSoup(data.text, features="lxml")

			movie_table = soup.find('div', { 'id': 'table' }).find('div').find('table')

			for row in movie_table.find_all('tr')[1:]:
				movie_id = row.find_all('td')[1].find('a').text + f'_{year}'
				movie_list.append([movie_id, season])

			print(f'{year} {season} complete')

			print(f'movie_list size is now - {len(movie_list)}')

	movies_by_season = pd.DataFrame(movie_list, columns = ['movie_id','season'])

	pickle.dump(movies_by_season, open(f'data/movies_by_season/seasons_df', 'wb'))

movies_by_yearly_season(2010, 2019)