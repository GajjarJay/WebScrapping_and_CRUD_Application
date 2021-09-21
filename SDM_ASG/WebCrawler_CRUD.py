import pandas as pd
import numpy as np
import requests 
import json 
import csv
from pprint import pprint
from bs4 import BeautifulSoup as bs
from pymongo import MongoClient
from flask import Flask, jsonify, request, render_template, redirect
from requests.models import REDIRECT_STATI


URL = "https://www.rottentomatoes.com/top/bestofrt/top_100_comedy_movies/"

r = requests.get(URL)

table_content_list = []

#CODE TO SCRAPE THE DATA
if r.status_code == 200:
    rt = bs(r.text,"html.parser")
    tab = rt.find('table', attrs={'class':'table'})
#    print(tab.prettify())
    
    #get columns
    for data in tab.find_all('tr'):
        row_data = []
        
        #rank of the movie
        if data.find('td', attrs = {'class' : 'bold'}) is not None:
            rnk = data.find('td', attrs = {'class' : 'bold'})
            row_data.append(rnk.text.strip())
#            print("'" + rnk.text + "'")
            
        #Ratings of the movie    
        if data.find('span', attrs = {'class' : 'tMeterScore'}) is not None:
            rating = data.find('span', attrs = {'class' : 'tMeterScore'})
            row_data.append(rating.text.strip())
#            print("'" + rating.text.strip() + "'")
        
        #Name of the movie
        if data.find('a', attrs = {'class' : 'unstyled articleLink'}) is not None:
            title = data.find('a', attrs = {'class' : 'unstyled articleLink'})
            row_data.append(title.text.strip())
#            print("'" + title.text.strip() + "'")
            
        #Number of reviews
        if data.find('td', attrs = {'class' : 'right hidden-xs'}) is not None:
            review = data.find('td', attrs = {'class' : 'right hidden-xs'})
            row_data.append(review.text.strip())
#            print("'" + review.text + "'")
        
        table_content_list.append(row_data)


# Saving in CSV file using pandas dataframe
df = pd.DataFrame(table_content_list, columns=['Rank','Rating','Title','Reviews'])
df.to_csv("D:\Project\Python\SDM_ASG\Top_100_Comedy_Movie_list.csv")


''' **************************************************************************************************************** '''
''' ***************************************** CRUD OPERATIONS METHODS ********************************************** '''
''' **************************************************************************************************************** '''

#connection string of mongoDB
client = MongoClient('mongodb+srv://jay:haha1234@sdmcluster.6rfbh.mongodb.net/exchange?ssl=true&ssl_cert_reqs=CERT_NONE')
db = client.get_database('rottentomatoes')
records = db.movies

FilePath = r'C:\Users\JAY\Desktop\Georgian - BDAT\Semester 2\1007 - Social Data Mining Techniques\Web Scrapping'


def mongoInsert(var_rank_num, var_rating, var_title, var_reviews) :

    movie = {'Rank' : int(var_rank_num), 'Rating' : var_rating, 'Title' : var_title, 'Reviews' : int(var_reviews)} 
#    print(movie)
    db.movies.insert_one(movie)
    print('Insert complete.')


def mongoUpdate(var_rank_num, var_rating, var_title, var_reviews):
    
#    var_title = input('New Title : ')
    db.movies.update_one({'Rank' : int(var_rank_num)}, {'$set': {"Title" : var_title, "Rating" : var_rating, "Reviews" : int(var_reviews)}})
    selectedRecord = db.movies.find_one({'Rank': int(var_rank_num)})
#    pprint(selectedRecord)

    print('update complete.')

def mongoDelete(var_rank_num):

    db.movies.delete_many({'Rank': int(var_rank_num)})
    print('Delete complete.')

def mongoDocImport():
    #Path for CSV and JSON File
    dataFile = pd.read_csv(FilePath + '\Top_100_Comedy_Movie_list.csv')

    #droping null values if any
    dataFile.dropna(inplace=True)
    dataFile.drop(columns = ['Unnamed: 0'], inplace=True)
#    print(dataFile)

    dataFile.to_json('Top_100_Comedy_Movie_list - Copy.json', orient = 'records')

    #inserting json data to mongoDB
    with open('Top_100_Comedy_Movie_list - Copy.json') as file:
        file_data = json.load(file)

    if isinstance(file_data, list):
        records.insert_many(file_data)  
    else:
        records.insert_one(file_data)

def mongoDocExport():
    fields={}
    series_list=[]
    for doc in db.movies.find({}):
        for key,val in doc.items():
            try: fields[key] = np.append(fields[key], val)
            except KeyError: fields[key] = np.array([val])

    #print(fields)

    for key, val in fields.items():
        if key != "_id":
            fields[key] = pd.Series(fields[key])
            series_list += [fields[key]]

    #print(series_list)

    df_series = {}
    for num, series in enumerate(series_list):
        # same as: df_series["data 1"] = series
        df_series['data ' + str(num)] = series

    mongo_df = pd.DataFrame(df_series)
#    print ("\nmongo_df:", mongo_df)
    return mongo_df


mongoDocImport()


''' **************************************************************************************************************** '''
'''                                           FLASK CODE FOR WEB APPLICATION                                         '''
''' **************************************************************************************************************** '''


#Flask code / visualiation code
app = Flask(__name__)

@app.route('/',  methods = ['GET','POST'])
def RetrieveDataList():
    if request.method == 'POST':
        if request.form.get('get') == 'Get the full list of movies':
            return render_template('home.html')
        elif request.form.get('create') == 'Create a new entry':
            return render_template('CreateView.html')
        elif request.form.get('update') == 'Update the existing record':
            return render_template('UpdateView.html')
        elif request.form.get('delete') == 'Delete the record':
            return render_template('DeleteView.html')
        else:
            pass
    elif request.method == 'GET':
            dataset = mongoDocExport()
            dataset.columns = ['Rank', 'Rating', 'Title', 'Reviews']
            header = 'Top movies of Rotten tomatoes'
            return render_template('home.html', tables=[dataset.to_html(classes='data')], titles = dataset.columns )


@app.route('/create' , methods = ['GET','POST'])
def create():
    if request.method == 'GET':
        return render_template('CreateView.html')
 
    if request.method == 'POST':
        rank = request.form['rank']
        rating = request.form['rating']
        title = request.form['title']
        reviews = request.form['reviews']

        mongoInsert(rank, rating, title, reviews)
        
        return redirect('/')

@app.route('/delete', methods=['GET','POST'])
def delete():
    if request.method == 'GET':
        return render_template('DeleteView.html')

    if request.method == 'POST':
        rank = request.form['rank']
        mongoDelete(rank)
        return redirect('/')


@app.route('/update',methods = ['GET','POST'])
def update():
    if request.method == 'GET':
        return render_template('UpdateView.html')

    if request.method == 'POST':
        rank = request.form['rank']
        rating = request.form['rating']
        title = request.form['title']
        reviews = request.form['reviews']
        
        mongoUpdate(rank, rating, title, reviews)
        return redirect('/')


if __name__ == "__main__":
    app.run() 
