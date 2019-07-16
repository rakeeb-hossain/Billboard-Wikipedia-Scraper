import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool
import timeit
import csv

def get_metadata(link):
    if (link == "none"):
        return ["N/A", "N/A", "N/A"]
    
    song_html = requests.get("https://en.wikipedia.org" + link)
    song_soup = BeautifulSoup(song_html.text, "html.parser")
    table = song_soup.find("table", {"class":"infobox vevent"})  
    
    metadata = []
    try:
        #Genre data
        data = table.find("a", {"href": "/wiki/Music_genre"})
        if (data != None):
            data = data.find_next('td')
            j = data.findAll('a')    
            metadata.append(str(j[0].contents[0]))
        else:
            metadata.append("N/A")
            
        #Label data
        data = table.find("a", {"href":"/wiki/Record_label"})
        if (data != None):
            data = data.find_next('td')
            j = data.findAll('a')
            try:
                metadata.append(str((j[0].contents[0]).replace("\n", ", ").replace("[1]", "").replace("[2]", "")))  
            except:
                metadata.append(str((data.text.strip()).replace("\n", ", ").replace("[1]", "").replace("[2]", "")))
        else:
            metadata.append("N/A")
            
        #Producer data
        data = table.find("a", {"href":"/wiki/Record_producer"})
        if (data != None):
            data = data.find_next('td')
            j = data.findAll('a')
            try:
                metadata.append(str((j[0].contents[0]).replace("\n", ", ").replace("[1]", "").replace("[2]", "")))  
            except:
                metadata.append(str((data.text.strip()).replace("\n", ", ").replace("[1]", "").replace("[2]", "")))
        else:
            metadata.append("N/A")
    
        return metadata
    except:
        return ["N/A", "N/A", "N/A"]

def get_artist_info(link):
    if (link == "none"):
        return ["N/A", ""]

    artist_html = requests.get("https://en.wikipedia.org" + link)
    artist_soup = BeautifulSoup(artist_html.text, "html.parser")
    exp = (artist_soup.find("p", {"class": ""})).text
    country = ["American", "Canadian", "English", "England", "U.K.", "London"]
    min = float('inf')
    min_index = -1
    for index in range (0, 6):
        e = exp.find(country[index])
        if (e != -1 and e < min):
            min = e
            min_index = index
    
    if (min_index == 0):
        origin = "United States"
    
    if (min_index == 1):
        origin = "Canada"
    
    if (min_index > 1):
        origin = "UK"
    
    if (min_index == -1):
        origin = "Other"
    
    exp = exp.lower()
    gender_list = [" he ", " his", " she ", " her ", " group ", " band ", " duo "]
    min = float('inf')
    min_index = -1
    for index in range (0, 7):
        e = exp.find(gender_list[index])
        if (e != -1 and e < min):
            min = e
            min_index = index
    
    if (min_index <= 1):
        gender = "Male"
    elif (min_index == 2 or min_index == 3):
        gender = "Female"
    elif (min_index >= 4):
        gender = "Group"
    
    
    if (min_index == -1):
        try:
            exp = (artist_soup.findAll("p", {"class": ""}))
            ex = exp[1].text.lower()                

            for index in range (0, 7):
                e = ex.find(gender_list[index])
                if (e != -1 and e < min):
                    min = e
                    min_index = index
            if (min_index <= 1):
                gender = "Male"
            elif (min_index == 2 or min_index == 3):
                gender = "Female"
            elif (min_index >= 4):
                gender = "Group"
            if (min_index == -1):
                gender = ""    
        except:
            gender = ""

    return [origin, gender]
    
def retrieve_data(year):
    start = timeit.default_timer()    
    html = requests.get("https://en.wikipedia.org/wiki/Billboard_Year-End_Hot_100_singles_of_"+str(year)+"?fbclid=IwAR0kduRTx2RfxBGp3863OD1zNErelkCt0nf1vSDr5N4RkbUKEmyVv7ULYjM")
    soup = BeautifulSoup(html.text, "html.parser")

    #Scrape table for song names, artist names, link to wiki articles for songs (for more processing)
    my_table = (soup.find("table",{"class":"wikitable sortable"})).find("tbody")
    song_links = []
    artist_links = []
    song_names = []
    artist_names = []
    count = 0
    if (int(year) >= 1982):
        for i in my_table.findAll('td'): 
            j = i.findAll('a')
            if count % 2 == 0:
                try: 
                    song_links.append(j[0].get('href'))
                    song_names.append(j[0].contents[0]) 
                except:
                    song_links.append("none")
                    song_names.append(i.contents[0])
            else:
                try:
                    artist_links.append(j[0].get('href'))
                    artist_names.append((i.text.strip()).replace("\n", ", "))
                except:
                    artist_links.append("none")
                    artist_names.append((i.text.strip()).replace("\n", ", "))
            count += 1
    else:
        for i in my_table.findAll('td'): 
            j = i.findAll('a')
            if count % 3 == 1:
                try: 
                    song_links.append(j[0].get('href'))
                    song_names.append(j[0].contents[0])
                except:
                    song_links.append("none")
                    song_names.append(i.contents[0])
            elif count % 3 == 2:
                try:
                    artist_links.append(j[0].get('href'))
                    artist_names.append((i.text.strip()).replace("\n", ", "))
                except:
                    artist_links.append("none")
                    artist_names.append((i.text.strip()).replace("\n", ", "))
            count += 1        

    #Create multiple threads to parse through links for information
    with Pool(10) as p:
        song_info = p.map(get_metadata, song_links)
        
    with Pool(10) as c:
        artist_info = c.map(get_artist_info, artist_links)
    
    #Runtime
    stop = timeit.default_timer()
    print("Time: ", stop-start)
    
    #Output information to CSV file
    with open("/Users/rakeeb/Documents/billboard_data/"+str(yr)+".csv", mode='w', encoding='utf-8') as csv_file:
        #fieldnames = ["Year End Chart Position", "Artist", "Song", "Genre", "Producer(s)", "Label", "Country of Origin"]
        writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for i in range(0, 100):
            writer.writerow([str(i+1), artist_names[i], song_names[i], song_info[i][0], artist_info[i][1], song_info[i][2], song_info[i][1], artist_info[i][0]])    

if __name__ == '__main__':
    #Get html request and parse html
    for yr in range (1960, 2019):
        retrieve_data(yr)
    
