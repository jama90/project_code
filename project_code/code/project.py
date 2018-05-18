import csv
import multiprocessing as mp
import pandas as pd
import time
import math
import numpy as np
import os
import argparse
import glob


parser = argparse.ArgumentParser()
parser.add_argument('directory', type=str)
parser.add_argument('longtiude', type=str)
parser.add_argument('latitude', type=str)
parser.add_argument('distance_in_kilometer', type=str)

args = parser.parse_args()





def havetstine(f,q):
	start=time.clock()
	dict1=[]
	f_file = pd.read_csv(f,header=None, sep='\t',low_memory=False, error_bad_lines=False,  names=["GLOBALEVENTID","DATE","MonthYear",	"Year","FractionDate","Actor1Code","Actor1Name"	,"Actor1CountryCode","Actor1KnownGroupCode","Actor1EthnicCode",
	"Actor1Religion1Code",	"Actor1Religion2Code",	"Actor1Type1Code","Actor1Type2Code","Actor1Type3Code","Actor2Code","Actor2Name","Actor2CountryCode",
"Actor2KnownGroupCode",	"Actor2EthnicCode","Actor2Religion1Code","Actor2Religion2Code",	"Actor2Type1Code","Actor2Type2Code","Actor2Type3Code","IsRootEvent",
"EventCode","EventBaseCode","EventRootCode","QuadClass","GoldsteinScale","NumMentions",	"NumSources","NumArticles","AvgTone","Actor1Geo_Type","Actor1Geo_FullName",
"Actor1Geo_CountryCode","Actor1Geo_ADM1Code","Actor1Geo_Lat","Actor1Geo_Long","Actor1Geo_FeatureID","Actor2Geo_Type","Actor2Geo_FullName","Actor2Geo_CountryCode",
"Actor2Geo_ADM1Code","Actor2Geo_Lat","Actor2Geo_Long","Actor2Geo_FeatureID","ActionGeo_Type","ActionGeo_FullName","ActionGeo_CountryCode","ActionGeo_ADM1Code",
"ActionGeo_Lat","ActionGeo_Long","ActionGeo_FeatureID","DATEADDED","SOURCEURL"])
	f_file = f_file.fillna(0.0)

	latit2=(f_file.ActionGeo_Lat)
	longt2=(f_file.ActionGeo_Long)
	ID_num=f_file.GLOBALEVENTID
	url=f_file.SOURCEURL
	Goldsteinscore=f_file.GoldsteinScale

	#print('================it is saving the contents into the output file please check================')

	longtude1, latitude1, longtude2, latitude2 = map(np.radians, [longt1, latit1, longt2, latit2])
	RLON = longtude2 - longtude1
	RLAT = latitude2 - latitude1
	a = np.sin(RLAT/2.0)**2 + np.cos(latitude1) * np.cos(latitude2) * np.sin(RLON/2.0)**2
	c = 2*np.arcsin(np.sqrt(a))
	km =6371*c

	for j,i in enumerate(km):
		if(i<distance):
			#q.put([ID_num[j],Goldsteinscore[j],url[j],km[j],])
			dict1.append([ID_num[j],Goldsteinscore[j],url[j],])
			print(dict1)	
			#q.put(dict1)				 

	

if __name__ == '__main__':
	distance=float(args.distance_in_kilometer)
	longt1=float(args.longtiude)
	latit1=float(args.latitude)
	q=mp.Queue()
	p =[mp.Process(target=havetstine, args =(f,q)) for f in glob.glob(os.path.join(args.directory,'*'))]
	for i in p:
		i.start()

	#with open('output.csv','w') as fp:
	#	while True:
	#		item=q.get()
	#		if item is None:
	#			break
	#		fp.write('%s\n' % str(item))
		    
	for pp in p:
		pp.join()
print ("List processing complete.")



