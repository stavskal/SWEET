import json,csv,sys,os,psycopg2
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date
from collections import Counter

create = "CREATE TABLE {0} (time_stamp INT) ;"


# -----------------------------------------------------------------------------------
# This script has two functionalities:     
# Get received / sent email data and create features, provoked with: '-received' / '-sent'
# It also assumes that there is a folder called 'ContextData' in the same directory
# in which all the context data from the SWEET study are contained
# ----------------------------------------------------------------------------------


def splitTimeEmail(row):
	"""Applied to email dataframe columns in order to create day,hour columns"""
	month, day, rest_info = str(row).split('/')
	year, time, period = rest_info.split(' ')
	hours, minutes, seconds = time.split(':')
	dt_now = datetime(year=2015,month=int(month[-1:]),day=int(day),hour=int(hours), minute= int(minutes))
	timestamp = (dt_now - datetime(1970,1,1)).total_seconds()

	return(day, hours, period[:-1], timestamp)

def splitTimeStress(row):
	"""Applied to stress dataframe columns in order to create day,hour columns"""
	month, day, rest_info = str(row).split('/')
	year, time = rest_info.split(' ')
	hours, minutes, seconds = time.split(':')
	return(day, hours)

def statistical_features_rec(email_rec, email_sent, stress_df):
	"""Given a dataframe with emails for a participant,
	compute features for days on which we have stress responses """

	#Instantiating empty vectors for feature matrix creation
	feature_vectors = np.zeros([1,10])
	feature_days = list(stress_df.index)
	temp_feats = np.zeros([len(feature_days),10])
	ground_truth = np.zeros(len(feature_days))
	email_count , _ = email_rec.shape
	day_group = email_rec.groupby('day')
	
	days_dict ={}
	for i,day in enumerate(feature_days):
		days_dict[day]=i

	# Iterating throuh all days for which we have emails,
	# checking if there is a stress label for that day
	# If yes, proceed to feature calculation
	print(feature_days)
	for index,email in enumerate([email_rec,email_sent]):
		day_group = email.groupby('day')
		for day, group in day_group:
			print(day)
			if day in feature_days:
				# Emails count is equal to the sub-group's rows
				daily_email_count , _ = group.shape
				before_lunch , _ = (group[group['period']=='AM']).shape
				after_lunch , _ = (group[group['period']=='PM']).shape
				
				#Calculating percentage of daily mails received before / after lunch
				if daily_email_count:
					mail_feats = (float(before_lunch)/daily_email_count, float(after_lunch)/daily_email_count)
				else:
					mail_feats = (0,0)
				# Average time between consecutive mails for that day
				times = np.asarray(group['timestamp'])
				# sorry for huge one liner, python = swiss army knife
				mean_time_between = np.abs(np.mean([times[i] - times[i+1] for i in range(0,len(times)-1)]))

				# Bringing it all together, features and max_stress
				# Reshaping for concatenation purposes
				temp_feats[days_dict[day] , index*5 : 5+index*5] = np.array((mail_feats[0],mail_feats[1], mean_time_between, group['timestamp'].std(), daily_email_count))
				ground_truth[days_dict[day]] = stress_df['MAX_STRESS'].ix[str(day)]
	print(temp_feats)
	# returning without first row because it's dummy
	return(temp_feats, ground_truth,feature_days)

def statistical_features_sent(email_rec,stress_df):
	#feature_vectors = np.zeros([1,5])	
	#ground_truth = np.zeros([1])
	feature_days = list(stress_df.index)
	email_count , _ = email_rec.shape
	day_group = email_rec.groupby('day')

	#for day,group in day_group:
		#	if day in feature_days:
				


def process_stress(stress_df):
	"""Finds max stress value for each day and
	returns a dataframe containing this information
	"""
	day_group = stress_df.groupby('day')
	max_stress_list = []
	days_list = []
	for day, group in day_group:
		max_stress_list.append(group['MAXIMUM_STRESS'].max())
		days_list.append(day)

	max_stress_df = pd.DataFrame(data=max_stress_list,index=days_list, columns=['MAX_STRESS'])
	return(max_stress_df)





def main(argv):

	try:
		con = psycopg2.connect(database='sweet', user='tabrianos')
		cur = con.cursor()

	except psycopg2.DatabaseError as err:
		print('Error %s' % err)
		exit()

	# Initializing empty vectors to concatenate with 
	# feature vectors returned by functions. To be dumped afterwards
	X = np.zeros([1,10])
	Y = np.zeros([1])
	labels =[]
	feature_days =[]
	labels.append(0)
	feature_days.append(0)

	if sys.argv[1]=='-received':
		directories = os.path.dirname(os.path.abspath(__file__)) + '/ContextData'
		# items for directories list are the folders and also the 
		# unique IDs of participants in the SWEET study
		for user in os.listdir(directories):
			print(user)
			# Creating strings to load user files
			# Mails sent, mails received and reported stress (3)
			base_dir = directories + '/' + user +'/' +'Ilumivu'
			user_email_rec = base_dir +'/Mailrecipient.xls'
			user_email_sent = base_dir +'/Mailsender.xls'
			user_stress = base_dir +'/current-stress-merged.csv' 

			if not os.path.isfile(user_stress):
				continue

			stress_df = pd.read_csv(user_stress)

			if os.path.isfile(user_email_rec) and os.path.isfile(user_email_sent):
				email_rec = pd.read_excel(user_email_rec)
				email_sent = pd.read_excel(user_email_sent)

			 	# Create two columns by splitting the initial column data
			 	# This will help create features more easily
			 	email_rec['day'], email_rec['hour'], email_rec['period'], email_rec['timestamp'] = \
			 		zip(*email_rec['"Received"'].map(splitTimeEmail))


				email_sent['day'], email_sent['hour'], email_sent['period'], email_sent['timestamp'] = \
			 		zip(*email_sent['"Received"'].map(splitTimeEmail))
			 	# Many times the same email is sent to many people, thus multiple
			 	# entries exist for one timestamp. For now dropping them
			 	email_sent.drop_duplicates(subset='timestamp', inplace=True)

			 	# Although stress csv exists, some are empty. Ignoring them atm
			 	try:
			 		stress_df['day'], stress_df['hour'] = zip(*stress_df['ts'].map(splitTimeStress))
			 	except ValueError:
			 		continue
			 	# Computing maximum stress for each day
				max_stress_df = process_stress(stress_df)
				#Creating features
				user_feats, user_labels, temp_feature_days = statistical_features_rec(email_rec, email_sent ,max_stress_df)

				X = np.concatenate((X,user_feats),axis=0)
				Y = np.concatenate((Y,user_labels), axis=0)
				labels += [user for i in range(0,user_labels.shape[0])]
				feature_days += temp_feature_days
		


		print(X.shape,Y.shape, len(labels), len(feature_days))
		print(X[1,:], Y[1:15])

		# Getting indices of examples with 'nan' stress
		ynan = np.nan_to_num(Y)
		del_rows = np.where(ynan==0)[0]
		
		# Deleting rows that had 'Nan' stress, also dummy row
		Y = np.delete(Y,del_rows,axis=0)
		X = np.delete(X,del_rows,axis=0)
		labels = np.delete(np.array(labels),del_rows,axis=0)
		feature_days = np.delete(np.array(feature_days),del_rows,axis=0)
		print(X.shape,Y.shape, len(labels), len(feature_days))
		print(Counter(Y))
		exit()
		micolumns = pd.MultiIndex.from_tuples([('received','email_ratio_before_lunch'),
											('received','email_ratio_after_lunch'),
											('received','mean_time_between_mails'),
											('received','timestamp_std'),
											('received','total_emails'),
											('sent','email_ratio_before_lunch'),
											('sent','email_ratio_after_lunch'),
											('sent','mean_time_between_mails'),
											('sent','timestamp_std'),
											('sent','total_emails'),],names=['lvl0', 'lvl1'])
		stress_features = pd.DataFrame(data=X,columns=micolumns)
		stress_features['max_stress'] = pd.Series(data=Y)
		stress_features['user'] = pd.Series(data=labels)
		stress_features['day'] = pd.Series(data=feature_days)
		#print(stress_features.head())
		stress_features.to_csv('feature_matrix_all.csv')


		from sklearn.ensemble import RandomForestClassifier
		from sklearn.cross_validation import cross_val_score
		rf = RandomForestClassifier(n_estimators=100)
		X = np.nan_to_num(X)
		a = cross_val_score(rf,X,Y,cv=4)
		print(np.mean(a))
	#elif sys.argv[1]=='-sent':
		











if __name__ == '__main__':
	main(sys.argv[1:])
