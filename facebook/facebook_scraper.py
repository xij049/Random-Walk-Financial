import json, sqlite3, time, datetime, requests, random

APP_SECRET = "a9ed7cbddee855e15af2439c83ec62cd"
APP_ID = "1632784396962207"
access_token = APP_ID + "|" + APP_SECRET

proxies = [{'http://54.218.105.164':'http://205.234.153.91:1026'},{'http://54.218.105.164':'http://205.234.153.91:1027'},
		{'http://54.218.105.164':'http://205.234.153.91:1028'},{'http://54.218.105.164':'http://205.234.153.91:1029'},
		{'http://54.218.105.164':'http://205.234.153.91:1030'}]

headers = [{'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; WOW64)'},{'User-Agent' : 'AppleWebKit/537.36 (KHTML, like Gecko)'},
			{'User-Agent' : 'Chrome/49.0.2623.110'},{'User-Agent' : 'Safari/537.36'}]

def load_user_agents(proxy):
	num = random.randint(1,len(proxy)) - 1
	return proxy[num]
def load_user_headers(head):
	num = random.randint(1,len(head)) - 1
	return head[num]

def request_until_succeed(url):
	print url
	success = False
	while success is False:
		try:
			response = requests.get(url,headers=load_user_headers(headers), proxies=load_user_agents(proxies),timeout=10)
			if response.status_code == 200:
				success = True
		except Exception, e:
			print e
			time.sleep(5)
			print "Error for URL %s: %s" %(url, datetime.datetime.now())
	return response.content

def getFacebookPostComment(next):
	# construct the URL string
	base = next.split('?')[0] + '?'
	key = 'access_token='+access_token
	parameters = "&pretty=1&summary=true&limit=100&order=chronological"
	url = base + key + parameters
	data = json.loads(request_until_succeed(url))
	return data

def getFacebookPageFeedData(company, num_statuses):
	base = 'https://graph.facebook.com/v2.6'
	node = '/' + company + '/feed'
	parameters = '/?fields=from,message,link,created_time,type,name,id,reactions.summary(true),comments.limit(1).summary(true),shares&limit=%s&access_token=%s'  %(num_statuses, access_token)
	url = base + node + parameters

	data = json.loads(request_until_succeed(url))
	return data

def safe_encode(obj):
	try:
		return obj.encode('utf-8')
	except:
		return str(obj)

def processFacebookPageFeedStatus(status):
	status_id = '' if 'id' not in status.keys() else status['id'].encode('utf-8')
	message = '' if 'message' not in status.keys() else status['message'].encode('utf-8')
	created_date = datetime.datetime.strptime(status['created_time'],'%Y-%m-%dT%H:%M:%S+0000').strftime('%m/%d/%Y')
	type_name = '' if 'type' not in status.keys() else  status['type'].encode('utf-8')
	status_link = '' if 'link' not in status.keys() else status['link'].encode('utf-8')
	num_likes = 0 if 'reactions' not in status.keys() else status['reactions']['summary']['total_count']
	num_comments = 0 if 'comments' not in status.keys() else status['comments']['summary']['total_count']
	num_shares = 0 if 'shares' not in status.keys() else status['shares']['count']
	return (status_id, message, created_date, type_name, status_link ,int(num_likes), int(num_comments), int(num_shares))

def processFacebookPostComment(comment):
	status_published = datetime.datetime.strptime(comment['created_time'],'%Y-%m-%dT%H:%M:%S+0000')
	status_published = status_published.strftime('%m/%d/%Y')
	user_id = '' if 'from' not in comment.keys() else safe_encode(comment['from']['id'])
	user_name = '' if 'from' not in comment.keys() else safe_encode(comment['from']['name'])
	message = '' if 'message' not in comment.keys() else safe_encode(comment['message'])
	comment_id = '' if 'id' not in comment.keys() else safe_encode(comment['id'])
	return (comment_id,message,user_id,user_name,status_published)

def scrapeFacebookPostComment(next_page):
	has_next_page = True
	num_processed = 0
	record = []
	comments = getFacebookPostComment(next_page)
	while has_next_page:
		for comment in comments['data']:
			record.append(processFacebookPostComment(comment))
			if 'next' in comments['paging'].keys():
				comments = json.loads(request_until_succeed(comments['paging']['next'].replace('\u00257C','|')))
			else:
				has_next_page = False
	return record

def scrapeFacebookPageFeedStatus(company_name, company_id, end_date):
	num_processed = 0
	has_next_page = True
	scrape_starttime = datetime.datetime.now()
	print "-----------------------------------------------"
	print "Scraping %s Facebook Page: %s\n" % (company_name, scrape_starttime)
	statuses = getFacebookPageFeedData(company_name, 100)
	conn = sqlite3.connect('Facebook.db')
	conn.text_factory = str
	cur = conn.cursor()
	while has_next_page:
		for status in statuses['data']:
			st = processFacebookPageFeedStatus(status)
			date = st[2].split('/')
			if status['from']['id'].encode('utf-8') == str(company_id):
				if datetime.date(int(date[2]),int(date[0]),int(date[1])) < end_date:
					cur.close()
					conn.close()
					print "\nDone!\n%s Reviews Processed in %s" % (num_processed, datetime.datetime.now() - scrape_starttime)
					print "-----------------------------------------------"  
					return
				cur.execute('''INSERT OR IGNORE INTO Type (name) VALUES (?)''',(st[3],))
				cur.execute('''SELECT id FROM Type WHERE name = ?''', (st[3],))
				type_id = cur.fetchone()[0]
				##################################################################
				if (st[6] > 0) and 'paging' in status['comments']:
					if 'next' in status['comments']['paging']:
						comment_link = status['comments']['paging']['next']
						comments = scrapeFacebookPostComment(comment_link)
						for ct in comments:
						# cur.execute('''INSERT OR IGNORE INTO User(id,name)
						# 	VALUES (?,?)''',(ct[2],ct[3]))
							cur.execute('''INSERT OR IGNORE INTO Comment (id,message,created_date,post_id,user_id,company_id)
								VALUES (?,?,?,?,?,?)''',(ct[0],ct[1],ct[4],st[0],ct[2],company_id))

				###################################################################
				cur.execute('''INSERT OR REPLACE INTO Official_Post
					(id, message, created_date, type_id, link, total_likes, total_comments, total_shares, company_id)
					VALUES (?,?,?,?,?,?,?,?,?)''', (st[0].split('_')[1],st[1],st[2],type_id,st[4],st[5],st[6],st[7],company_id))
				num_processed += 1
			
				if num_processed % 1000 == 0:
					print "%s Products Info. Processed: %s" % (num_processed, datetime.datetime.now())
		conn.commit()
		if 'paging' in statuses.keys():
			statuses = json.loads(request_until_succeed(statuses['paging']['next']))
		else:
			has_next_page = False
			cur.close()
			conn.close()
	print "\nDone!\n%s Statuses Processed in %s" % (num_processed, datetime.datetime.now() - scrape_starttime)
	print "-----------------------------------------------"
	return

def main():
	conn = sqlite3.connect('Facebook.db')
	conn.text_factory = str
	cur = conn.cursor()
	cur.execute('''SELECT name,id FROM Company WHERE id = ?''',(24902886692,))
	data = cur.fetchall()
	cur.close()
	conn.close()
	raw_date = str(raw_input('Enter end_date format (yyyy/mm/dd): '))
	end_date = datetime.date(int(raw_date.split('/')[0]),int(raw_date.split('/')[1]),int(raw_date.split('/')[2]))
	print len(data)
	for company in data:
		scrapeFacebookPageFeedStatus(company[0],company[1],end_date)


if __name__ == '__main__':
	main()