#!/user/bin/env python

import web
import json
import exceptions


urls = (
		'/', 'index',
		'/query', 'query',
		)

app = web.application(urls, globals())

db = web.database(dbn = 'mysql', user = 'root', pw = 'zs', db = 'recommender')

def queryfunc(userId):
	web.debug('query')
	result = db.select('user_item', vars = locals(), what = 'recommender', where = "userId = $userId")
	recommender = result[0]['recommender']
	web.debug("recommender:"+recommender)
	return recommender


class index:
	def GET(self):
		web.seeother('/query')

class query:
	def GET(self):
		try:
			userId = web.input().userId
			userId = int(userId)
			web.debug(userId)
			recommender = queryfunc(userId)
		except exceptions.AttributeError:
			return "please give userId"
		except exceptions.IndexError:
			return "we haven't record this userId"
		b = recommender.split(',')
		result = []
		for dictionary in b:
			item = dictionary.split(':')
			mydic = {}
			mydic[item[0]] = item[1]
			result.append(mydic)
		encodedjson = json.dumps(result)
		return encodedjson
		

if __name__ == "__main__":
	app.run()
