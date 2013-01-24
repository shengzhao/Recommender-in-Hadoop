#!/usr/bin/env python
# -*- coding: utf-8 -*-

import web

render = web.template.render('/home/hadoop/git/webpy-app/templates/')
db = web.database(dbn='mysql', user='root', pw='zs', db='recommender')
urls = (
	 '/', 'index',
	 '/add', 'add'
	 )
app = web.application(urls, globals())
class index:
	def GET(self):
		todos = db.select('todo')
		return render.index(todos)

class add:
	def POST(self):
		i = web.input()
		n = db.insert('todo', title=i.title)
		raise web.seeother('/')

if __name__ == "__main__":
	web.wsgi.runwsgi = lambda func, addr=None: web.wsgi.runfcgi(func, addr)
	
	app.run()
