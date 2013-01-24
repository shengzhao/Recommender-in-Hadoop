#!/usr/bin/env python
# -*- coding: utf-8 -*-

import web

web.config.debug = False

urls =  (
		'/', 'index',
		'/login', 'login',
		'/add', 'add',
		'/reset', 'reset',
		'/logout', 'logout',
		)
app = web.application(urls, globals())

render = web.template.render('/home/hadoop/git/webpy-app/templates')

session = web.session.Session(app, web.session.DiskStore('sessions'), initializer={'login': 0})

db = web.database(dbn = 'mysql', user = 'root', pw = 'zs', db = 'recommender')

def logged():
	if session.login == 1:
		return True
	else:
		return False

def LoginJudge(name, password):
	result = db.select('example_users', vars = locals(), what = 'password', where = "user = $name")
	ps = result[0]['password']
	if ps == password:
		return True
	else:
		return False

class index:
	def GET(self):
		raise web.seeother('/login')

class login:
	def GET(self):
		if logged():
			return render.login_suc()
		else:
			return render.login()

class add:
	def POST(self):
		name, passwd = web.input().name, web.input().passwd
		if LoginJudge(name, passwd):
			session.login = 1
			return render.login_suc()
		else:
			raise web.seeother('login')

if __name__ == "__main__":
#	web.wsgi.runwsgi = lambda func, addr = None: web.wsgi.runfcgi(func, addr)
	app.run()
