#!/usr/bin/env python
# -*- coding: utf-8 -*-

import web

urls = (
		'/', 'index',
		'/login', 'login',
		'/add', 'add',
		'/add_ano', 'add_ano',
		'/register', 'register',
		'/register_add', 'register_add',
		)
app = web.application(urls, globals())

render = web.template.render('/home/hadoop/git/webpy-app/templates')

db = web.database(dbn = 'mysql', user = 'root', pw = 'zs', db = 'recommender')

def LoginJudge(name, pw):
	web.debug(name)
	result = db.select( 'example_users', vars = locals(), what = "password", where = "user = $name")
	password = result[0]['password']
	web.debug(password)
	if pw == password:
		return True
	else:
		return False

def RegisterJudge(name, pw, email, privilege):
	sequence_id = db.insert('example_users', user = name, password = pw, email = email, privilege = privilege)
	web.debug(sequence_id)
	return True

class index:
	def GET(self):
		raise web.seeother('/login')

class login:
	def GET(self):
		return render.login()

class add:
	def POST(self):
		name, passwd = web.input().name, web.input().passwd
		web.debug(web.input())
		web.debug(passwd)
		if LoginJudge(name, passwd):
			return render.login_suc()
		else:
			raise web.seeother('/login')

class add_ano:
	def POST(self):
		raise web.seeother('/register')

class register:
	def GET(self):
		return render.register()

class register_add:
	def POST(self):
		name, pw, email, privilege = web.input().name, web.input().pw, web.input().email, web.input().privilege
		if RegisterJudge(name, pw, email, privilege):
			return render.login_suc()
		else:
			raise web.seeother('/register')

if __name__ == "__main__":
#	web.wsgi.runwsgi = lambda func, addr = None: web.wsgi.runfcgi(func, addr)
	app.run()
