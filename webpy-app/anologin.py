#!/usr/bin/env python
# -*- coding: utf-8 -*-

import web

web.config.debug = False

urls =  (
		'/', 'index',
		'/login', 'login',
		'/add', 'add',
		'/add_ano', 'add_ano',
		'/register', 'register',
		'/register_add', 'register_add',
		'/reset', 'reset',
		'/logout', 'logout',
		'/login_success', 'login_success',
		)
app = web.application(urls, globals())

render = web.template.render('/home/hadoop/git/Recommender-in-Hadoop/webpy-app/templates')

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

def RegisterJudge(name, pw, email, privilege):
	sequence_id = db.insert('example_users', user = name, password = pw, email = email, privilege = privilege)
	return True
def LoginCookie():
	try:
		a = web.cookies()
		web.debug(a)
		name = web.cookies().username
		password = web.cookies().password
		web.debug(name)
		web.debug(password)
		if LoginJudge(name, password):
			return True
		else:
			return False
	except:
		web.debug("cookie expire")
		return False

class index:
	def GET(self):
		web.debug("index")
#		return {'message': 'Hello', 'name': 'zhaosheng'}
		raise web.seeother('/login')

class login:
	def GET(self):
		if logged() or LoginCookie():
#			return render.login_suc()
			raise web.seeother('/login_success')
		else:
			return render.login()

class add:
	def POST(self):
		name, passwd = web.input().name, web.input().passwd
		if LoginJudge(name, passwd):
			session.login = 1
			# record the cookie
			web.setcookie('username', name, 60)
			web.setcookie('password', passwd, 60)
#			return render.login_suc()
			raise web.seeother('/login_success')
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
			web.debug(name)
			web.setcookie('username', name, 60)
			web.setcookie('password', pw, 60)
			a = web.cookies()
			web.debug(a)
#			return render.login_suc()
			raise web.seeother('/login_success')
		else:
			raise web.seeother('/register')

class logout:
	def GET(self):
		web.setcookie('username', "", -1)
		web.setcookie('password', "", -1)
		session.kill()
		raise web.seeother('/login')

class login_success:
	def GET(self):
		return render.login_suc()

if __name__ == "__main__":
#	web.wsgi.runwsgi = lambda func, addr = None: web.wsgi.runfcgi(func, addr)
	app.run()
