import web
import render_mako

web.config.debug = False
db = web.database(dbn='mysql', user='root', pw='zs', db='recommender')
urls = (
		    "/login", "login",
		    "/reset", "reset"
		)
app = web.application(urls, locals())

# judge the user login or not
def logged():
	if session.login==1:
		return True
	else:
		return False

# judge the category of user
def create_render(privilege):
	if logged():
		if privilege==0:
			render = render_mako(
		 			directories=['templates/reader'],
					input_encoding='utf-8',
					output_encoding='utf-8',
					)
		elif privilege==1:
			render = render_mako(
					directories=['templates/user'],
					input_encoding='utf-8',
					output_encoding='utf-8',
					)
		elif privilege==2:
			render = render_mako(
					directories=['templates/admin'],
					input_encoding='utf-8'
					output_encoding='utf-8'
					)
	else:
		render = render_mako(
				directories=['templates/commus'],
				input_encoding='utf-8',
				output_encoding='utf-8',
				)
	return render


class login:
	def GET(self):
		if logged():
			render = create_render(session.privilege)
			return "%s" % (
					render.login_double()               )
		else:
			render = create_render(session.privilege)
			return "%s" % (
					render.login()		                )
	def POST(self):
		user, passwd = web.input().user, web.input().passwd
		ident = db.query("select * from example_user where user = '%s'" %(user)).getresult()
		try:
			if passwd==ident[0][2]:
				session.login=1
				session.privilege=ident[0][4]
				render = create_render(session.privilege)
	 			return "%s" %(render.login_ok())
			else:
	 			session.logon=0
				session.provolege=0
				render = create_render(session.privilege)
				return "%s" %(render.login_error())
		except:
			session.login=0
			session.privilege=0
			render = create_render(session.privilege)
			return "%s" %(render.login_error())

class reset:
	def GET(self):
		session.kill()
		return "%s" %(render.login())

if __name__ == "__main__":
	app.run()
