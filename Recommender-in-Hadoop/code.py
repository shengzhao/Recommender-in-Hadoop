import web

render = web.template.render('var/www/webpy-app/templates/')
db = web.database(dbn='mysql', user='root', pw='zs', db='recommender')
urls = (
	 '/', 'index',
	 '/add', 'add'
	 )
class index:
	def GET(self):
		todos = db.select('todo')
		return render.index(todos)

class add:
	def POST(self):
		i = web.input()
		n = db.insert('todo', title=i.title)
		raise web.seeother('/')
application = web.application(urls, globals()).wsgifunc()
