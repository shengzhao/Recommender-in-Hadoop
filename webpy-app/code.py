import web

render = web.template.render('/home/hadoop/git/webpy-app/templates/')
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
		web.debug("test")
		i = web.input()
		n = db.insert('todo', title=i.title)
		raise web.seeother('/')
application = web.application(urls, globals()).wsgifunc()
#if __name__ == "__main__":
#	app = web.application(urls, globals())
#	app.run()
