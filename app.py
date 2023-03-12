from flask import Flask, render_template, request, jsonify
from sqlalchemy_config.sqlalchemy_config import db_session, Infos
from flask_cors import CORS

app = Flask(__name__)

CORS().init_app(app)

url_wiki_introduce = 'http://127.0.0.1:4000/wiki/'


# 初始界面，启动flask服务器后运行出来的第一个网站
@app.route('/')
def index():
    # 返回前端初始界面index.html
    return render_template('index.html')


'''
--具体介绍界面
'''
@app.route('/wiki/<name>', methods=['GET', 'POST'])
def wiki_introduce(name):
    if request.method == 'GET':
        title = str(name)
        infos = db_session.query(Infos).filter(Infos.title == title).first()
        db_session.commit()
        db_session.close()
        query_title = infos.title
        query_introduce = infos.introduce
        # query_url = infos.url
        # return render_template('wiki.html', title=title, introduce=introduce, url=url)
        return render_template('wiki.html', title=str(query_title), introduce=str(query_introduce))


# 搜索结果界面
@app.route('/search/<name>', methods=['GET', 'POST'])
def search(name):
    if request.method == 'GET':
        return render_template('show_index.html')
    if request.method == 'POST':
        wiki = str(name)
        id = int(request.get_json()['id'])
        infos = db_session.query(Infos).filter().all()
        db_session.commit()
        db_session.close()
        infos_list = []
        for i in infos:
            if wiki.lower() in str(i.title).lower() or wiki.upper() in str(i.title).upper():
                infos_list.append({'title': i.title, 'introduce': i.introduce[0:200] + '...', 'url': i.url})
        len_number = int(len(infos_list))
        infos_list = infos_list[(id * 10 - 10):(id * 10)]
        return jsonify({'infos_list': infos_list, 'len_number': len_number})


# 输入框输入Query
@app.route('/input_value', methods=['POST'])
def input_value():
    if request.method == 'POST':
        input_value = request.get_json()['input_value']
        infos = db_session.query(Infos).filter().all()
        db_session.commit()
        db_session.close()
        infos_list = []
        for i in infos:
            if str(input_value).lower() in str(i.title).lower() or str(input_value).upper() in str(i.title).upper():
                infos_list.append({'title': i.title, 'introduce': i.introduce, 'url': url_wiki_introduce + i.title})
        infos_list = infos_list[0:10]

        return jsonify(infos_list)


if __name__ == '__main__':
    app.run(port=4000, debug=app.config["DEBUG"], threaded=False)
