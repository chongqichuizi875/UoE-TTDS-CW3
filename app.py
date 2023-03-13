from flask import Flask, render_template, request, jsonify
from sqlalchemy_config.sqlalchemy_config import db_session, Infos
from flask_cors import CORS

app = Flask(__name__)

CORS().init_app(app)

url_search_results = 'http://127.0.0.1:5000/search/'
url_wiki_introduce = 'http://127.0.0.1:5000/wiki/'


@app.route('/')
def index():
    """
    初始界面，启动flask服务器后运行出来的第一个网站
    """
    # 返回前端初始界面index.html
    return render_template('index.html')


@app.route('/wiki/<name>', methods=['GET', 'POST'])
def wiki_introduce(name):
    """
    --- 具体介绍界面
    前端-->后端:
    (1) title=str(name): 词条title,需将其转化为字符串
    后端-->前端：
    (1) title: 词条title，需转化为字符串
    (2) introduce: 词条具体介绍，需转化为字符串
    """
    if request.method == 'GET':
        title = str(name)
        # 数据库操作，获取数据库中匹配的词条
        infos = db_session.query(Infos).filter(Infos.title == title).first()
        db_session.commit()
        db_session.close()
        query_title = infos.title  # 词条title
        query_introduce = infos.introduce  # 词条介绍
        # query_url = infos.url
        # return render_template('wiki.html', title=title, introduce=introduce, url=url)
        return render_template('wiki.html', title=str(query_title), introduce=str(query_introduce))


# 搜索结果界面
@app.route('/search/<name>', methods=['GET', 'POST'])
def search_results(name):
    """
    --- 搜索结果界面
    前端-->后端
    (1) title=str(name): 词条title,需将其转化为字符串
    (2) page_id: 作为分页id，比如100条记录，每页存储10条记录，第1页id就是1;当前端转到第2页, id自动变成2
    后端-->前端
    (1) infos_list=[{'title': title1, 'introduce':introduce1, 'url': url1},
                    {'title': title2, 'introduce':introduce2, 'url': url2},
                    ...]: 包含所有搜索结果的列表形式，每一个搜索结果用字典储存
        字典里每一项按顺序分别为词条title，词条introduce和词条url
        url格式为'http://127.0.0.1:5000/wiki/'+词条title，在前端点击链接后转到词条具体介绍界面
    (2) len_number: 搜索结果的数量
    """
    # GET操作，则显示搜索结果界面
    if request.method == 'GET':
        return render_template('show_index.html')
    # POST操作，将搜索结果内容显示在该界面上
    if request.method == 'POST':
        title = str(name)
        page_id = int(request.get_json()['id'])
        # 数据库操作
        infos = db_session.query(Infos).filter().all()
        db_session.commit()
        db_session.close()
        infos_list = []
        for i in infos:
            # 不管是大写小写都能搜索出结果
            if title.lower() in str(i.title).lower() or title.upper() in str(i.title).upper():
                infos_list.append({'title': i.title, 'introduce': i.introduce[0:200] + '...',
                                   'url': url_wiki_introduce + i.title})
        len_number = int(len(infos_list))
        # 第1页就是放搜索结果[0:10], 第2页[11:20]，以此类推
        infos_list = infos_list[(page_id * 10 - 10):(page_id * 10)]
        return jsonify({'infos_list': infos_list, 'len_number': len_number})


@app.route('/input_value', methods=['POST'])
def input_value():
    """
    -- 输入框输入query，并显示模糊搜索
    前端-->后端
    (1) input_value: 在输入框中输入的query
    后端-->前端
    (1) infos_list=[{'title': title1, 'introduce':introduce1, 'url': url1},
                    {'title': title2, 'introduce':introduce2, 'url': url2},
                    ...]: 包含前10条模糊搜索的列表形式，每一个搜索结果用字典储存
        字典里每一项按顺序分别为词条title，词条introduce和词条url
        url格式为'http://127.0.0.1:5000/search/'+词条title，在前端点击链接后转到搜索结果界面
    这部分考虑修改为，点击模糊搜索提示词，不是转到具体介绍界面，而是将模糊搜索词填充到输入框，然后点击搜索，跳转到搜索结果页面
    :return:
    """
    if request.method == 'POST':
        input_value = str(request.get_json()['input_value'])
        infos = db_session.query(Infos).filter().all()
        db_session.commit()
        db_session.close()
        infos_list = []
        # 不区分大小写，建议模糊搜索直接按照以下逻辑开发
        # e.g. 假如输入框输入ja，则直接从整个wikipedia的词条数据库中获得所有带ja的词条，并选取前10个显示在模糊提示框内
        for i in infos:
            if input_value.lower() in i.title.lower() or input_value.upper() in i.title.upper():
                infos_list.append({'title': i.title, 'introduce': i.introduce, 'url': url_search_results + i.title})
        infos_list = infos_list[0:10]

        return jsonify(infos_list)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=app.config["DEBUG"], threaded=False)
