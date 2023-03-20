import sys
import re
import html
from pathlib import Path
from flask_cors import CORS
from flask import Flask, render_template, request, jsonify
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from search_func import query_parse
from ranking import ir_rankings
from db.MongoDB import MongoDB
from trie_search.process_query import Query_Completion
from trie_search.trie_tree import Trie_Log, Trie_Hit
import urllib.parse
app = Flask(__name__)
app.jinja_env.variable_start_string = '[[['
app.jinja_env.variable_end_string = ']]]'
CORS().init_app(app)
mongoDB = MongoDB()

web_url = 'https://en.wikipedia.org/'

title_path = "titles.trie"
log_path = 'log.trie'
tl = Trie_Log(log_path)
th = Trie_Hit(title_path)
qc = Query_Completion(tl, th)

@app.route('/')
def index():
    """
    初始界面，启动flask服务器后运行出来的第一个网站
    """
    # 返回前端初始界面index.html
    return render_template('index.html')


@app.route('/wiki/<doc_id>/<title>', methods=['GET', 'POST'])
def wiki_introduce(doc_id, title):
    """
    --- 具体介绍界面
    前端-->后端:
    (1) title=str(name): 词条title,需将其转化为字符串
    后端-->前端：
    (1) title: 词条title，需转化为字符串
    (2) introduce: 词条具体介绍，需转化为字符串
    (3) web_url: 提前设定好的，后端提前设定好web_url的值为'https://en.wikipedia.org/'，
    "wiki.html"这个html文件中已设定好前端输出格式为web_url+"wiki/"+后端向前端传输的doc_id，
    比如Association football，前端输出格式就为"https://en.wikipedia.org/wiki/Association football"
    """
    if request.method == 'GET':
        doc_id = str(doc_id)
        title = str(title)
        # 数据库操作，获取数据库中匹配的词条
        # contents = ir_rankings.process_retrieved_doc_content(doc_id=doc_id)
        contents_list = ir_rankings.process_retrieved_doc_content(doc_id=doc_id)
        infos_list = []
        for i in contents_list:
            if re.match('^====.*====$', i):
                b0 = i.replace('====', '')
                infos_list.append({'b0': b0, 'code': 'b0'})
                continue
            if re.match('^===.*===$', i):
                b1 = i.replace('===', '')
                infos_list.append({'b1': b1, 'code': 'b1'})
                continue
            if re.match('^==.*==$', i):
                b2 = i.replace('==', '')
                infos_list.append({'b2': b2, 'code': 'b2'})
                continue
            else:
                infos_list.append({'info': i, 'code': 'b'})

        # infos = db_session.query(Infos).filter(Infos.title == title).first()
        # db_session.commit()
        # db_session.close()
        # query_title = str(infos.title)  # 词条title
        # query_introduce = str(infos.introduce)  # 词条介绍
        return render_template('wiki.html', title=title, web_url=web_url, infos_list=infos_list)

# 搜索结果界面
@app.route('/search/<query_str>', methods=['GET', 'POST'])
def search_results(query_str):
    """
    --- 搜索结果界面
    前端-->后端
    (1) title=str(name): 词条title,需将其转化为字符串
    (2) page_id: 作为分页id，比如100条记录，每页存储10条记录，第1页id就是1;当前端转到第2页, id自动变成2
    后端-->前端
    (1) infos_list=[{'title': title1, 'introduce':introduce1},
                    {'title': title2, 'introduce':introduce2},
                    ...]: 包含所有搜索结果的列表形式，每一个搜索结果用字典储存
        字典里每一项按顺序分别为词条title和词条introduce
    (2) len_number: 搜索结果的数量
    """
    query_str = urllib.parse.unquote(query_str)
    # GET操作，则显示搜索结果界面
    if request.method == 'GET':
        return render_template('show_index.html')

    # POST操作，将搜索结果内容显示在该界面上
    if request.method == 'POST':
        query_str = query_str.replace('%23', '#')
        page_id = int(request.get_json()['id'])
        # 数据库操作
        infos_list = query_parse.run_search(query=query_str, db=mongoDB)

        len_number = int(len(infos_list))
        # 第1页就是放搜索结果[0:10], 第2页[11:20]，以此类推
        infos_list = infos_list[(page_id * 10 - 10): (page_id * 10)]
        return jsonify({'input_value': query_str, 'infos_list': infos_list, 'len_number': len_number})

@app.route('/input_value', methods=['POST'])
def input_value():
    """
    -- 输入框输入query，并显示query suggestion
    前端-->后端
    (1) input_value: 在输入框中输入的query
    后端-->前端
    (1) infos_list=[{'title': title1}, {'title': title2}, ...]: 包含前10条模糊搜索的列表形式，每一个搜索结果用字典储存
        字典里包含词条title
    """
    if request.method == 'POST':
        input_value = str(request.get_json()['input_value'])
        # infos = db_session.query(Infos).filter().all()
        # db_session.commit()
        # db_session.close()
        infos_list = []
        # print(qc.trie_page_title.trie.suffixes('a'))
        infos_list = qc.get_info_list(input_value)
        # 不区分大小写，建议模糊搜索直接按照以下逻辑开发
        # e.g. 假如输入框输入ja，则直接从整个wikipedia的词条数据库中获得所有带ja的词条，并选取前10个显示在模糊提示框内
        # for i in infos:
        #     if input_value.lower() in i.title.lower() or input_value.upper() in i.title.upper():
        #         infos_list.append({'title': i.title})
                # infos_list.append({'title': i.title, 'introduce': i.introduce})
        infos_list = infos_list[0:10]
        return jsonify(infos_list)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=12001, debug=app.config["DEBUG"], threaded=False)
