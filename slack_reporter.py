# -*- coding: utf-8 -*-
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from flask import Flask
from flask_apscheduler import APScheduler
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func
import mysql.connector as mariadb
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from slackclient import SlackClient
import logging
from logging.handlers import RotatingFileHandler
from flask_migrate import Migrate
import datetime

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///flask.db'
db = SQLAlchemy()
migrate = Migrate(app, db)


database_config = {"db alias":["db host","db user","db password", "database name"],
                    }

slack_token = "...."


class Report(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(80))
    db_name = db.Column(db.String(20))
    query_string = db.Column(db.Text)
    pub_date = db.Column(db.DateTime(timezone=True), server_default=func.now())
    is_active = db.Column(db.Boolean, server_default='t', default=True)
    order = db.Column(db.Integer)

    def __repr__(self):
        return '<Report %r> %s %s' % (self.title, self.db_name, self.query_string)


def make_report():
    app.logger.info("make_report")
    reports = get_active_report()

    report_list = []
    for r in reports:
        result = execute_sql(r.db_name, r.query_string)
        report_dict = dict()
        report_dict['title'] = r.title
        report_dict['value'] = result
        report_dict['short'] = True
        report_list.append(report_dict)

    if len(report_list) > 0:
        send_report(report_list)


def get_active_report():
    with db.app.app_context():
        return Report.query.filter_by(is_active=1).order_by(Report.order).all()


def execute_sql(db_name, query):
    app.logger.info("execute_sql - db_name: %s, query: %s", db_name, query)
    mariadb_connection = mariadb.connect(host=database_config[db_name][0], 
        user=database_config[db_name][1], password=database_config[db_name][2],
        database=database_config[db_name][3],
        charset='utf8')
    cursor = mariadb_connection.cursor()
    cursor.execute(query)
    
    result = ""
    for row in cursor:
        app.logger.info("row : %s", row)
        result += ", ".join(map(lambda x: x if type(x) is unicode else str(x), row)) # python 2.7
        # result += ", ".join(map(str, row)) # python 3
        result += "\n"
    
    mariadb_connection.close()
    return result


def send_report(report_list):
    app.logger.info("send_report - %s", report_list)
    sc = SlackClient(slack_token)
    attachments_dict = dict()
    attachments_dict['fallback'] = "Daily Matric Report"
    attachments_dict['color'] = "#36a64f"
    attachments_dict['title'] = "Panel Daily Matric Report"
    attachments_dict['title_link'] = "http://slack-reporter-host.com:8000/admin/report/"
    attachments_dict['text'] = datetime.date.today().strftime("%Y-%m-%d")
    attachments_dict['fields'] = report_list
    attachments = [attachments_dict]

    app.logger.info(sc.api_call("chat.postMessage", channel="#channel", attachments=attachments, username='Daily Matric Report', icon_emoji=':rolled_up_newspaper:'))


@app.route("/make_report")
def make_report_api():
    make_report()
    return "success"


class Config(object):
    JOBS = [
        {
            'id': 'job1',
            'func': make_report,
            'trigger': 'cron',
            'second': '*/20',
        }
    ]

    SCHEDULER_API_ENABLED = True

    SQLALCHEMY_TRACK_MODIFICATIONS = False


if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fileHandler = RotatingFileHandler('slack-reporter.log', maxBytes=100000, backupCount=1)
    streamHandler = logging.StreamHandler()
    fileHandler.setFormatter(formatter)
    streamHandler.setFormatter(formatter)
    app.logger.addHandler(fileHandler)
    app.logger.addHandler(streamHandler)
    
    app.config.from_object(Config())
    app.secret_key = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'

    db.app = app
    db.init_app(app)
    db.create_all()

    scheduler = APScheduler()
    scheduler.init_app(app)
    scheduler.start()

    admin = Admin(app, name='Slack Reporter', template_mode='bootstrap3')
    admin.add_view(ModelView(Report, db.session))

    app.run(host='0.0.0.0', port=8000)